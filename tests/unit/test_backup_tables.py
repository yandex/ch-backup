from contextlib import AbstractContextManager, nullcontext
from dataclasses import replace
from unittest.mock import MagicMock, Mock, call, patch

import pytest

from ch_backup.backup.metadata import BackupMetadata, PartMetadata, TableMetadata
from ch_backup.backup_context import BackupContext
from ch_backup.clickhouse.client import ClickhouseError
from ch_backup.clickhouse.models import Database, Table
from ch_backup.config import DEFAULT_CONFIG
from ch_backup.logic.table import TableBackup, TableMetadataChangeTime

UUID = "fa8ff291-1922-4b7f-afa7-06633d5e16ae"


_METADATA_UNCHANGED = TableMetadataChangeTime(
    metadata_path="", mtime_ns=16890001958000000, ctime_ns=16890001958000000
)
_METADATA_MTIME_CHANGED = replace(_METADATA_UNCHANGED, mtime_ns=16890001958000111)
_METADATA_CTIME_CHANGED = replace(_METADATA_UNCHANGED, ctime_ns=16890001958000111)
_FREEZE_ERROR = ClickhouseError("Cannot freeze table")
_EXISTS_ERROR = ClickhouseError("Cannot check table existence")


@pytest.mark.parametrize(
    "metadata_after_freeze, freeze_error, table_exists, expected_error, expected_databases",
    [
        pytest.param(
            [_METADATA_UNCHANGED, _METADATA_UNCHANGED],
            None,
            True,
            None,
            ["db1", "db2"],
            id="metadata-unchanged",
        ),
        pytest.param(
            [_METADATA_MTIME_CHANGED, _METADATA_UNCHANGED],
            None,
            True,
            None,
            ["db2"],
            id="mtime-changed-after-freeze",
        ),
        pytest.param(
            [_METADATA_CTIME_CHANGED, _METADATA_UNCHANGED],
            None,
            True,
            None,
            ["db2"],
            id="ctime-changed-after-freeze",
        ),
        pytest.param(
            [_METADATA_UNCHANGED, _METADATA_CTIME_CHANGED],
            None,
            True,
            None,
            ["db1"],
            id="exchange-between-databases",
        ),
        pytest.param(
            [_METADATA_MTIME_CHANGED, _METADATA_UNCHANGED],
            _FREEZE_ERROR,
            True,
            None,
            ["db2"],
            id="freeze-error-mtime-changed",
        ),
        pytest.param(
            [_METADATA_CTIME_CHANGED, _METADATA_UNCHANGED],
            _FREEZE_ERROR,
            True,
            None,
            ["db2"],
            id="freeze-error-ctime-changed",
        ),
        pytest.param(
            [None, _METADATA_UNCHANGED],
            _FREEZE_ERROR,
            True,
            None,
            ["db2"],
            id="freeze-error-metadata-missing",
        ),
        pytest.param(
            [_METADATA_UNCHANGED, _METADATA_UNCHANGED],
            _FREEZE_ERROR,
            False,
            None,
            ["db2"],
            id="freeze-error-table-missing",
        ),
        pytest.param(
            [_METADATA_UNCHANGED, _METADATA_UNCHANGED],
            _FREEZE_ERROR,
            True,
            _FREEZE_ERROR,
            [],
            id="freeze-error-table-unchanged",
        ),
        pytest.param(
            [_METADATA_UNCHANGED, _METADATA_UNCHANGED],
            _FREEZE_ERROR,
            _EXISTS_ERROR,
            _EXISTS_ERROR,
            [],
            id="existence-check-error",
        ),
    ],
)
# pylint: disable=too-many-locals
def test_backup_table_skipping_if_metadata_updated_during_backup(
    metadata_after_freeze: list[TableMetadataChangeTime | None],
    freeze_error: ClickhouseError | None,
    table_exists: bool | ClickhouseError,
    expected_error: ClickhouseError | None,
    expected_databases: list[str],
) -> None:
    table_name = "table1"
    db1_name = "db1"
    db2_name = "db2"
    creation_statement = f"ATTACH TABLE {db1_name}.{table_name} UUID '{UUID}' (date Date) ENGINE = MergeTree();"

    # Prepare involved data objects
    context = BackupContext(DEFAULT_CONFIG)  # type: ignore[arg-type]
    db1 = Database(
        db1_name, "Atomic", "/var/lib/clickhouse/metadata/db1.sql", None, None
    )
    db2 = Database(
        db2_name, "Atomic", "/var/lib/clickhouse/metadata/db2.sql", None, None
    )
    table_backup = TableBackup()
    backup_meta = BackupMetadata(
        name="20181017T210300",
        # DEPRECATED: kept for backward compatibility with older versions.
        path="ch_backup/20181017T210300",
        version="1.0.100",
        ch_version="19.1.16",
        time_format="%Y-%m-%dT%H:%M:%S%Z",
        hostname="clickhouse01.test_net_711",
    )

    backup_meta.add_database(db1)
    backup_meta.add_database(db2)
    context.backup_meta = backup_meta

    # Mock external interactions
    # Each database has its own metadata path (EXCHANGE TABLES swaps inodes, not paths)
    tables_by_db = {
        db1_name: [
            Table(
                db1_name,
                table_name,
                "MergeTree",
                [],
                [],
                f"/var/lib/clickhouse/metadata/{db1_name}/{table_name}.sql",
                "",
                UUID,
            )
        ],
        db2_name: [
            Table(
                db2_name,
                table_name,
                "MergeTree",
                [],
                [],
                f"/var/lib/clickhouse/metadata/{db2_name}/{table_name}.sql",
                "",
                UUID,
            )
        ],
    }
    clickhouse_ctl_mock = Mock()
    clickhouse_ctl_mock.get_tables.side_effect = lambda db_name, *a, **kw: tables_by_db[
        db_name
    ]
    clickhouse_ctl_mock.get_disks.return_value = {}
    context.ch_ctl = clickhouse_ctl_mock

    context.backup_layout = Mock()

    clickhouse_ctl_mock.freeze_table.side_effect = [freeze_error, None]
    if isinstance(table_exists, ClickhouseError):
        clickhouse_ctl_mock.does_table_exist.side_effect = table_exists
    else:
        clickhouse_ctl_mock.does_table_exist.return_value = table_exists

    # Capture metadata for both databases before freezing either table.
    # Keep reads scoped to each path, without mocking global filesystem calls.
    change_times = {
        table.metadata_path: iter(
            [
                replace(_METADATA_UNCHANGED, metadata_path=table.metadata_path),
                replace(after, metadata_path=table.metadata_path) if after else None,
            ]
        )
        for tables, after in zip(tables_by_db.values(), metadata_after_freeze)
        for table in tables
    }
    error_context: AbstractContextManager[pytest.ExceptionInfo[ClickhouseError] | None]
    if expected_error:
        error_context = pytest.raises(ClickhouseError)
    else:
        error_context = nullcontext()
    with (
        patch.object(
            TableBackup,
            "_get_change_time",
            side_effect=lambda path: next(change_times[path]),
        ),
        patch.object(
            TableBackup,
            "_load_create_statement_from_disk",
            return_value=creation_statement,
        ),
        patch.object(
            table_backup,
            "_backup_frozen_table_data",
            wraps=table_backup._backup_frozen_table_data,  # pylint: disable=protected-access
        ) as backup_frozen_table_data,
        error_context as raised,
    ):
        table_backup.backup(
            context,
            [db1, db2],
            {db1_name: [table_name], db2_name: [table_name]},
            schema_only=False,
            multiprocessing_config=DEFAULT_CONFIG["multiprocessing"],  # type: ignore
        )

    if expected_error:
        assert raised is not None
        assert raised.value is expected_error
        clickhouse_ctl_mock.remove_freezed_data.assert_not_called()
    else:
        # Exactly one cleanup per table, whether skipped or successfully backed up,
        # and one cleanup after each database.
        assert clickhouse_ctl_mock.remove_freezed_data.call_args_list == [
            call(backup_meta.get_sanitized_name(), tables_by_db[db1_name][0]),
            call(),
            call(backup_meta.get_sanitized_name(), tables_by_db[db2_name][0]),
            call(),
        ]

    assert [(table.database, table.name) for table in backup_meta.get_tables()] == [
        (db_name, table_name) for db_name in expected_databases
    ]
    assert context.backup_layout.upload_create_statements.call_args_list == [
        call(backup_meta, db, [(table_name, creation_statement)])
        for db in [db1, db2]
        if db.name in expected_databases
    ]
    assert backup_frozen_table_data.call_args_list == [
        call(context, tables_by_db[db_name][0], backup_meta.get_sanitized_name())
        for db_name in expected_databases
    ]
    if freeze_error and metadata_after_freeze[0] == _METADATA_UNCHANGED:
        clickhouse_ctl_mock.does_table_exist.assert_called_once_with(
            db1_name, table_name
        )
    else:
        clickhouse_ctl_mock.does_table_exist.assert_not_called()


class TestValidateUploadedParts:
    """
    Tests for TableBackup._validate_uploaded_parts.
    """

    # pylint: disable=protected-access

    _BACKUP_NAME = "20181017T210300"

    def _make_part(self, name: str, link: str | None = None) -> PartMetadata:
        return PartMetadata(
            database="db1",
            table="table1",
            name=name,
            checksum="abc123",
            size=1024,
            files=["data.bin"],
            tarball=True,
            link=link,
        )

    def _make_context(
        self, validate: bool, check_returns: bool
    ) -> tuple[BackupContext, MagicMock]:
        context = Mock(spec=BackupContext)
        context.config = {"validate_part_after_upload": validate}
        context.backup_meta = MagicMock()
        context.backup_meta.name = self._BACKUP_NAME
        check_data_part_mock = MagicMock(return_value=check_returns)
        layout_mock = MagicMock()
        layout_mock.check_data_part = check_data_part_mock
        context.backup_layout = layout_mock
        return context, check_data_part_mock

    def test_validate_disabled_skips_check(self):
        """When validate_part_after_upload is False, check_data_part is never called."""
        part = self._make_part("all_1_1_0")
        context, check_mock = self._make_context(validate=False, check_returns=True)

        TableBackup._validate_uploaded_parts(context, [part])

        check_mock.assert_not_called()

    def test_validate_calls_check_with_backup_name(self):
        """check_data_part must receive the backup *name* (not a path)."""
        part = self._make_part("all_1_1_0")
        context, check_mock = self._make_context(validate=True, check_returns=True)

        TableBackup._validate_uploaded_parts(context, [part])

        check_mock.assert_called_once_with(self._BACKUP_NAME, part)

    def test_validate_raises_on_broken_part(self):
        """RuntimeError is raised when check_data_part returns False."""
        part = self._make_part("all_1_1_0")
        context, _ = self._make_context(validate=True, check_returns=False)

        with pytest.raises(RuntimeError, match="all_1_1_0"):
            TableBackup._validate_uploaded_parts(context, [part])

    def test_validate_deduplicated_part_uses_backup_name(self):
        """
        For a deduplicated part (link set to a source backup name),
        _validate_uploaded_parts still passes the *current* backup name to
        check_data_part — the layout itself resolves the link internally.
        """
        source_backup = "20181010T120000"
        part = self._make_part("all_1_1_0", link=source_backup)
        context, check_mock = self._make_context(validate=True, check_returns=True)

        TableBackup._validate_uploaded_parts(context, [part])

        check_mock.assert_called_once_with(self._BACKUP_NAME, part)

    def test_validate_all_parts_checked_before_raising(self):
        """All invalid parts are collected before RuntimeError is raised."""
        parts = [self._make_part(f"all_{i}_1_0") for i in range(3)]
        context, check_mock = self._make_context(validate=True, check_returns=False)

        with pytest.raises(RuntimeError):
            TableBackup._validate_uploaded_parts(context, parts)

        assert check_mock.call_count == 3


class TestRestorePreprocessing:
    @pytest.mark.parametrize(
        ("backup_uuid", "expected_detached_name"),
        [
            (UUID, "detached_by_uuid"),
            ("uuid-missed", "table1"),
        ],
    )
    def test_preprocess_tables_to_restore_matches_detached_table_by_uuid_then_name(
        self,
        backup_uuid,
        expected_detached_name,
    ):
        # pylint: disable=protected-access
        table_backup = TableBackup()
        context = Mock(spec=BackupContext)
        context.ch_ctl = Mock()
        uuid_matched_table = Table(
            "db1", "detached_by_uuid", "MergeTree", [], [], "meta-uuid.sql", "", UUID
        )
        name_matched_table = Table(
            "db1", "table1", "MergeTree", [], [], "meta-name.sql", "", "uuid-other"
        )
        context.ch_ctl.get_detached_tables.return_value = [
            name_matched_table,
            uuid_matched_table,
        ]
        context.ch_ctl.get_tables.return_value = []
        context.ch_ctl.get_replicas.return_value = []
        backup_table = Table("db1", "table1", "MergeTree", [], [], "", "", backup_uuid)
        databases = {"db1": Database("db1", "Atomic", None, None, None)}

        with patch.object(
            table_backup,
            "_rewrite_table_schema",
            side_effect=lambda *_args, **_kwargs: setattr(
                backup_table, "create_statement", "CREATE TABLE"
            ),
        ):
            result, _ = table_backup._preprocess_tables_to_restore(
                context,
                databases,
                [backup_table],
                keep_going=False,
                restore_tables_in_replicated_database=True,
                metadata_cleaner=None,
            )

        attached_table = context.ch_ctl.attach_table.call_args.args[0]
        assert attached_table.name == expected_detached_name
        assert result == [backup_table]


class TestCloudStorageFlags:
    """
    Tests that the cloud_storage options reach backup metadata.
    """

    @staticmethod
    def _backup_with_cloud_conf(cloud_conf: dict) -> BackupMetadata:
        """Helper: run a schema-only backup with a given cloud_storage config."""
        config = {**DEFAULT_CONFIG, "cloud_storage": cloud_conf}
        context = BackupContext(config)  # type: ignore[arg-type]
        context.ch_ctl = MagicMock()
        context.backup_meta = BackupMetadata(
            name="20181017T210300",
            path="ch_backup/20181017T210300",
            version="1.0.100",
            ch_version="19.1.16",
            time_format="%Y-%m-%dT%H:%M:%S%Z",
            hostname="clickhouse01.test_net_711",
        )

        TableBackup().backup(
            context,
            databases=[],
            db_tables={},
            schema_only=True,
            multiprocessing_config={},
        )

        return context.backup_meta

    @pytest.mark.parametrize(
        "cloud_conf,encrypted,compressed",
        [
            ({"encryption": False, "compression": False}, False, False),
            ({"encryption": True, "compression": True}, True, True),
            ({}, True, True),
        ],
        ids=["disabled", "enabled", "absent"],
    )
    def test_metadata_protection_follows_the_options(
        self, cloud_conf: dict, encrypted: bool, compressed: bool
    ) -> None:
        """
        The options decide whether cloud storage metadata is encrypted and
        compressed. Absent, they keep the default of doing both.
        """
        backup_meta = self._backup_with_cloud_conf(cloud_conf)

        assert backup_meta.cloud_storage.encrypted is encrypted
        assert backup_meta.cloud_storage.compressed is compressed


class TestUniqueKeyTables:
    """
    Tests that tables with a UNIQUE KEY clause are backed up without data.

    ClickHouse 26.5+ rejects ALTER ... PARTITION on them, so FREEZE is not an
    option and the backup must record that the data was left out.
    """

    _DB = "db1"
    _TABLE = "table1"
    _PLAIN_STATEMENT = (
        f"ATTACH TABLE {_DB}.{_TABLE} UUID '{UUID}' (id UInt64) "
        "ENGINE = MergeTree ORDER BY id"
    )
    _ON_DISK_STATEMENT = (
        f"ATTACH TABLE _ UUID '{UUID}'\n(\n"
        "    `id` UInt64 COMMENT 'no UNIQUE KEY here'\n)\n"
        "ENGINE = MergeTree\nORDER BY id\nUNIQUE KEY id\n"
        "SETTINGS index_granularity = 8192"
    )
    _UNIQUE_KEY_STATEMENTS = (
        f"{_PLAIN_STATEMENT} UNIQUE KEY (id)",
        f"{_PLAIN_STATEMENT} UNIQUE KEY id",
        _ON_DISK_STATEMENT,
    )
    _LOOKALIKE_STATEMENTS = (
        f"{_PLAIN_STATEMENT} COMMENT 'UNIQUE KEY (id) is kept by the application'",
        f"{_PLAIN_STATEMENT} COMMENT 'it''s a UNIQUE KEY (id)'",
        f"ATTACH TABLE {_DB}.{_TABLE} UUID '{UUID}' (`UNIQUE KEY (id)` UInt64) "
        "ENGINE = MergeTree ORDER BY id",
        f"ATTACH TABLE {_DB}.{_TABLE} UUID '{UUID}' (id UInt64) ENGINE = "
        f"ReplicatedMergeTree('/clickhouse/tables/UNIQUE KEY (id)/{_TABLE}', 'r1') "
        "ORDER BY id",
    )

    @classmethod
    def _make_context(cls) -> BackupContext:
        """Helper: build a context with db1 registered and ClickHouse mocked."""
        context = BackupContext(DEFAULT_CONFIG)  # type: ignore[arg-type]
        context.backup_meta = BackupMetadata(
            name="20181017T210300",
            path="ch_backup/20181017T210300",
            version="1.0.100",
            ch_version="19.1.16",
            time_format="%Y-%m-%dT%H:%M:%S%Z",
            hostname="clickhouse01.test_net_711",
        )
        context.backup_meta.add_database(cls._database())
        context.ch_ctl = MagicMock()
        context.backup_layout = MagicMock()
        return context

    @classmethod
    def _database(cls) -> Database:
        return Database(
            cls._DB, "Atomic", f"/var/lib/clickhouse/metadata/{cls._DB}.sql", None, None
        )

    @classmethod
    def _table(cls) -> Table:
        return Table(
            cls._DB,
            cls._TABLE,
            "MergeTree",
            [],
            [],
            f"/var/lib/clickhouse/metadata/{cls._DB}/{cls._TABLE}.sql",
            "",
            UUID,
        )

    @classmethod
    def _backup_with_statement(cls, create_statement: str) -> BackupContext:
        """Helper: back up the table with the given create statement."""
        context = cls._make_context()
        context.ch_ctl.get_tables.return_value = [cls._table()]  # type: ignore[attr-defined]
        context.ch_ctl.get_disks.return_value = {}  # type: ignore[attr-defined]
        context.ch_ctl.scan_frozen_parts.return_value = []  # type: ignore[attr-defined]

        with (
            patch.object(
                TableBackup, "_get_change_time", return_value=_METADATA_UNCHANGED
            ),
            patch.object(
                TableBackup,
                "_load_create_statement_from_disk",
                return_value=create_statement,
            ),
        ):
            TableBackup().backup(
                context,
                [cls._database()],
                {cls._DB: [cls._TABLE]},
                schema_only=False,
                multiprocessing_config=DEFAULT_CONFIG["multiprocessing"],  # type: ignore[arg-type]
            )

        return context

    @pytest.mark.parametrize("create_statement", _UNIQUE_KEY_STATEMENTS)
    def test_unique_key_table_is_not_frozen(self, create_statement: str) -> None:
        """
        A UNIQUE KEY table never reaches FREEZE, so the backup does not fail.
        """
        context = self._backup_with_statement(create_statement)

        context.ch_ctl.freeze_table.assert_not_called()  # type: ignore[attr-defined]

    @pytest.mark.parametrize("create_statement", _UNIQUE_KEY_STATEMENTS)
    def test_unique_key_table_is_marked_as_data_skipped(
        self, create_statement: str
    ) -> None:
        """
        The schema is backed up and the metadata says why there is no data.
        """
        context = self._backup_with_statement(create_statement)

        table_meta = context.backup_meta.get_tables(self._DB)[0]
        assert table_meta.name == self._TABLE
        assert table_meta.data_skipped_reason == "unique_key"
        assert not table_meta.get_parts()

    @pytest.mark.parametrize(
        "create_statement", (_PLAIN_STATEMENT, *_LOOKALIKE_STATEMENTS)
    )
    def test_plain_table_is_frozen_and_not_marked(self, create_statement: str) -> None:
        """
        A table without a UNIQUE KEY clause keeps the usual behaviour, even when
        its comments, identifiers or engine arguments mention one.
        """
        context = self._backup_with_statement(create_statement)

        context.ch_ctl.freeze_table.assert_called_once()  # type: ignore[attr-defined]
        table_meta = context.backup_meta.get_tables(self._DB)[0]
        assert table_meta.data_skipped_reason is None

    def test_unique_key_is_only_looked_for_in_merge_tree(self) -> None:
        """
        The clause belongs to the MergeTree family, other engines are left alone.
        """
        table = self._table()
        table.engine = "Distributed"
        table.create_statement = self._UNIQUE_KEY_STATEMENTS[0]

        assert not table.has_unique_key()

    def test_data_restore_is_skipped_for_marked_table(self) -> None:
        """
        Restore leaves a marked table empty instead of looking up its parts.
        """
        context = self._make_context()
        context.ch_ctl.get_table.return_value = self._table()  # type: ignore[attr-defined]
        table_meta = TableMetadata(
            self._DB, self._TABLE, "MergeTree", UUID, "unique_key"
        )

        TableBackup()._restore_data(  # pylint: disable=protected-access
            context,
            [table_meta],
            MagicMock(),
            skip_cloud_storage=False,
            keep_going=False,
        )

        context.ch_ctl.attach_part.assert_not_called()  # type: ignore[attr-defined]
        context.ch_ctl.get_table.assert_called_once_with(  # type: ignore[attr-defined]
            self._DB, self._TABLE, short_query=True
        )
