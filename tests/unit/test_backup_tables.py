from dataclasses import dataclass
from typing import List, Optional
from unittest.mock import MagicMock, Mock, patch

import pytest

from ch_backup.backup.metadata import BackupMetadata, PartMetadata, TableMetadata
from ch_backup.backup_context import BackupContext
from ch_backup.clickhouse.models import Database, Table
from ch_backup.config import DEFAULT_CONFIG
from ch_backup.logic.table import TableBackup

UUID = "fa8ff291-1922-4b7f-afa7-06633d5e16ae"


@dataclass
class FakeStatResult:
    st_mtime_ns: int
    st_ctime_ns: int


_STAT_UNCHANGED = FakeStatResult(
    st_mtime_ns=16890001958000000, st_ctime_ns=16890001958000000
)
_STAT_MTIME_CHANGED = FakeStatResult(
    st_mtime_ns=16890001958000111, st_ctime_ns=16890001958000000
)
_STAT_CTIME_CHANGED = FakeStatResult(
    st_mtime_ns=16890001958000000, st_ctime_ns=16890001958000111
)


@pytest.mark.parametrize(
    "fake_stats, backups_expected_db1, backups_expected_db2",
    [
        # Metadata unchanged in both dbs -> both tables backed up
        ([_STAT_UNCHANGED, _STAT_UNCHANGED, _STAT_UNCHANGED, _STAT_UNCHANGED], 1, 1),
        # db1 table mtime changed after freeze -> db1 skipped, db2 backed up
        (
            [_STAT_UNCHANGED, _STAT_UNCHANGED, _STAT_MTIME_CHANGED, _STAT_UNCHANGED],
            0,
            1,
        ),
        # db1 table ctime changed after freeze -> db1 skipped, db2 backed up
        (
            [_STAT_UNCHANGED, _STAT_UNCHANGED, _STAT_CTIME_CHANGED, _STAT_UNCHANGED],
            0,
            1,
        ),
        # EXCHANGE TABLES between db1 and db2: db1 backed up normally,
        # db2 table ctime changed (EXCHANGE happened) -> db2 skipped
        (
            [_STAT_UNCHANGED, _STAT_UNCHANGED, _STAT_UNCHANGED, _STAT_CTIME_CHANGED],
            1,
            0,
        ),
    ],
)
def test_backup_table_skipping_if_metadata_updated_during_backup(
    fake_stats: List[FakeStatResult],
    backups_expected_db1: int,
    backups_expected_db2: int,
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

    read_bytes_mock = Mock(return_value=creation_statement.encode())

    with (
        patch("os.stat", side_effect=fake_stats),
        patch("ch_backup.logic.table.Path", read_bytes=read_bytes_mock),
    ):
        table_backup.backup(
            context,
            [db1, db2],
            {db1_name: [table_name], db2_name: [table_name]},
            schema_only=False,
            multiprocessing_config=DEFAULT_CONFIG["multiprocessing"],  # type: ignore
        )

    assert len(context.backup_meta.get_tables(db1_name)) == backups_expected_db1
    assert len(context.backup_meta.get_tables(db2_name)) == backups_expected_db2
    # One call after each table and one after each database is backed up
    assert clickhouse_ctl_mock.remove_freezed_data.call_count == 4


class TestValidateUploadedParts:
    """
    Tests for TableBackup._validate_uploaded_parts.
    """

    # pylint: disable=protected-access

    _BACKUP_NAME = "20181017T210300"

    def _make_part(self, name: str, link: Optional[str] = None) -> PartMetadata:
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
    def test_resolve_duplicate_backup_tables_keep_going(self):
        tables_meta = [
            TableMetadata("db1", "table1", "MergeTree", UUID),
            TableMetadata("db2", "table2", "MergeTree", UUID),
        ]

        result, skipped = TableBackup._resolve_duplicate_backup_tables(
            tables_meta, keep_going=True
        )

        assert skipped == 1
        assert [(table.database, table.name) for table in result] == [("db1", "table1")]

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
            result = table_backup._preprocess_tables_to_restore(
                context,
                databases,
                [backup_table],
                keep_going=False,
                restore_tables_in_replicated_database=True,
                metadata_cleaner=None,
            )

        attached_table = context.ch_ctl.attach_table.call_args.args[0]
        assert attached_table.name == expected_detached_name
        assert result.tables == [backup_table]

    def test_restore_keep_going_accumulates_skips_and_restores_remaining_tables(self):
        table_backup = TableBackup()
        context = Mock(spec=BackupContext)
        context.backup_meta = Mock()
        context.config_root = {"restore": {"use_inplace_cloud_restore": False}}
        context.backup_meta.get_tables.return_value = [
            TableMetadata("db1", "table1", "MergeTree", UUID),
            TableMetadata("db1", "table2", "MergeTree", UUID),
            TableMetadata("db1", "table3", "MergeTree", "uuid-3"),
            TableMetadata("db1", "table4", "MergeTree", "uuid-4"),
            TableMetadata("db1", "table5", "MergeTree", "uuid-5"),
        ]
        databases = {"db1": Database("db1", "Atomic", None, None, None)}
        source_tables = [
            Table(
                "db1",
                "table3",
                "MergeTree",
                [],
                [],
                "",
                "ATTACH TABLE db1.table3",
                "uuid-3",
            ),
            Table(
                "db1",
                "table4",
                "MergeTree",
                [],
                [],
                "",
                "CREATE TABLE db1.table4",
                "uuid-4",
            ),
            Table(
                "db1",
                "table5",
                "MergeTree",
                [],
                [],
                "",
                "CREATE TABLE db1.table5",
                "uuid-5",
            ),
        ]
        restored_tables = [source_tables[1], source_tables[2]]

        with (
            patch.object(
                table_backup,
                "_get_tables_from_meta",
                return_value=(source_tables, 1),
            ),
            patch.object(
                table_backup,
                "_preprocess_tables_to_restore",
                return_value=type(
                    "R",
                    (),
                    {
                        "tables": restored_tables,
                        "preprocessing_failed_skipped": 1,
                    },
                )(),
            ) as preprocess_tables,
            patch.object(
                table_backup,
                "_restore_tables",
                return_value=[
                    Table(
                        "db1",
                        "table1",
                        "MergeTree",
                        [],
                        [],
                        "",
                        "ATTACH TABLE db1.table1",
                        UUID,
                    ),
                    source_tables[1],
                ],
            ) as restore_tables,
            patch.object(table_backup, "_restore_data") as restore_data,
            patch.object(
                table_backup, "_log_restore_preprocess_summary"
            ) as log_summary,
            patch("ch_backup.logic.table.ClickHouseTemporaryDisks"),
        ):
            table_backup.restore(
                context,
                databases,
                schema_only=False,
                tables=[],
                metadata_cleaner=None,
                cloud_storage_source_bucket=None,
                cloud_storage_source_path=None,
                cloud_storage_source_endpoint=None,
                skip_cloud_storage=False,
                keep_going=True,
                restore_tables_in_replicated_database=True,
            )

        filtered_meta = preprocess_tables.call_args.args[2]
        assert [(table.database, table.name) for table in filtered_meta] == [
            ("db1", "table3"),
            ("db1", "table4"),
            ("db1", "table5"),
        ]
        restore_tables.assert_called_once_with(
            context,
            databases,
            restored_tables,
            True,
        )
        restore_data.assert_called_once()
        restored_data_tables = list(restore_data.call_args.kwargs["tables"])
        assert [(table.database, table.name) for table in restored_data_tables] == [
            ("db1", "table3"),
            ("db1", "table5"),
        ]
        log_summary.assert_called_once_with(
            duplicate_uuid_skipped=1,
            missing_create_statement_skipped=1,
            preprocessing_failed_skipped=1,
        )

    def test_restore_keep_going_uses_present_subset_after_missing_requested_tables(
        self,
    ):
        table_backup = TableBackup()
        context = Mock(spec=BackupContext)
        context.backup_meta = Mock()
        context.config_root = {"restore": {"use_inplace_cloud_restore": False}}
        existing_table = TableMetadata("db1", "table1", "MergeTree", UUID)
        missing_table = TableMetadata("db1", "missing", "MergeTree", "uuid-2")
        context.backup_meta.get_tables.return_value = [existing_table]
        databases = {"db1": Database("db1", "Atomic", None, None, None)}
        restored_table = Table(
            "db1", "table1", "MergeTree", [], [], "", "CREATE TABLE", UUID
        )

        with (
            patch.object(
                table_backup,
                "_get_tables_from_meta",
                return_value=([restored_table], 0),
            ) as get_tables_from_meta,
            patch.object(
                table_backup,
                "_preprocess_tables_to_restore",
                return_value=type(
                    "R",
                    (),
                    {
                        "tables": [restored_table],
                        "preprocessing_failed_skipped": 0,
                    },
                )(),
            ) as preprocess_tables,
            patch.object(table_backup, "_restore_tables", return_value=[]),
            patch.object(table_backup, "_restore_data"),
            patch.object(table_backup, "_log_restore_preprocess_summary"),
            patch("ch_backup.logic.table.ClickHouseTemporaryDisks"),
        ):
            table_backup.restore(
                context,
                databases,
                schema_only=False,
                tables=[existing_table, missing_table],
                metadata_cleaner=None,
                cloud_storage_source_bucket=None,
                cloud_storage_source_path=None,
                cloud_storage_source_endpoint=None,
                skip_cloud_storage=False,
                keep_going=True,
                restore_tables_in_replicated_database=True,
            )

        filtered_meta = get_tables_from_meta.call_args.args[1]
        assert [(table.database, table.name) for table in filtered_meta] == [
            ("db1", "table1")
        ]
        preprocess_tables.assert_called_once()
