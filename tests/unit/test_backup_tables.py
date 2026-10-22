# pylint: disable=too-many-lines

import copy
import errno
import os
import threading
from concurrent.futures.process import BrokenProcessPool
from contextlib import AbstractContextManager, nullcontext
from dataclasses import replace
from pathlib import Path
from tarfile import BLOCKSIZE
from typing import Callable, Sequence
from unittest.mock import ANY, MagicMock, Mock, call, patch

import pytest
from loguru import logger

from ch_backup.backup.layout import BackupLayout
from ch_backup.backup.metadata import BackupMetadata, PartMetadata, TableMetadata
from ch_backup.backup.metadata.detached_part_metadata import DetachedPartMetadata
from ch_backup.backup.metadata.table_metadata import DataSkippedReason
from ch_backup.backup.restore_context import RestoreContext
from ch_backup.backup_context import BackupContext
from ch_backup.calculators import calc_aligned_files_size, calc_tarball_size
from ch_backup.clickhouse.client import ClickhouseError
from ch_backup.clickhouse.disks import ClickHouseDisksException
from ch_backup.clickhouse.models import Database, DetachedPart, Disk, FrozenPart, Table
from ch_backup.clickhouse.storage_schema import StoragePolicy
from ch_backup.config import DEFAULT_CONFIG
from ch_backup.exceptions import ClickhouseBackupError, StorageError, TerminatingSignal
from ch_backup.logic.table import TableBackup, TableMetadataChangeTime
from ch_backup.storage.async_pipeline.base_pipeline.exec_pool import ThreadExecPool
from ch_backup.storage.engine.s3.s3_retry import S3RetryingError

UUID = "fa8ff291-1922-4b7f-afa7-06633d5e16ae"
BACKUP_NAME = "20181017T210300"
DATABASE = Database("db1", "Atomic", "/var/lib/clickhouse/metadata/db1.sql", None, None)


def _make_backup_context(cloud_conf: dict | None = None) -> BackupContext:
    """Helper: build a context backing up tables of database db1."""
    config: dict = copy.deepcopy(DEFAULT_CONFIG)
    if cloud_conf is not None:
        config["cloud_storage"] = cloud_conf
    context = BackupContext(config)  # type: ignore[arg-type]
    context.ch_ctl = MagicMock()
    context.backup_layout = MagicMock()
    context.ch_config = MagicMock()
    context.ch_config.config = {}
    context.backup_meta = BackupMetadata(
        name=BACKUP_NAME,
        path=f"ch_backup/{BACKUP_NAME}",
        version="1.0.100",
        ch_version="19.1.16",
        time_format="%Y-%m-%dT%H:%M:%S%Z",
        hostname="clickhouse01.test_net_711",
    )
    context.backup_meta.add_database(DATABASE)
    return context


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
        call(context, tables_by_db[db_name][0], backup_meta.get_sanitized_name(), None)
        for db_name in expected_databases
    ]
    if freeze_error and metadata_after_freeze[0] == _METADATA_UNCHANGED:
        clickhouse_ctl_mock.does_table_exist.assert_called_once_with(
            db1_name, table_name
        )
    else:
        clickhouse_ctl_mock.does_table_exist.assert_not_called()
    clickhouse_ctl_mock.scan_detached_parts.assert_not_called()


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
        self, validate: bool, check_returns: bool, keep_going: bool = False
    ) -> tuple[BackupContext, MagicMock]:
        context = Mock(spec=BackupContext)
        context.config = {
            "validate_part_after_upload": validate,
            "keep_going": keep_going,
        }
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

    def test_broken_part_fails_the_backup_even_when_keeping_going(self):
        """
        A failed check may come from storage being unavailable, so a part that
        may be intact is not deleted on it.
        """
        part = self._make_part("all_1_1_0")
        context, _ = self._make_context(
            validate=True, check_returns=False, keep_going=True
        )

        with pytest.raises(RuntimeError):
            TableBackup._validate_uploaded_parts(context, [part])

        context.backup_layout.delete_data_parts.assert_not_called()  # type: ignore[attr-defined]

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
            result, _, failed_tables = table_backup._preprocess_tables_to_restore(
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
        assert not failed_tables


class TestCloudStorageFlags:
    """
    Tests that the cloud_storage options reach backup metadata.
    """

    @staticmethod
    def _backup_with_cloud_conf(
        cloud_conf: dict, schema_only: bool = True
    ) -> BackupMetadata:
        """Helper: run a backup of no tables with a given cloud_storage config."""
        context = _make_backup_context(cloud_conf)

        TableBackup().backup(
            context,
            databases=[],
            db_tables={},
            schema_only=schema_only,
            multiprocessing_config={},
        )

        return context.backup_meta

    @pytest.mark.parametrize(
        "cloud_conf,encrypted,compressed",
        [
            ({"encryption": False, "compression": False}, False, False),
            ({"encryption": True, "compression": True}, True, True),
            ({}, False, True),
        ],
        ids=["disabled", "enabled", "absent"],
    )
    def test_metadata_protection_follows_the_options(
        self, cloud_conf: dict, encrypted: bool, compressed: bool
    ) -> None:
        """
        The options decide whether cloud storage metadata is encrypted and
        compressed. By default metadata is public and compressed.
        """
        backup_meta = self._backup_with_cloud_conf(cloud_conf)

        assert backup_meta.cloud_storage.encrypted is encrypted
        assert backup_meta.cloud_storage.compressed is compressed

    @pytest.mark.parametrize(
        "cloud_conf,schema_only,data_copied",
        [
            ({"copy_data": True}, False, True),
            ({"copy_data": True}, True, False),
            ({"copy_data": False}, False, False),
            ({}, False, False),
        ],
        ids=["enabled", "schema only", "disabled", "absent"],
    )
    def test_data_copying_follows_the_option(
        self, cloud_conf: dict, schema_only: bool, data_copied: bool
    ) -> None:
        """
        The option marks the backup as containing copied cloud storage data.
        Disabled or absent, it keeps the default of storing references only.
        A schema-only backup has no data to copy.
        """
        backup_meta = self._backup_with_cloud_conf(cloud_conf, schema_only)

        assert backup_meta.cloud_storage.data_copied is data_copied


class TestBackupCloudStorageData:
    """
    Tests for TableBackup._backup_cloud_storage_data.
    """

    # pylint: disable=protected-access,no-member

    @staticmethod
    def _make_table(disks: list[Disk]) -> Table:
        return Table(
            "db1",
            "table1",
            "MergeTree",
            disks,
            [os.path.join(disk.path, "store/abc/abcdef") for disk in disks],
            "",
            "",
            UUID,
        )

    @staticmethod
    def _make_context(
        has_frozen_data: bool = True, keep_going: bool = False
    ) -> tuple[BackupContext, MagicMock, MagicMock]:
        """Helper: build a context with mocked layout and backup metadata."""
        context = Mock(spec=BackupContext)
        context.config = {"keep_going": keep_going}
        context.backup_layout = MagicMock()
        context.backup_layout.has_frozen_cloud_storage_data.return_value = (
            has_frozen_data
        )
        context.backup_meta = MagicMock()
        return context, context.backup_layout, context.backup_meta.cloud_storage

    @staticmethod
    def _backup_cloud_storage(
        context: BackupContext, table: Table, backup_disks: MagicMock | None = None
    ) -> None:
        """Helper: copy data of a table through the pool and upload its metadata."""
        with ThreadExecPool(1) as pool:
            TableBackup._backup_cloud_storage_data(context, pool, table, backup_disks)
            TableBackup._upload_cloud_storage_metadata(context, pool)

    def test_metadata_is_uploaded_from_the_disk_itself_without_copying(self):
        """
        Without backup disks nothing is copied and metadata is read from the
        disk holding the frozen data.
        """
        disk = Disk("s3", "/var/lib/clickhouse/disks/s3/", "s3")
        context, layout, cloud_storage = self._make_context()

        self._backup_cloud_storage(context, self._make_table([disk]))

        upload_kwargs = layout.upload_cloud_storage_metadata.call_args.kwargs
        assert upload_kwargs["source_disk"] is None
        cloud_storage.add_disk.assert_called_once_with("s3")

    def test_data_is_copied_and_metadata_is_read_from_the_backup_disk(self):
        """
        With backup disks the data is copied first and metadata of the copies
        is uploaded instead of the frozen one.
        """
        disk = Disk("s3", "/var/lib/clickhouse/disks/s3/", "s3")
        backup_disk = Disk("s3_backup", "/var/lib/clickhouse/disks/s3_backup/", "s3")
        backup_disks = MagicMock()
        backup_disks.copy_table_data.return_value = backup_disk
        context, layout, cloud_storage = self._make_context()
        table = self._make_table([disk])

        self._backup_cloud_storage(context, table, backup_disks)

        backup_disks.copy_table_data.assert_called_once_with("s3", table)
        upload_kwargs = layout.upload_cloud_storage_metadata.call_args.kwargs
        assert upload_kwargs["source_disk"] is backup_disk
        cloud_storage.add_disk.assert_called_once_with("s3")

    def test_nothing_is_copied_when_no_data_is_frozen(self):
        """
        Copying an empty shadow directory would fail, so the check must happen
        before the copy.
        """
        disk = Disk("s3", "/var/lib/clickhouse/disks/s3/", "s3")
        backup_disks = MagicMock()
        context, layout, cloud_storage = self._make_context(has_frozen_data=False)

        self._backup_cloud_storage(context, self._make_table([disk]), backup_disks)

        backup_disks.copy_table_data.assert_not_called()
        layout.upload_cloud_storage_metadata.assert_not_called()
        cloud_storage.add_disk.assert_not_called()

    def test_failed_copy_is_not_silently_ignored(self):
        """
        clickhouse-disks reports copy errors with a zero exit code, so a copy
        that produced no metadata must fail the backup.
        """
        disk = Disk("s3", "/var/lib/clickhouse/disks/s3/", "s3")
        backup_disk = Disk("s3_backup", "/var/lib/clickhouse/disks/s3_backup/", "s3")
        backup_disks = MagicMock()
        backup_disks.copy_table_data.return_value = backup_disk
        context, layout, cloud_storage = self._make_context()
        layout.has_frozen_cloud_storage_data.side_effect = (
            lambda _meta, checked_disk, _table: checked_disk is disk
        )

        with pytest.raises(ClickhouseBackupError):
            self._backup_cloud_storage(context, self._make_table([disk]), backup_disks)

        layout.upload_cloud_storage_metadata.assert_not_called()
        cloud_storage.add_disk.assert_not_called()

    def test_failed_copy_leaves_the_table_without_data_when_keeping_going(self):
        """
        The metadata of a table whose copy failed is not uploaded, and the table
        is reported on, since a worker thread must not touch backup metadata.
        """
        disk = Disk("s3", "/var/lib/clickhouse/disks/s3/", "s3")
        backup_disks = MagicMock()
        backup_disks.copy_table_data.side_effect = ClickHouseDisksException("failed")
        context, layout, cloud_storage = self._make_context(keep_going=True)
        table = self._make_table([disk])

        self._backup_cloud_storage(context, table, backup_disks)

        assert table.data_skipped_reason == DataSkippedReason.BACKUP_FAILED
        layout.upload_cloud_storage_metadata.assert_not_called()
        cloud_storage.add_disk.assert_not_called()
        context.backup_meta.skip_table_data.assert_called_once_with(  # type: ignore[attr-defined]
            table.database, table.name, DataSkippedReason.BACKUP_FAILED
        )
        layout.delete_data_parts.assert_called_once()

    def test_failed_scheduling_drops_copies_already_submitted(self):
        """
        A disk whose copy was submitted before scheduling failed on another disk
        must not be claimed by the backup, which has no data of the table.
        """
        disk = Disk("s3", "/var/lib/clickhouse/disks/s3/", "s3")
        broken_disk = Disk("s3_broken", "/var/lib/clickhouse/disks/s3_broken/", "s3")
        context, layout, cloud_storage = self._make_context(keep_going=True)

        def has_frozen_data(_meta, checked_disk, _table):
            """Report frozen data on every disk, failing to read the broken one."""
            if checked_disk is broken_disk:
                raise OSError("Input/output error")
            return True

        layout.has_frozen_cloud_storage_data.side_effect = has_frozen_data
        table = self._make_table([disk, broken_disk])

        with (
            ThreadExecPool(1) as pool,
            patch.object(TableBackup, "_backup_frozen_table_data", return_value=None),
        ):
            TableBackup()._backup_table_data(
                context, table, "backup1", MagicMock(), pool
            )
            TableBackup._upload_cloud_storage_metadata(context, pool)

        assert table.data_skipped_reason == DataSkippedReason.BACKUP_FAILED
        layout.upload_cloud_storage_metadata.assert_not_called()
        cloud_storage.add_disk.assert_not_called()

    def test_local_disks_are_skipped(self):
        """
        Only cloud storage disks are backed up here.
        """
        disk = Disk("default", "/var/lib/clickhouse/", "local")
        backup_disks = MagicMock()
        context, layout, _ = self._make_context()

        self._backup_cloud_storage(context, self._make_table([disk]), backup_disks)

        backup_disks.copy_table_data.assert_not_called()
        layout.upload_cloud_storage_metadata.assert_not_called()

    def test_cached_disks_are_skipped(self):
        """
        Data of a cached disk is handled through the disk behind the cache.
        """
        disk = Disk(
            "s3", "/var/lib/clickhouse/disks/s3/", "s3", cache_path="/var/cache/s3"
        )
        backup_disks = MagicMock()
        context, layout, _ = self._make_context()

        self._backup_cloud_storage(context, self._make_table([disk]), backup_disks)

        backup_disks.copy_table_data.assert_not_called()
        layout.upload_cloud_storage_metadata.assert_not_called()


class TestCloudStorageCopyPool:
    """
    Tests for parallel copying of cloud storage data into the backup.
    """

    _DISK = Disk("s3", "/var/lib/clickhouse/disks/s3/", "s3")
    _BACKUP_DISK = Disk("s3_backup", "/var/lib/clickhouse/disks/s3_backup/", "s3")

    @classmethod
    def _make_tables(cls, count: int) -> list[Table]:
        return [
            Table(
                "db1",
                f"table{num}",
                "MergeTree",
                [cls._DISK],
                [os.path.join(cls._DISK.path, f"store/abc/table{num}")],
                f"/var/lib/clickhouse/metadata/db1/table{num}.sql",
                "",
                UUID,
            )
            for num in range(count)
        ]

    @classmethod
    def _run_backup(
        cls,
        backup_disks: MagicMock,
        table_count: int = 2,
        ch_ctl: Mock | None = None,
    ) -> BackupContext:
        """Helper: back up tables storing their data on a cloud storage disk."""
        tables = cls._make_tables(table_count)

        context = _make_backup_context({"copy_data": True})
        ch_ctl = MagicMock() if ch_ctl is None else ch_ctl
        context.ch_ctl = ch_ctl
        ch_ctl.get_tables.return_value = tables
        ch_ctl.get_disks.return_value = {}
        ch_ctl.scan_frozen_parts.return_value = []
        context.backup_layout.has_frozen_cloud_storage_data.return_value = True  # type: ignore[attr-defined]

        change_time = Mock(
            side_effect=lambda path: TableMetadataChangeTime(
                path, mtime_ns=1, ctime_ns=1
            )
        )
        with (
            patch.object(TableBackup, "_get_change_time", change_time),
            patch("ch_backup.logic.table.Path"),
            patch("ch_backup.logic.table.ClickHouseBackupDisks") as backup_disks_class,
        ):
            backup_disks_class.return_value.__enter__.return_value = backup_disks
            TableBackup().backup(
                context,
                [DATABASE],
                {"db1": [table.name for table in tables]},
                schema_only=False,
                multiprocessing_config=DEFAULT_CONFIG["multiprocessing"],  # type: ignore[arg-type]
            )

        return context

    def test_data_of_every_table_is_copied(self):
        """
        Every table is copied on its own, so that copies can go in parallel.
        """
        backup_disks = MagicMock()
        backup_disks.copy_table_data.return_value = self._BACKUP_DISK

        context = self._run_backup(backup_disks, table_count=3)

        assert backup_disks.copy_table_data.call_count == 3
        upload = context.backup_layout.upload_cloud_storage_metadata
        assert upload.call_count == 3  # type: ignore[attr-defined]
        assert context.backup_meta.cloud_storage.disks == ["s3"]

    def test_copies_of_different_tables_run_in_parallel(self):
        """
        A copy must not wait for the previous one to complete.

        Copies meet at a barrier, so a sequential implementation hangs there
        until the barrier breaks by timeout.
        """
        barrier = threading.Barrier(2, timeout=10)
        backup_disks = MagicMock()
        backup_disks.copy_table_data.side_effect = lambda *_: (
            barrier.wait(),
            self._BACKUP_DISK,
        )[1]

        self._run_backup(backup_disks, table_count=2)

        assert backup_disks.copy_table_data.call_count == 2

    def test_freezed_data_is_removed_after_the_copies(self):
        """
        Removing frozen data of a database must wait for copies of its tables.
        """
        calls: list[str] = []
        backup_disks = MagicMock()
        backup_disks.copy_table_data.side_effect = lambda *_: (
            calls.append("copy"),  # type: ignore[func-returns-value]
            self._BACKUP_DISK,
        )[1]
        ch_ctl = Mock()
        ch_ctl.remove_freezed_data.side_effect = lambda *args: calls.append(
            "remove_table" if args else "remove_all"
        )

        self._run_backup(backup_disks, table_count=2, ch_ctl=ch_ctl)

        assert calls.count("copy") == 2
        assert calls[-1] == "remove_all"

    def test_failed_copy_fails_the_backup(self):
        """
        A copy failing in the pool must not be lost.
        """
        backup_disks = MagicMock()
        backup_disks.copy_table_data.side_effect = ClickHouseDisksException(
            "copy failed"
        )

        with pytest.raises(ClickHouseDisksException):
            self._run_backup(backup_disks, table_count=2)


class TestCloudStorageDeduplication:
    """
    Tests for deduplication of parts stored on cloud storage disks.
    """

    # pylint: disable=protected-access

    _BACKUP_NAME = "20181017T210300"
    _DISK = Disk("s3", "/var/lib/clickhouse/disks/s3/", "s3")
    _OTHER_DISK = Disk("s3_second", "/var/lib/clickhouse/disks/s3_second/", "s3")

    @classmethod
    def _make_table(cls) -> Table:
        disks = [cls._DISK, cls._OTHER_DISK]
        return Table(
            "db1",
            "table1",
            "MergeTree",
            disks,
            [os.path.join(disk.path, "store/abc/abcdef") for disk in disks],
            "/var/lib/clickhouse/metadata/db1/table1.sql",
            "",
            UUID,
        )

    @classmethod
    def _make_frozen_part(cls, name: str, disk: Disk) -> FrozenPart:
        return FrozenPart(
            "db1",
            "table1",
            name,
            disk.name,
            os.path.join(
                disk.path, "shadow", cls._BACKUP_NAME, "store/abc/abcdef", name
            ),
            "checksum",
            1024,
            ["checksums.txt"],
        )

    @classmethod
    def _make_context(cls, copy_data: bool = True) -> BackupContext:
        """Helper: build a context backing up a table stored on cloud storage."""
        context = _make_backup_context()
        context.backup_meta.add_table(TableMetadata("db1", "table1", "MergeTree", UUID))
        context.backup_meta.cloud_storage.data_copied = copy_data
        return context

    @staticmethod
    def _make_deduplicated_part(name: str, disk_name: str) -> PartMetadata:
        return PartMetadata(
            database="db1",
            table="table1",
            name=name,
            checksum="checksum",
            size=1024,
            files=["checksums.txt"],
            tarball=True,
            link="20181016T210300",
            disk_name=disk_name,
        )

    @classmethod
    def _backup_frozen_parts(
        cls, context: BackupContext, frozen_parts: list[FrozenPart], deduplicated: dict
    ) -> tuple[Mock | None, MagicMock]:
        """
        Helper: back up given frozen parts with a fixed deduplication result.

        Returns the backup disks and the deduplication call.
        """
        context.ch_ctl.scan_frozen_parts.side_effect = lambda _table, disk, *_, **__: [  # type: ignore[attr-defined]
            part for part in frozen_parts if part.disk_name == disk.name
        ]
        backup_disks = Mock() if context.backup_meta.cloud_storage.data_copied else None
        with patch(
            "ch_backup.logic.table.deduplicate_parts", return_value=deduplicated
        ) as deduplicate:
            TableBackup()._backup_frozen_table_data(
                context, cls._make_table(), cls._BACKUP_NAME, backup_disks
            )
        return backup_disks, deduplicate

    def test_deduplicated_part_is_linked_and_excluded_from_the_copy(self):
        """
        A deduplicated part must be left out of the copy of the table data.
        """
        context = self._make_context()
        frozen_part = self._make_frozen_part("all_1_1_0", self._DISK)

        backup_disks, _ = self._backup_frozen_parts(
            context,
            [frozen_part],
            {"all_1_1_0": self._make_deduplicated_part("all_1_1_0", "s3")},
        )

        disks, removed = backup_disks.exclude_frozen_parts.call_args.args  # type: ignore[union-attr]
        assert removed == [frozen_part]
        assert disks[frozen_part.disk_name] is self._DISK
        part = next(iter(context.backup_meta.get_tables("db1")[0].get_parts()))
        assert part.link == "20181016T210300"
        assert context.backup_meta.cloud_storage.disks == ["s3"]

    def test_part_that_is_not_deduplicated_stays_in_shadow(self):
        """
        A part that has to be copied must keep its frozen data.
        """
        context = self._make_context()

        backup_disks, _ = self._backup_frozen_parts(
            context, [self._make_frozen_part("all_1_1_0", self._DISK)], {}
        )

        assert not backup_disks.exclude_frozen_parts.call_args.args[1]  # type: ignore[union-attr]
        part = next(iter(context.backup_meta.get_tables("db1")[0].get_parts()))
        assert part.link is None
        assert context.backup_meta.cloud_storage.disks == ["s3"]

    def test_match_on_another_disk_is_ignored(self):
        """
        Data of a part is bound to its disk, so a match on another disk is not
        the same data.
        """
        context = self._make_context()
        frozen_part = self._make_frozen_part("all_1_1_0", self._DISK)

        backup_disks, _ = self._backup_frozen_parts(
            context,
            [frozen_part],
            {"all_1_1_0": self._make_deduplicated_part("all_1_1_0", "s3_second")},
        )

        assert not backup_disks.exclude_frozen_parts.call_args.args[1]  # type: ignore[union-attr]
        part = next(iter(context.backup_meta.get_tables("db1")[0].get_parts()))
        assert part.link is None

    def test_parts_are_not_deduplicated_without_copying(self):
        """
        Without copying, a part on a cloud storage disk is a reference to the
        source bucket and has nothing to be deduplicated against.
        """
        context = self._make_context(copy_data=False)

        _, deduplicate = self._backup_frozen_parts(
            context, [self._make_frozen_part("all_1_1_0", self._DISK)], {}
        )

        deduplicate.assert_not_called()
        part = next(iter(context.backup_meta.get_tables("db1")[0].get_parts()))
        assert part.link is None


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
        f"{_PLAIN_STATEMENT} COMMENT 'UNIQUE KEY (id)",
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


class TestDownloadPartsOfOtherReplicas:
    """
    Tests for downloading parts from another backup before SYSTEM RESTORE REPLICA.
    """

    # pylint: disable=protected-access

    _DB = "db1"
    _TABLE = "table1"
    _STATEMENT = (
        f"CREATE TABLE {_DB}.{_TABLE} UUID '{UUID}' (id UInt64) ENGINE = "
        "ReplicatedMergeTree('/clickhouse/tables/table1', '{replica}') ORDER BY id"
    )

    @classmethod
    def _parts_backup(cls) -> BackupMetadata:
        """Helper: a backup with parts on a local disk and on object storage."""
        backup = _make_backup_context().backup_meta
        table = TableMetadata(cls._DB, cls._TABLE, "ReplicatedMergeTree", UUID)
        for name, disk in [
            ("all_1_1_0", "default"),
            ("all_2_2_0", "default"),
            ("all_3_3_0", "s3"),
        ]:
            table.add_part(
                PartMetadata(
                    database=cls._DB,
                    table=cls._TABLE,
                    name=name,
                    checksum="abc123",
                    size=1024,
                    files=["data.bin"],
                    tarball=True,
                    disk_name=disk,
                )
            )
        backup.add_table(table)
        return backup

    @classmethod
    def _setup(
        cls,
        parts_backup: BackupMetadata | None,
        detached: Path,
        *,
        download_error: Exception | None = None,
        check_error: Exception | None = None,
        active_parts: set[str] | None = None,
        lookup_error: Exception | None = None,
    ) -> tuple[MagicMock, BackupContext, Table]:
        """Helper: a context with the table and the mock that sees all calls."""
        calls = MagicMock()
        calls.backup_layout.download_data_part.side_effect = download_error or (
            lambda _backup, _part, path, callback: os.makedirs(path)
        )
        calls.check_part_files.side_effect = check_error
        context = BackupContext(DEFAULT_CONFIG)  # type: ignore[arg-type]
        context.ch_ctl = calls.ch_ctl
        context.zk_ctl = calls.zk_ctl
        context.backup_layout = calls.backup_layout
        context.parts_backup_meta = parts_backup
        ch_table = MagicMock()
        ch_table.paths_with_disks = [
            ("", Disk("default", "", "local")),
            ("", Disk("s3", "", "s3")),
        ]
        calls.ch_ctl.get_table.return_value = ch_table
        calls.ch_ctl.get_parts_of_other_replicas.return_value = {
            "all_1_1_0",
            "all_3_3_0",
        }
        calls.ch_ctl.get_parts_of_other_replicas.side_effect = lookup_error
        calls.ch_ctl.get_active_parts.return_value = active_parts or set()
        calls.ch_ctl.get_detached_part_path.side_effect = (
            lambda _table, _disk, name: str(detached / name)
        )
        table = Table(
            cls._DB, cls._TABLE, "ReplicatedMergeTree", [], [], "", cls._STATEMENT, UUID
        )
        return calls, context, table

    @classmethod
    def _restore(
        cls,
        parts_backup: BackupMetadata | None,
        detached: Path,
        *,
        download_error: Exception | None = None,
        check_error: Exception | None = None,
        active_parts: set[str] | None = None,
        lookup_error: Exception | None = None,
    ) -> MagicMock:
        """Helper: restore the table object and return the mock that saw all calls."""
        calls, context, table = cls._setup(
            parts_backup,
            detached,
            download_error=download_error,
            check_error=check_error,
            active_parts=active_parts,
            lookup_error=lookup_error,
        )
        with patch("ch_backup.logic.table.check_part_files", calls.check_part_files):
            TableBackup._restore_table_object(context, DATABASE, table)
        return calls

    def test_only_parts_of_other_replicas_on_local_disks_are_downloaded_before_restore_replica(
        self, tmp_path: Path
    ) -> None:
        """
        Only local parts of replicas are downloaded, and checked before the clone.
        """
        calls = self._restore(self._parts_backup(), tmp_path)

        names = [name for name, _, _ in calls.mock_calls]
        downloaded = [
            args[1].name
            for name, args, _ in calls.mock_calls
            if name == "backup_layout.download_data_part"
        ]
        assert downloaded == ["all_1_1_0"]
        calls.backup_layout.wait.assert_called_once_with(keep_going=True)
        calls.check_part_files.assert_called_once_with(str(tmp_path / "all_1_1_0"))
        assert (tmp_path / "all_1_1_0").exists()
        assert (
            names.index("backup_layout.wait")
            < names.index("check_part_files")
            < names.index("ch_ctl.restore_replica")
        )

    def test_part_active_in_table_is_not_downloaded(self, tmp_path: Path) -> None:
        """
        SYSTEM RESTORE REPLICA moves an active part to detached/ under the same name.
        """
        calls = self._restore(
            self._parts_backup(), tmp_path, active_parts={"all_1_1_0"}
        )

        calls.backup_layout.download_data_part.assert_not_called()
        calls.ch_ctl.restore_replica.assert_called_once()

    def test_parts_are_downloaded_before_restore_replica_of_readonly_table(
        self, tmp_path: Path
    ) -> None:
        """
        A readonly table fixed in place gets the parts before RESTORE REPLICA.
        """
        calls, context, table = self._setup(self._parts_backup(), tmp_path)
        calls.ch_ctl.get_replicas.side_effect = [
            [{"database": self._DB, "table": self._TABLE}],
            [],
        ]

        with patch("ch_backup.logic.table.check_part_files", calls.check_part_files):
            (
                still_readonly,
                _,
            ) = TableBackup()._get_remaining_readonly_tables_after_fix_attempt(
                context, [table], calls.metadata_cleaner
            )

        names = [name for name, _, _ in calls.mock_calls]
        assert not still_readonly
        assert (
            names.index("metadata_cleaner.clean_tables_metadata")
            < names.index("backup_layout.download_data_part")
            < names.index("ch_ctl.restore_replica")
        )

    def test_failed_choice_of_parts_is_left_to_replicas(self, tmp_path: Path) -> None:
        """
        A failure to reach ZooKeeper of ch-backup must not fail the restore.
        """
        calls = self._restore(
            self._parts_backup(), tmp_path, lookup_error=RuntimeError("x")
        )

        calls.backup_layout.download_data_part.assert_not_called()
        calls.ch_ctl.drop_table_if_exists.assert_not_called()
        calls.ch_ctl.restore_replica.assert_called_once()

    def test_failed_choice_of_parts_does_not_skip_restore_replica_of_readonly_table(
        self, tmp_path: Path
    ) -> None:
        """
        A readonly table is still fixed in place, not recreated.
        """
        calls, context, table = self._setup(
            self._parts_backup(), tmp_path, lookup_error=RuntimeError("x")
        )
        calls.ch_ctl.get_replicas.side_effect = [
            [{"database": self._DB, "table": self._TABLE}],
            [],
        ]

        (
            still_readonly,
            _,
        ) = TableBackup()._get_remaining_readonly_tables_after_fix_attempt(
            context, [table], calls.metadata_cleaner
        )

        assert not still_readonly
        calls.backup_layout.download_data_part.assert_not_called()
        calls.ch_ctl.restore_replica.assert_called_once()

    def test_part_already_in_detached_is_not_touched(self, tmp_path: Path) -> None:
        """
        A directory already in detached/ is neither overwritten nor removed.
        """
        (tmp_path / "all_1_1_0").mkdir()
        (tmp_path / "all_1_1_0" / "data.bin").write_bytes(b"x")

        calls = self._restore(
            self._parts_backup(), tmp_path, check_error=ValueError("x")
        )

        calls.backup_layout.download_data_part.assert_not_called()
        calls.check_part_files.assert_not_called()
        assert (tmp_path / "all_1_1_0" / "data.bin").read_bytes() == b"x"
        calls.ch_ctl.restore_replica.assert_called_once()

    def test_failed_download_is_left_to_replicas(self, tmp_path: Path) -> None:
        """
        An error raised before the download is queued must not fail the restore.
        """
        calls = self._restore(
            self._parts_backup(), tmp_path, download_error=StorageError("x")
        )

        calls.backup_layout.wait.assert_called_once_with(keep_going=True)
        calls.check_part_files.assert_not_called()
        calls.ch_ctl.restore_replica.assert_called_once()

    def test_part_that_does_not_match_its_checksums_is_left_to_replicas(
        self, tmp_path: Path
    ) -> None:
        """
        A rotten part is removed, as ClickHouse would take it.
        """
        calls = self._restore(
            self._parts_backup(), tmp_path, check_error=ValueError("x")
        )

        assert not (tmp_path / "all_1_1_0").exists()
        calls.ch_ctl.restore_replica.assert_called_once()

    def test_nothing_is_downloaded_without_parts_backup(self, tmp_path: Path) -> None:
        """
        Without the option the replica is restored exactly as before.
        """
        calls = self._restore(None, tmp_path)

        calls.backup_layout.download_data_part.assert_not_called()
        calls.ch_ctl.get_parts_of_other_replicas.assert_not_called()
        calls.ch_ctl.restore_replica.assert_called_once()


def _upload_part(fail: bool) -> None:
    """Helper: stand for the upload of a data part, failing it if asked."""
    if fail:
        raise OSError("Input/output error")


def _upload_parts_on_pool(
    layout: MagicMock, pool: ThreadExecPool, failing: Sequence[str]
) -> None:
    """
    Helper: run uploads of data parts on a pool awaited by wait() of the layout.

    An upload of a part named in failing fails.
    """

    def upload(_meta, fpart, callback, on_error=None):
        """Submit the upload of a part to the pool."""
        pool.submit(
            fpart.name,
            _upload_part,
            fpart.name in failing,
            callback=callback,
            on_error=on_error,
        )

    layout.upload_data_part.side_effect = upload
    layout.wait.side_effect = pool.wait_all


class TestKeepGoing:
    """
    Tests that a part or a table whose data cannot be backed up is left out of
    the backup when keeping going, instead of failing the whole backup.
    """

    _TABLE = "table1"
    _STATEMENT = (
        f"ATTACH TABLE db1.{_TABLE} UUID '{UUID}' (id UInt64) ENGINE = MergeTree"
    )
    _DISK = Disk("default", "/var/lib/clickhouse/", "Local")
    _READ_ERROR = OSError("Input/output error")

    @classmethod
    def _table(cls) -> Table:
        return Table(
            "db1",
            cls._TABLE,
            "MergeTree",
            [cls._DISK],
            [f"/var/lib/clickhouse/data/db1/{cls._TABLE}"],
            f"/var/lib/clickhouse/metadata/db1/{cls._TABLE}.sql",
            "",
            UUID,
        )

    @classmethod
    def _part(
        cls,
        table_name: str,
        disk_name: str,
        name: str = "all_1_1_0",
        link: str | None = None,
    ) -> PartMetadata:
        """Helper: build metadata of a part of the given table and disk."""
        return PartMetadata(
            database="db1",
            table=table_name,
            name=name,
            checksum="checksum",
            size=1024,
            files=["checksums.txt"],
            tarball=True,
            disk_name=disk_name,
            link=link,
        )

    @classmethod
    def _frozen_part(cls, name: str) -> FrozenPart:
        """Helper: build a frozen part of the table on the local disk."""
        return FrozenPart(
            "db1",
            cls._TABLE,
            name,
            cls._DISK.name,
            f"/var/lib/clickhouse/shadow/{BACKUP_NAME}/data/db1/{cls._TABLE}/{name}",
            "checksum",
            1024,
            ["checksums.txt"],
        )

    @classmethod
    def _backup(
        cls,
        keep_going: bool,
        freeze_error: Exception | None = None,
        scan: object = None,
        failing_uploads: Sequence[str] = (),
        upload: object = None,
    ) -> BackupContext:
        """
        Helper: back up a table, by default one whose frozen data cannot be read.

        Uploads run on a pool awaited by wait(), and those of failing_uploads
        fail there, unless upload replaces the upload of a part.
        """
        context = _make_backup_context()
        context.config["keep_going"] = keep_going
        context.config["deduplication_batch_size"] = 1
        context.ch_ctl.get_tables.return_value = [cls._table()]  # type: ignore[attr-defined]
        context.ch_ctl.freeze_table.side_effect = freeze_error  # type: ignore[attr-defined]
        context.ch_ctl.scan_frozen_parts.side_effect = scan or cls._READ_ERROR  # type: ignore[attr-defined]
        with (
            ThreadExecPool(1) as pool,
            patch.object(
                TableBackup, "_get_change_time", return_value=_METADATA_UNCHANGED
            ),
            patch.object(
                TableBackup,
                "_load_create_statement_from_disk",
                return_value=cls._STATEMENT,
            ),
            patch("ch_backup.logic.table.deduplicate_parts", return_value={}),
        ):
            _upload_parts_on_pool(context.backup_layout, pool, failing_uploads)  # type: ignore[arg-type]
            if upload:
                context.backup_layout.upload_data_part.side_effect = upload  # type: ignore[attr-defined]
            TableBackup().backup(
                context,
                [DATABASE],
                {"db1": [cls._TABLE]},
                schema_only=False,
                multiprocessing_config=DEFAULT_CONFIG["multiprocessing"],  # type: ignore[arg-type]
            )

        return context

    def test_unreadable_data_fails_the_backup_by_default(self) -> None:
        """A backup that silently loses data is worse than one that fails."""
        with pytest.raises(OSError):
            self._backup(keep_going=False)

    def test_unreadable_data_leaves_the_table_without_data(self) -> None:
        """The schema is backed up and the metadata says why there is no data."""
        context = self._backup(keep_going=True)

        table_meta = context.backup_meta.get_tables("db1")[0]
        assert table_meta.data_skipped_reason == DataSkippedReason.BACKUP_FAILED
        assert not table_meta.get_parts()
        context.backup_layout.upload_create_statements.assert_called_once()  # type: ignore[attr-defined]

    def test_failed_freeze_leaves_the_table_without_data(self) -> None:
        """
        A table that cannot be frozen keeps its schema in the backup, instead of
        disappearing from it along with the failed job.
        """
        context = self._backup(keep_going=True, freeze_error=_FREEZE_ERROR)

        table_meta = context.backup_meta.get_tables("db1")[0]
        assert table_meta.data_skipped_reason == DataSkippedReason.BACKUP_FAILED
        upload = context.backup_layout.upload_create_statements
        assert upload.call_args.args[2] == [  # type: ignore[attr-defined]
            (self._TABLE, self._STATEMENT)
        ]

    def test_part_that_fails_to_upload_is_left_out(self) -> None:
        """The other parts of the table stay in the backup."""
        context = self._backup(
            keep_going=True,
            scan=lambda *_, **__: [
                self._frozen_part("all_1_1_0"),
                self._frozen_part("all_2_2_0"),
            ],
            failing_uploads=["all_2_2_0"],
        )

        table_meta = context.backup_meta.get_tables("db1")[0]
        assert table_meta.data_skipped_reason is None
        assert [part.name for part in table_meta.get_parts()] == ["all_1_1_0"]
        assert context.backup_meta.get_skipped_data_tables() == (
            [],
            [f"db1.{self._TABLE}"],
        )
        raw_table = context.backup_meta.dump()["databases"]["db1"]["tables"]
        assert raw_table[self._TABLE]["skipped_parts"] == ["all_2_2_0"]
        deleted = context.backup_layout.delete_data_parts.call_args  # type: ignore[attr-defined]
        assert [part.name for part in deleted.args[1]] == ["all_2_2_0"]
        assert deleted.kwargs == {"is_async": False}

    def test_failed_deletion_of_a_skipped_part_fails_the_backup(self) -> None:
        """Data left out must not be left in storage unnoticed."""
        context = _make_backup_context()
        context.backup_layout.delete_data_parts.side_effect = StorageError("denied")  # type: ignore[attr-defined]
        part = PartMetadata.from_frozen_part(self._frozen_part("all_1_1_0"), True)

        with pytest.raises(StorageError):
            try:
                raise OSError("Input/output error")
            except OSError as e:
                TableBackup._skip_failed_part(  # pylint: disable=protected-access
                    context, self._table(), part.name, e, uploaded=part
                )

    @pytest.mark.parametrize(
        "error",
        [BrokenProcessPool("worker died"), TerminatingSignal()],
        ids=["broken-pool", "terminating-signal"],
    )
    def test_fatal_errors_are_not_recorded_as_skipped_parts(
        self, error: BaseException
    ) -> None:
        """The backup fails on them, so nothing is left out on purpose."""
        context = _make_backup_context()
        context.backup_meta.add_table(
            TableMetadata("db1", self._TABLE, "MergeTree", UUID)
        )

        TableBackup._skip_failed_part(  # pylint: disable=protected-access
            context, self._table(), "all_1_1_0", error
        )

        assert context.backup_meta.count_skipped_data_tables() == (0, 0)
        context.backup_layout.delete_data_parts.assert_not_called()  # type: ignore[attr-defined]

    @classmethod
    def _two_parts(cls, *_: object, **__: object) -> list[FrozenPart]:
        """Helper: scan two parts of the table."""
        return [cls._frozen_part("all_1_1_0"), cls._frozen_part("all_2_2_0")]

    @staticmethod
    def _upload_failing_at_once(
        error: Exception, call_handler: bool = True
    ) -> Callable[..., None]:
        """
        Helper: make the upload of the broken part fail at once, as it does
        without upload workers.
        """

        def upload(_meta, fpart, callback, on_error=None):
            """Upload a part, failing on the broken one before returning."""
            if fpart.name != "all_2_2_0":
                callback()
                return
            try:
                raise error
            except Exception as e:
                if on_error and call_handler:
                    on_error(e)
                raise StorageError("Failed to create async upload") from e

        return upload

    def test_failed_upload_without_workers_leaves_out_the_part_only(self) -> None:
        """Without upload workers a failed upload raises at once."""
        context = self._backup(
            keep_going=True,
            scan=self._two_parts,
            upload=self._upload_failing_at_once(OSError("Input/output error")),
        )

        table_meta = context.backup_meta.get_tables("db1")[0]
        assert table_meta.data_skipped_reason is None
        assert [part.name for part in table_meta.get_parts()] == ["all_1_1_0"]

    def test_broken_upload_pool_fails_the_backup_when_keeping_going(self) -> None:
        """Every table after it would be left without data otherwise."""
        with pytest.raises(StorageError):
            self._backup(
                keep_going=True,
                scan=self._two_parts,
                upload=self._upload_failing_at_once(BrokenProcessPool("died")),
            )

    def test_upload_error_not_taken_by_the_handler_is_not_ignored(self) -> None:
        """
        A part neither uploaded nor recorded as left out would make the backup
        look complete without it.
        """
        context = self._backup(
            keep_going=True,
            scan=self._two_parts,
            upload=self._upload_failing_at_once(
                OSError("Input/output error"), call_handler=False
            ),
        )

        table_meta = context.backup_meta.get_tables("db1")[0]
        assert table_meta.data_skipped_reason == DataSkippedReason.BACKUP_FAILED

    def test_dropped_table_data_is_deleted_from_every_uploaded_disk(self) -> None:
        """
        Parts of disks other than s3 go through upload, so all of them are
        deleted; a deduplicated part belongs to another backup and is kept.
        """
        encrypted_disk = Disk(
            "encrypted", "/var/lib/clickhouse/disks/enc/", "Encrypted"
        )
        s3_disk = Disk("s3", "/var/lib/clickhouse/disks/s3/", "s3")
        table = Table(
            "db1",
            self._TABLE,
            "MergeTree",
            [encrypted_disk, s3_disk, self._DISK],
            [
                "/var/lib/clickhouse/disks/enc/store/abc/abcdef",
                "/var/lib/clickhouse/disks/s3/store/abc/abcdef",
                f"/var/lib/clickhouse/data/db1/{self._TABLE}",
            ],
            "",
            "",
            UUID,
        )
        context = _make_backup_context()
        context.backup_meta.add_table(
            TableMetadata("db1", self._TABLE, "MergeTree", UUID)
        )
        for name, disk_name, link in [
            ("all_1_1_0", self._DISK.name, None),
            ("all_2_2_0", encrypted_disk.name, None),
            ("all_3_3_0", s3_disk.name, None),
            ("all_4_4_0", self._DISK.name, "20181016T210300"),
        ]:
            context.backup_meta.add_part(self._part(self._TABLE, disk_name, name, link))

        TableBackup._drop_table_data(context, table)  # pylint: disable=protected-access

        deleted = context.backup_layout.delete_data_parts.call_args  # type: ignore[attr-defined]
        assert sorted(part.name for part in deleted.args[1]) == [
            "all_1_1_0",
            "all_2_2_0",
        ]
        assert deleted.kwargs == {"is_async": False}
        assert not context.backup_meta.get_tables("db1")[0].get_parts()

    def test_failed_upload_fails_the_backup_by_default(self) -> None:
        """Without keep_going a broken part is not silently left out."""
        with pytest.raises(OSError):
            self._backup(
                keep_going=False,
                scan=lambda *_, **__: [self._frozen_part("all_1_1_0")],
                failing_uploads=["all_1_1_0"],
            )

    def test_uploads_are_awaited_before_the_table_is_skipped(self) -> None:
        """
        A part uploaded before the failure must not be added to the table after
        it is skipped, and its uploaded data must not be left in storage.
        """

        def scan(*_, **__):
            """Yield one part, then fail to read the rest of the table."""
            yield self._frozen_part("all_1_1_0")
            raise self._READ_ERROR

        context = self._backup(keep_going=True, scan=scan)

        table_meta = context.backup_meta.get_tables("db1")[0]
        assert table_meta.data_skipped_reason == DataSkippedReason.BACKUP_FAILED
        assert not table_meta.get_parts()
        deleted = context.backup_layout.delete_data_parts.call_args.args[1]  # type: ignore[attr-defined]
        assert [part.name for part in deleted] == ["all_1_1_0"]

    def test_unreadable_parts_are_left_out_on_local_disks_only(self) -> None:
        """Data of a cloud storage disk is copied whole, so it is not split."""
        s3_disk = Disk("s3", "/var/lib/clickhouse/disks/s3/", "s3")
        table = Table(
            "db1",
            self._TABLE,
            "MergeTree",
            [s3_disk, self._DISK],
            [
                f"/var/lib/clickhouse/data/db1/{self._TABLE}",
                "/var/lib/clickhouse/disks/s3/store/abc/abcdef",
            ],
            "",
            "",
            UUID,
        )
        context = _make_backup_context()
        context.config["keep_going"] = True
        context.ch_ctl.scan_frozen_parts.return_value = []  # type: ignore[attr-defined]

        TableBackup()._backup_frozen_table_data(  # pylint: disable=protected-access
            context, table, BACKUP_NAME, None
        )

        handlers = {
            scan.args[1].name: scan.kwargs["on_error"]
            for scan in context.ch_ctl.scan_frozen_parts.call_args_list  # type: ignore[attr-defined]
        }
        assert handlers[self._DISK.name] is not None
        assert handlers[s3_disk.name] is None

    def test_skipped_data_counts_are_kept_in_light_metadata(self) -> None:
        """Purge and list read light metadata only."""
        context = _make_backup_context()
        context.backup_meta.time_format = DEFAULT_CONFIG["backup"]["time_format"]  # type: ignore[index]
        context.backup_meta.add_table(
            TableMetadata("db1", self._TABLE, "MergeTree", UUID)
        )
        meta = context.backup_meta.dump(light=True)["meta"]
        assert "data_skipped_tables" not in meta
        assert "incomplete_tables" not in meta

        context.backup_meta.skip_part("db1", self._TABLE, "all_1_1_0")
        light = BackupMetadata.load_json(context.backup_meta.dump_json(light=True))
        assert light.count_skipped_data_tables() == (0, 1)

        context.backup_meta.skip_table_data(
            "db1", self._TABLE, DataSkippedReason.BACKUP_FAILED
        )
        light = BackupMetadata.load_json(context.backup_meta.dump_json(light=True))
        assert light.count_skipped_data_tables() == (1, 0)

    def test_parts_collected_before_the_failure_are_dropped(self) -> None:
        """
        After a failure that is not put down to a part it is not known which of
        the parts are left out.
        """
        context = _make_backup_context()
        context.backup_meta.add_table(
            TableMetadata("db1", self._TABLE, "MergeTree", UUID)
        )
        context.backup_meta.add_part(self._part(self._TABLE, self._DISK.name))

        context.backup_meta.skip_table_data(
            "db1", self._TABLE, DataSkippedReason.BACKUP_FAILED
        )

        table_meta = context.backup_meta.get_tables("db1")[0]
        assert table_meta.data_skipped_reason == DataSkippedReason.BACKUP_FAILED
        assert not table_meta.get_parts()

    def test_cloud_disk_is_dropped_when_no_part_is_left_on_it(self) -> None:
        """
        Restore creates a temporary disk for every disk of the backup, so the
        backup must not claim a disk it holds no data on.
        """
        context = _make_backup_context()
        for table_name in (self._TABLE, "table2"):
            context.backup_meta.add_table(
                TableMetadata("db1", table_name, "MergeTree", UUID)
            )
            context.backup_meta.add_part(self._part(table_name, "s3"))
        context.backup_meta.cloud_storage.add_disk("s3")

        context.backup_meta.skip_table_data(
            "db1", self._TABLE, DataSkippedReason.BACKUP_FAILED
        )
        assert context.backup_meta.cloud_storage.disks == ["s3"]

        context.backup_meta.skip_table_data(
            "db1", "table2", DataSkippedReason.BACKUP_FAILED
        )
        assert context.backup_meta.cloud_storage.disks == []
        assert context.backup_meta.size == 0


class TestDetachedPartsBackup:
    """Detached parts are filtered and stored separately."""

    @staticmethod
    def _backup_meta() -> BackupMetadata:
        backup = BackupMetadata(
            name="backup",
            version="1",
            ch_version="25.1",
            time_format="%Y-%m-%d %H:%M:%S %z",
        )
        backup.add_database(Database("db", "Atomic", None, None, None))
        backup.add_table(TableMetadata("db", "table", "MergeTree", None))
        return backup

    @staticmethod
    def _table() -> Table:
        return Table("db", "table", "MergeTree", [], [], "", "", None)

    @staticmethod
    def _detached(
        name: str, disk: str, reason: str, object_storage: bool = False
    ) -> DetachedPart:
        return DetachedPart(
            database="db",
            table="table",
            name=name,
            reason=reason,
            disk_name=disk,
            path=f"/data/{disk}/detached/{name}",
            checksum="checksum",
            size=1024,
            files=["checksums.txt", "data.bin"],
            object_storage=object_storage,
        )

    @staticmethod
    def _backup_context(parts: list[DetachedPart]) -> MagicMock:
        context = MagicMock()
        context.backup_meta = TestDetachedPartsBackup._backup_meta()
        context.config = {
            "update_metadata_interval": 1800,
            "validate_part_after_upload": False,
        }
        context.ch_ctl.scan_detached_parts.return_value = parts

        def complete_upload(_backup, _part, callback):
            callback()

        context.backup_layout.upload_detached_part.side_effect = complete_upload
        return context

    def test_filters_disk_and_empty_reason(self) -> None:
        selected = self._detached("all_1_1_0", "default", "")
        context = self._backup_context(
            [
                selected,
                self._detached("broken_all_2_2_0", "default", "broken"),
                self._detached("all_3_3_0", "cold", ""),
            ]
        )

        TableBackup()._backup_detached_table_data(  # pylint: disable=protected-access
            context,
            self._table(),
            disk_names=["default"],
            reasons=[""],
        )

        context.backup_layout.upload_detached_part.assert_called_once()
        backed_up = context.backup_meta.get_detached_parts()
        assert [(part.disk_name, part.reason, part.name) for part in backed_up] == [
            ("default", "", "all_1_1_0")
        ]

    def test_no_filters_selects_every_detached_part(self) -> None:
        parts = [
            self._detached("all_1_1_0", "default", ""),
            self._detached("broken_all_2_2_0", "cold", "broken"),
        ]
        context = self._backup_context(parts)

        TableBackup()._backup_detached_table_data(  # pylint: disable=protected-access
            context, self._table()
        )

        assert len(context.backup_meta.get_detached_parts()) == 2
        context.backup_layout.wait.assert_called_once_with(keep_going=True)

    def test_failed_upload_is_skipped_and_successful_part_is_recorded(self) -> None:
        failed = self._detached("all_1_1_0", "default", "")
        successful = self._detached("all_2_2_0", "default", "")
        context = self._backup_context([failed, successful])

        def upload(_backup, part, callback):
            if part is failed:
                raise PermissionError("permission denied")
            callback()

        context.backup_layout.upload_detached_part.side_effect = upload

        TableBackup()._backup_detached_table_data(  # pylint: disable=protected-access
            context, self._table()
        )

        assert [part.name for part in context.backup_meta.get_detached_parts()] == [
            successful.name
        ]
        context.backup_layout.wait.assert_called_once_with(keep_going=True)

    def test_async_failed_upload_is_skipped(self) -> None:
        failed = self._detached("all_1_1_0", "default", "")
        successful = self._detached("all_2_2_0", "default", "")
        context = self._backup_context([failed, successful])

        def fail_upload() -> None:
            raise OSError(errno.EIO, "I/O error")

        def complete_upload() -> None:
            return

        with ThreadExecPool(1) as pool:

            def upload(_backup, part, callback):
                operation = fail_upload if part is failed else complete_upload
                pool.submit(part.name, operation, callback=callback)

            context.backup_layout.upload_detached_part.side_effect = upload
            context.backup_layout.wait.side_effect = pool.wait_all

            TableBackup()._backup_detached_table_data(  # pylint: disable=protected-access
                context, self._table()
            )

        assert [part.name for part in context.backup_meta.get_detached_parts()] == [
            successful.name
        ]
        context.backup_layout.wait.assert_called_once_with(keep_going=True)

    def test_changed_part_failing_validation_is_removed(self, tmp_path: Path) -> None:
        failed = self._detached("all_1_1_0", "default", "")
        successful = self._detached("all_2_2_0", "default", "")
        failed.files = ["data.bin"]
        failed.size = BLOCKSIZE
        failed.path = str(tmp_path / failed.name)
        source_file = Path(failed.path) / "data.bin"
        source_file.parent.mkdir()
        source_file.write_bytes(b"x")
        context = self._backup_context([failed, successful])
        context.config["validate_part_after_upload"] = True

        def upload(_backup, part, callback):
            if part is failed:
                source_file.write_bytes(b"x" * 513)
            callback()

        context.backup_layout.upload_detached_part.side_effect = upload

        def check(_backup_name, part):
            if part.name != failed.name:
                return True
            current_size = calc_aligned_files_size([source_file], alignment=BLOCKSIZE)
            return calc_tarball_size(part.files, part.size) == calc_tarball_size(
                part.files, current_size
            )

        context.backup_layout.check_detached_part.side_effect = check

        TableBackup()._backup_detached_table_data(  # pylint: disable=protected-access
            context, self._table()
        )

        assert source_file.stat().st_size == 513
        assert [part.name for part in context.backup_meta.get_detached_parts()] == [
            successful.name
        ]
        assert context.backup_meta.size == successful.size
        assert context.backup_meta.real_size == successful.size
        deleted_parts = context.backup_layout.delete_detached_parts.call_args.args[1]
        assert [part.name for part in deleted_parts] == [failed.name]
        assert context.backup_layout.wait.call_args_list == [
            call(keep_going=True),
            call(keep_going=True),
        ]

    def test_object_storage_kind_is_recorded_on_detached_part(self) -> None:
        context = self._backup_context(
            [self._detached("all_1_1_0", "object_storage", "", True)]
        )
        disk = Disk("object_storage", "/object_storage", "ObjectStorage", "S3")
        table = Table(
            "db",
            "table",
            "MergeTree",
            [disk],
            ["/object_storage/store/table"],
            "",
            "",
            None,
        )

        TableBackup()._backup_detached_table_data(  # pylint: disable=protected-access
            context, table
        )

        [part] = context.backup_meta.get_detached_parts()
        assert part.object_storage is True
        assert context.backup_meta.cloud_storage.disks == []

    @patch("ch_backup.logic.table.logging.warning")
    def test_full_cloud_storage_backup_warns_and_keeps_detached_metadata(
        self, warning: MagicMock
    ) -> None:
        context = self._backup_context(
            [self._detached("all_1_1_0", "object_storage", "", True)]
        )
        context.backup_meta.cloud_storage.data_copied = True
        disk = Disk("object_storage", "/object_storage", "ObjectStorage", "S3")
        table = Table(
            "db",
            "table",
            "MergeTree",
            [disk],
            ["/object_storage/store/table"],
            "",
            "",
            None,
        )

        TableBackup()._backup_detached_table_data(  # pylint: disable=protected-access
            context, table
        )

        context.backup_layout.upload_detached_part.assert_called_once()
        assert len(context.backup_meta.get_detached_parts()) == 1
        assert "only local metadata files" in warning.call_args.args[0]


@pytest.mark.parametrize("keep_going", [False, True])
@pytest.mark.parametrize("all_failed", [False, True])
@pytest.mark.parametrize("legacy", [False, True])
@pytest.mark.parametrize("skip_cloud_storage", [False, True])
# pylint: disable=too-many-locals,too-many-statements
def test_restore_skips_unreadable_database_schemas(
    keep_going: bool,
    all_failed: bool,
    legacy: bool,
    skip_cloud_storage: bool,
) -> None:
    context = _make_backup_context()
    context.restore_context = Mock(
        spec=RestoreContext,
        part_restored=Mock(return_value=False),
        part_downloaded=Mock(return_value=False),
    )
    other_db = Database("db2", "Atomic", "/metadata/db2.sql", None, None)
    context.backup_meta.add_database(other_db)
    broken = TableMetadata("db1", "existing", "MergeTree", None)
    healthy_db = "db1" if legacy else "db2"
    healthy = TableMetadata(healthy_db, "healthy", "MergeTree", None)
    for table in (broken, healthy):
        table.add_part(
            PartMetadata(
                table.database,
                table.name,
                "all_1_1_0",
                "checksum",
                512,
                ["checksums.txt"],
                True,
            )
        )
        context.backup_meta.add_table(table)
    broken.add_detached_part(
        DetachedPartMetadata(
            "db1",
            "existing",
            "all_2_2_0",
            None,
            "checksum",
            512,
            ["checksums.txt"],
            True,
            "default",
        )
    )
    existing = Table(
        "db1",
        "existing",
        "MergeTree",
        [],
        [],
        "",
        "",
        "CREATE TABLE db1.existing (x UInt64) ENGINE=MergeTree ORDER BY x",
    )
    tables = {("db1", "existing"): existing}
    controller = MagicMock()
    controller.get_tables.return_value = [existing]
    controller.explain_schema.return_value = (
        " Storage definition (children 1)\n  Function MergeTree"
    )
    controller.get_detached_tables.return_value = []
    controller.get_replicas.return_value = []
    controller.get_disks.return_value = {}
    controller.get_storage_policies.return_value = {
        "default": StoragePolicy("default", ("default",), ("default",), ("default",))
    }
    controller.get_table.side_effect = lambda db, name, **_kwargs: tables.get(
        (db, name)
    )
    controller.create_table.side_effect = lambda table: tables.__setitem__(
        (table.database, table.name), table
    )
    controller.get_detached_part_path.side_effect = lambda table, disk, name: (
        f"/data/{table.database}/{table.name}/detached/{name}"
    )
    context.ch_ctl = controller

    def download_schema(_backup, database, _tables, **_kwargs):
        if database == "db1" or all_failed:
            raise S3RetryingError("S3 GetObject failed")
        return [
            (
                "healthy",
                "CREATE TABLE db2.healthy (x UInt64) ENGINE=MergeTree ORDER BY x",
            )
        ]

    layout = MagicMock(
        get_table_create_statements=Mock(side_effect=download_schema),
        download_data_part=Mock(
            side_effect=lambda _backup, part, _path, callback: callback(part)
        ),
    )
    if legacy:
        context.config["path_root"] = "ch_backup"

        def download_legacy_schema(path, **_kwargs):
            if path.endswith("/existing.sql") or all_failed:
                raise S3RetryingError("S3 GetObject failed")
            return b"CREATE TABLE db1.healthy (x UInt64) ENGINE=MergeTree ORDER BY x"

        with patch("ch_backup.backup.layout.StorageLoader") as loader:
            loader.return_value.path_exists.return_value = False
            loader.return_value.download_data.side_effect = download_legacy_schema
            schema_layout = BackupLayout(context.config_root)
        layout.get_table_create_statements = schema_layout.get_table_create_statements
    context.backup_layout = layout
    messages: list[str] = []
    sink = logger.add(messages.append, format="{message}", level="WARNING")
    try:

        def restore():
            TableBackup().restore(
                context=context,
                databases={"db1": DATABASE, "db2": other_db},
                schema_only=False,
                tables=[],
                metadata_cleaner=None,
                cloud_storage_source_bucket=None,
                cloud_storage_source_path=None,
                cloud_storage_source_endpoint=None,
                skip_cloud_storage=skip_cloud_storage,
                restore_detached_parts=True,
                keep_going=keep_going,
                restore_tables_in_replicated_database=False,
            )

        with patch("ch_backup.logic.table.ClickHouseTemporaryDisks"):
            if keep_going:
                restore()
            else:
                with pytest.raises(S3RetryingError, match="S3 GetObject failed"):
                    restore()
    finally:
        logger.remove(sink)

    assert tables[("db1", "existing")] is existing
    controller.drop_table_if_exists.assert_not_called()
    layout.download_detached_part.assert_not_called()
    if keep_going and not all_failed:
        restored = tables[(healthy_db, "healthy")]
        controller.create_table.assert_called_once_with(restored)
        controller.attach_part.assert_called_once_with(restored, "all_1_1_0")
        layout.download_data_part.assert_called_once_with(
            context.backup_meta,
            healthy.get_parts()[0],
            f"/data/{healthy_db}/healthy/detached/all_1_1_0",
            callback=ANY,
        )
    else:
        assert (healthy_db, "healthy") not in tables
        controller.create_table.assert_not_called()
        controller.attach_part.assert_not_called()
        layout.download_data_part.assert_not_called()
    if keep_going:
        assert any(
            "db1" in message and "S3 GetObject failed" in message
            for message in messages
        )
        assert any("schema and data" in message for message in messages)
