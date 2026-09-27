from unittest.mock import MagicMock, Mock, patch

import pytest
import requests

from ch_backup.backup.metadata import BackupState, TableMetadata
from ch_backup.backup.sources import BackupSources
from ch_backup.ch_backup import ClickhouseBackup
from ch_backup.clickhouse.client import ClickhouseError
from ch_backup.clickhouse.models import Database
from ch_backup.exceptions import (
    ClickhouseBackupError,
    InvalidBackupStruct,
    StorageError,
)


def _backup_with_context(
    database_engine: str = "Atomic", table_engine: str = "MergeTree"
) -> tuple[ClickhouseBackup, Mock]:
    context = Mock()
    context.config = {"force_non_replicated": False}
    context.backup_meta.get_database.return_value = Database(
        "db", database_engine, None, None, None
    )
    context.backup_meta.get_tables.return_value = [
        TableMetadata("db", "table", table_engine, None)
    ]

    backup = ClickhouseBackup.__new__(ClickhouseBackup)
    backup.__dict__["_context"] = context
    return backup, context


def test_restore_checks_zookeeper_for_replicated_table() -> None:
    backup, context = _backup_with_context(table_engine="ReplicatedMergeTree")

    backup._check_zookeeper_for_restore(  # pylint: disable=protected-access
        BackupSources(), ["db"], []
    )

    context.ch_ctl.check_zookeeper_available.assert_called_once_with()


def test_restore_checks_zookeeper_for_replicated_database() -> None:
    backup, context = _backup_with_context(database_engine="Replicated")

    backup._check_zookeeper_for_restore(  # pylint: disable=protected-access
        BackupSources(), ["db"], []
    )

    context.ch_ctl.check_zookeeper_available.assert_called_once_with()


def test_restore_does_not_check_zookeeper_for_non_replicated_schema() -> None:
    backup, context = _backup_with_context()

    backup._check_zookeeper_for_restore(  # pylint: disable=protected-access
        BackupSources(), ["db"], []
    )

    context.ch_ctl.check_zookeeper_available.assert_not_called()


def test_restore_does_not_check_zookeeper_when_forcing_non_replicated() -> None:
    backup, context = _backup_with_context(table_engine="ReplicatedMergeTree")
    context.config["force_non_replicated"] = True

    backup._check_zookeeper_for_restore(  # pylint: disable=protected-access
        BackupSources(), ["db"], []
    )

    context.ch_ctl.check_zookeeper_available.assert_not_called()


def test_restore_checks_only_selected_tables() -> None:
    backup, context = _backup_with_context(table_engine="ReplicatedMergeTree")
    selected_tables = [TableMetadata("db", "local_table", "MergeTree", None)]

    backup._check_zookeeper_for_restore(  # pylint: disable=protected-access
        BackupSources(), ["db"], selected_tables
    )

    context.ch_ctl.check_zookeeper_available.assert_not_called()


@pytest.mark.parametrize(
    "error",
    [
        ClickhouseError("There is no Zookeeper configuration"),
        requests.exceptions.ConnectionError("ClickHouse is unreachable"),
        requests.exceptions.ReadTimeout("ClickHouse query timed out"),
    ],
)
def test_restore_reports_failed_zookeeper_check(error: Exception) -> None:
    backup, context = _backup_with_context(
        database_engine="Replicated", table_engine="ReplicatedMergeTree"
    )
    context.ch_ctl.check_zookeeper_available.side_effect = error

    with pytest.raises(ClickhouseBackupError) as exc:
        backup._check_zookeeper_for_restore(  # pylint: disable=protected-access
            BackupSources(), ["db"], []
        )

    assert str(exc.value) == (
        "Restore requires ZooKeeper or ClickHouse Keeper because we have replicated "
        "databases: `db`, tables: `db`.`table`. Availability check through "
        f"ClickHouse failed: {error}"
    )
    assert exc.value.__cause__ is error


def test_delete_without_references_uses_light_metadata() -> None:
    backup, context = _backup_with_context()
    context.locker = MagicMock()
    light_metadata = Mock(name="old", state=BackupState.CREATED)
    light_metadata.name = "old"
    context.backup_layout.get_backups.return_value = [light_metadata]

    with patch(
        "ch_backup.ch_backup.collect_dedup_references_for_batch_backup_deletion",
        return_value={"old": {}},
    ):
        assert backup.delete("old", purge_partial=False) == ("old", None)

    context.backup_layout.get_backup.assert_not_called()
    context.backup_layout.delete_backup.assert_called_once()
    assert context.backup_layout.delete_backup.call_args.args[0] == "old"


def test_delete_referenced_backup_requires_full_metadata() -> None:
    backup, context = _backup_with_context()
    context.locker = MagicMock()
    light_metadata = Mock(name="old", state=BackupState.CREATED)
    light_metadata.name = "old"
    context.backup_layout.get_backups.return_value = [light_metadata]
    context.backup_layout.get_backup.return_value = None

    with patch(
        "ch_backup.ch_backup.collect_dedup_references_for_batch_backup_deletion",
        return_value={"old": {"db": {"table": {"part"}}}},
    ):
        with pytest.raises(InvalidBackupStruct):
            backup.delete("old", purge_partial=False)

    context.backup_layout.delete_backup.assert_not_called()
    context.backup_layout.upload_backup_metadata.assert_not_called()


def test_delete_stops_when_reference_scan_fails() -> None:
    backup, context = _backup_with_context()
    context.locker = MagicMock()
    light_metadata = Mock(name="old", state=BackupState.CREATED)
    light_metadata.name = "old"
    context.backup_layout.get_backups.return_value = [light_metadata]

    with patch(
        "ch_backup.ch_backup.collect_dedup_references_for_batch_backup_deletion",
        side_effect=InvalidBackupStruct("retained metadata unavailable"),
    ):
        with pytest.raises(InvalidBackupStruct):
            backup.delete("old", purge_partial=False)

    context.backup_layout.delete_backup.assert_not_called()
    context.backup_layout.upload_backup_metadata.assert_not_called()


def test_referenced_delete_wait_failure_does_not_mark_partial() -> None:
    backup, context = _backup_with_context()
    light_metadata = Mock(name="old", state=BackupState.CREATED)
    light_metadata.name = "old"
    full_metadata = MagicMock()
    full_metadata.name = "old"
    full_metadata.get_databases.return_value = []
    context.backup_layout.get_backup.return_value = full_metadata
    context.backup_layout.wait.side_effect = StorageError("async deletion failed")

    with pytest.raises(StorageError):
        backup._delete(  # pylint: disable=protected-access
            light_metadata, {"db": {"table": {"part"}}}
        )

    assert full_metadata.state == BackupState.FAILED
    context.ch_ctl.system_unfreeze.assert_not_called()
    context.backup_layout.upload_backup_metadata.assert_called_once_with(full_metadata)
