"""
ClickhouseBackup unit tests.
"""

from collections import defaultdict
from datetime import timedelta
from typing import Callable, Sequence, cast
from unittest.mock import MagicMock, Mock, patch

import pytest
import requests

from ch_backup.backup.deduplication import DedupReferences
from ch_backup.backup.metadata import (
    BackupMetadata,
    BackupState,
    CloudStorageMetadata,
    PartMetadata,
    TableMetadata,
)
from ch_backup.backup.sources import BackupSources
from ch_backup.ch_backup import CleanZooKeeperMode, ClickhouseBackup
from ch_backup.clickhouse.client import ClickhouseError
from ch_backup.clickhouse.disks import ClickHouseTemporaryDisks
from ch_backup.clickhouse.models import Database
from ch_backup.config import DEFAULT_CONFIG
from ch_backup.exceptions import (
    ClickhouseBackupError,
    InvalidBackupStruct,
    StorageError,
)
from ch_backup.util import now


def _restore_backup_with_cloud_storage(
    data_copied: bool,
) -> tuple[MagicMock, MagicMock]:
    """Helper: restore a backup that has data on S3 disks, without a source bucket."""
    backup = ClickhouseBackup(DEFAULT_CONFIG)  # type: ignore[arg-type]
    backup.__dict__["_context"] = MagicMock()

    backup_meta = MagicMock()
    backup_meta.cloud_storage = CloudStorageMetadata(
        data_copied=data_copied, disks=["s3"]
    )
    backup_meta.get_databases.return_value = []
    backup_meta.get_skipped_data_tables.return_value = ([], [])

    sources = BackupSources.for_restore(False, False, False, False, False, False, False)
    assert sources.data

    with patch.object(ClickhouseBackup, "_get_backup", return_value=backup_meta):
        with patch.object(ClickhouseBackup, "_restore") as restore_mock:
            backup.restore(
                sources=sources,
                backup_name="backup",
                databases=[],
                exclude_databases=[],
            )
    return backup_meta, restore_mock


def test_restore_requires_source_bucket_when_data_is_not_copied():
    """
    Data left in the source bucket is unreachable without its coordinates.
    """
    try:
        _restore_backup_with_cloud_storage(data_copied=False)
        assert False, "Expected ClickhouseBackupError was not raised"
    except ClickhouseBackupError as exc:
        assert "Cloud storage source bucket" in str(exc)


def test_restore_of_copied_data_does_not_require_source_bucket():
    """
    Data copied into the backup is restored from the backup bucket.
    """
    _, restore_mock = _restore_backup_with_cloud_storage(data_copied=True)

    restore_mock.assert_called_once()
    assert restore_mock.call_args.kwargs["cloud_storage_source_bucket"] is None


SOURCES = BackupSources.for_restore(access=True, schema=True, named_collections=True)


def test_restore_from_replica_reads_schema_with_uuids_and_secrets() -> None:
    """
    Without UUIDs an inner table of a view would get a new one, and without secrets the
    statements cannot be used, so the source is queried with both settings.
    """
    backup = ClickhouseBackup(DEFAULT_CONFIG)  # type: ignore[arg-type]
    context = MagicMock()
    context.ch_ctl_conf = {"host": "local", "settings": None}
    backup.__dict__["_context"] = context

    with (
        patch("ch_backup.ch_backup.ClickhouseCTL") as ctl_cls,
        patch("ch_backup.ch_backup.ReplicaLayout") as layout_cls,
        patch.object(ClickhouseBackup, "restore") as restore_mock,
    ):
        backup.restore_from_replica("replica1", SOURCES, "old_replica")

    assert ctl_cls.call_args.args[0] == {
        "host": "replica1",
        "settings": {
            "format_display_secrets_in_show_and_select": 1,
            "show_table_uuid_in_table_create_query_if_not_nil": 1,
        },
    }
    layout_cls.assert_called_once_with(
        context.config_root,
        context.ch_ctl,
        ctl_cls.return_value,
        SOURCES,
        "old_replica",
    )
    assert context.backup_layout is layout_cls.return_value
    kwargs = restore_mock.call_args.kwargs
    assert kwargs["sources"] == SOURCES
    assert kwargs["backup_name"] == "replica1"
    assert kwargs["replica_name"] == "old_replica"
    assert kwargs["clean_zookeeper_mode"] == CleanZooKeeperMode.REPLICA_ONLY


def _delete_backup_with_cloud_storage(referenced_parts: Sequence[str]) -> MagicMock:
    """Helper: partially delete a backup holding copied cloud storage data."""
    backup = ClickhouseBackup(DEFAULT_CONFIG)  # type: ignore[arg-type]
    backup.__dict__["_context"] = MagicMock()

    part = PartMetadata(
        database="db1",
        table="table1",
        name="all_1_1_0",
        checksum="checksum",
        size=1024,
        files=["checksums.txt"],
        tarball=True,
        disk_name="s3",
    )
    table = MagicMock()
    table.database = "db1"
    table.name = "table1"
    table.get_parts.return_value = [part]

    backup_meta = MagicMock()
    backup_meta.name = "backup"
    backup_meta.cloud_storage = CloudStorageMetadata(data_copied=True, disks=["s3"])
    backup_meta.get_databases.return_value = ["db1"]
    backup_meta.get_tables.return_value = [table]
    backup.__dict__["_context"].backup_layout.get_backup.return_value = backup_meta

    dedup_references: DedupReferences = defaultdict(lambda: defaultdict(set))
    dedup_references["db1"]["table1"] = set(referenced_parts)

    # pylint: disable=protected-access
    backup._delete(backup_meta, dedup_references)
    return backup.__dict__["_context"].backup_layout


def test_delete_keeps_cloud_storage_data_in_use_by_other_backups():
    """
    Object keys are known only from the disk metadata inside the backup, so
    its cloud storage data is kept whole until the last reference is gone.
    """
    layout = _delete_backup_with_cloud_storage(referenced_parts=["all_1_1_0"])

    layout.delete_cloud_storage_data.assert_not_called()


def test_delete_removes_cloud_storage_data_that_is_not_referenced():
    """
    Cloud storage data of parts nobody reuses is deleted with the backup.
    """
    layout = _delete_backup_with_cloud_storage(referenced_parts=["all_2_2_0"])

    layout.delete_cloud_storage_data.assert_called_once_with("backup")


def _create_backup_next_to(existing_name: str, name: str) -> None:
    """Helper: create a backup while another backup already exists."""
    backup = ClickhouseBackup(DEFAULT_CONFIG)  # type: ignore[arg-type]
    backup.__dict__["_context"] = MagicMock()

    existing = MagicMock()
    existing.name = existing_name
    backup.__dict__["_context"].backup_layout.get_backups.return_value = [existing]
    backup.__dict__["_context"].backup_layout.get_backup_names.return_value = [
        existing_name
    ]

    backup.backup(BackupSources(), name=name)


def test_backup_rejects_an_existing_name():
    """
    A backup never overwrites another one.
    """
    with pytest.raises(ClickhouseBackupError) as exc:
        _create_backup_next_to("test-backup", "test-backup")

    assert "already exists" in str(exc.value)


def test_backup_rejects_a_name_taken_by_its_sanitized_form():
    """
    ClickHouse writes cloud storage data under the sanitized name, so such a
    backup would share the data path with the existing one.
    """
    with pytest.raises(ClickhouseBackupError) as exc:
        _create_backup_next_to("test_backup", "test-backup")

    assert "conflicts with existing backup test_backup" in str(exc.value)


def _get_backups_with_unreadable(*readable: Mock) -> Callable[..., list[Mock]]:
    """Mock get_backups that reports "broken" as unreadable or raises if not asked to."""

    def get_backups(*_args, unreadable=None, **_kwargs):
        """Return readable backups and record "broken" as unreadable."""
        if unreadable is None:
            raise StorageError("Unreadable light")
        unreadable["broken"] = "StorageError: Unreadable light"
        return list(readable)

    return get_backups


@pytest.mark.parametrize("force", [False, True])
@pytest.mark.parametrize("deduplicate_parts", [False, True])
def test_backup_skips_unreadable_old_metadata(force, deduplicate_parts):
    """A backup proceeds without the old backup whose metadata is unreadable."""
    backup = ClickhouseBackup(DEFAULT_CONFIG)  # type: ignore[arg-type]
    context = MagicMock()
    backup.__dict__["_context"] = context
    context.config = {
        **cast(dict, DEFAULT_CONFIG["backup"]),
        "deduplicate_parts": deduplicate_parts,
    }
    context.ch_ctl.get_databases.return_value = []
    context.backup_layout.get_backup_names.return_value = ["broken"]
    context.backup_layout.get_backups.side_effect = _get_backups_with_unreadable()
    sources = BackupSources(False, False, False, False, False, False)

    assert backup.backup(sources, name="new", force=force) == ("new", None)
    assert context.backup_meta.state == BackupState.CREATED
    if force and not deduplicate_parts:
        context.backup_layout.get_backups.assert_not_called()


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
    context.backup_layout.get_backup.return_value = None

    with patch(
        "ch_backup.ch_backup.collect_dedup_references_for_batch_backup_deletion",
        return_value={"old": {}},
    ):
        assert backup.delete("old", purge_partial=False) == ("old", None)

    context.backup_layout.delete_backup.assert_called_once()
    context.backup_layout.upload_backup_metadata.assert_called_once_with(
        light_metadata, light_only=True
    )


def test_delete_referenced_backup_requires_full_metadata() -> None:
    backup, context = _backup_with_context()
    light_metadata = Mock(name="old", state=BackupState.CREATED)
    light_metadata.name = "old"
    context.backup_layout.get_backup.return_value = None

    with pytest.raises(InvalidBackupStruct):
        backup._delete(  # pylint: disable=protected-access
            light_metadata, {"db": {"table": {"part"}}}
        )

    context.backup_layout.delete_backup.assert_not_called()
    context.backup_layout.upload_backup_metadata.assert_not_called()


@pytest.mark.parametrize("referenced", [False, True])
def test_delete_failure_persists_failed_state(referenced: bool) -> None:
    backup, context = _backup_with_context()
    light_metadata = Mock(name="old", state=BackupState.CREATED)
    light_metadata.name = "old"
    light_metadata.exception = None
    full_metadata = Mock(name="old", state=BackupState.CREATED, exception=None)
    full_metadata.name = "old"
    full_metadata.get_databases.return_value = []
    full_metadata.cloud_storage.enabled = False
    full_metadata.count_skipped_data_tables.return_value = (0, 0)
    context.backup_layout.get_backup.return_value = (
        full_metadata if referenced else None
    )
    error = StorageError("deletion failed")
    if referenced:
        context.backup_layout.wait.side_effect = error
    else:
        context.backup_layout.delete_backup.side_effect = error
    uploaded_states = []
    context.backup_layout.upload_backup_metadata.side_effect = (
        lambda metadata, light_only: uploaded_states.append(
            (metadata.state, metadata.exception, light_only)
        )
    )

    with pytest.raises(StorageError) as exc:
        backup._delete(  # pylint: disable=protected-access
            light_metadata, {"db": {"table": {"part"}}} if referenced else {}
        )

    assert exc.value is error
    context.ch_ctl.system_unfreeze.assert_not_called()
    assert uploaded_states == [
        (BackupState.DELETING, None, not referenced),
        (BackupState.FAILED, "StorageError: deletion failed", not referenced),
    ]


def test_referenced_delete_persists_partial_state() -> None:
    backup, context = _backup_with_context()
    light_metadata = Mock(name="old", state=BackupState.CREATED)
    light_metadata.name = "old"
    full_metadata = Mock(name="old", state=BackupState.CREATED)
    full_metadata.name = "old"
    full_metadata.exception = "Previous deletion failed"
    full_metadata.get_databases.return_value = []
    full_metadata.cloud_storage.enabled = False
    full_metadata.count_skipped_data_tables.return_value = (0, 0)
    full_metadata.cloud_storage.source = None
    full_metadata.copied_from = None
    context.backup_layout.get_backup.return_value = full_metadata

    result = backup._delete(  # pylint: disable=protected-access
        light_metadata, {"db": {"table": {"part"}}}
    )

    assert result[0] is None
    assert full_metadata.state == BackupState.PARTIALLY_DELETED
    assert full_metadata.exception is None
    assert context.backup_layout.upload_backup_metadata.call_count == 2


def _readable_backup(name: str, link: str | None = None) -> Mock:
    """Mock light metadata of a readable backup with one part linked to link."""
    meta = Mock(state=BackupState.CREATED)
    meta.name = name
    part = Mock(link=link, database="db", table="table", deduplicated_part_name="p")
    meta.get_databases.return_value = ["db"]
    meta.get_tables.return_value = [Mock(get_parts=Mock(return_value=[part]))]
    return meta


def _light_backup(name: str, age_days: int, incomplete: bool = False) -> Mock:
    """Mock light metadata of a created backup started age_days ago."""
    meta = Mock(state=BackupState.CREATED, start_time=now() - timedelta(days=age_days))
    meta.name = name
    meta.count_skipped_data_tables.return_value = (0, 1) if incomplete else (0, 0)
    return meta


def _purge(backups: list[Mock], retain_count: int, retain_time: dict) -> list[str]:
    """Helper: purge backups listed newest first and return the deleted ones."""
    backup, context = _backup_with_context()
    context.locker = MagicMock()
    context.config = {"retain_time": retain_time, "retain_count": retain_count}
    context.backup_layout.get_backups.return_value = backups
    with (
        patch(
            "ch_backup.ch_backup.collect_dedup_references_for_batch_backup_deletion",
            return_value=defaultdict(dict),
        ),
        patch.object(
            ClickhouseBackup,
            "_delete",
            side_effect=lambda meta, _refs: (meta.name, None),
        ),
    ):
        deleted, _ = backup.purge()
    return list(deleted)


def test_newest_complete_backup_outlives_retain_count() -> None:
    """The last copy of data the newer backups lack must survive purge."""
    backups = [
        _light_backup("incomplete", 0, incomplete=True),
        _light_backup("complete", 1),
        _light_backup("older", 2),
    ]

    assert _purge(backups, retain_count=1, retain_time={}) == ["older"]


def test_newest_complete_backup_outlives_retain_time() -> None:
    """Data the newer backups lack must survive purge by age as well."""
    backups = [
        _light_backup("incomplete", 0, incomplete=True),
        _light_backup("complete", 5),
        _light_backup("older", 6),
    ]

    assert _purge(backups, retain_count=0, retain_time={"days": 1}) == ["older"]


def test_deleted_incomplete_backup_does_not_keep_a_complete_one() -> None:
    """Only data the retained backups lack is a reason to keep more."""
    backups = [
        _light_backup("complete", 8),
        _light_backup("incomplete", 9, incomplete=True),
        _light_backup("older", 10),
    ]

    assert _purge(backups, retain_count=0, retain_time={"days": 7}) == [
        "complete",
        "incomplete",
        "older",
    ]


def test_purge_by_age_keeps_nothing_old_without_incomplete_backups() -> None:
    """Without incomplete backups purge by age is unchanged."""
    backups = [_light_backup("complete", 5), _light_backup("older", 6)]

    assert _purge(backups, retain_count=0, retain_time={"days": 1}) == [
        "complete",
        "older",
    ]


def test_delete_fails_on_unreadable_metadata_of_another_backup() -> None:
    """References of an unreadable backup are unknown, so nothing is deleted."""
    backup, context = _backup_with_context()
    context.locker = MagicMock()
    context.backup_layout.get_backups.side_effect = _get_backups_with_unreadable(
        _readable_backup("old")
    )

    with pytest.raises(StorageError, match="broken"):
        backup.delete("old", purge_partial=False)

    context.backup_layout.delete_backup.assert_not_called()
    context.backup_layout.upload_backup_metadata.assert_not_called()


def test_purge_fails_on_unreadable_metadata() -> None:
    """Purge never skips a backup it cannot read."""
    backup, context = _backup_with_context()
    context.locker = MagicMock()
    context.config = {"retain_time": {}, "retain_count": 1}
    context.backup_layout.get_backups.side_effect = _get_backups_with_unreadable()

    with pytest.raises(StorageError, match="Unreadable light"):
        backup.purge()

    context.backup_layout.delete_backup.assert_not_called()


def test_delete_unreadable_backup_without_references() -> None:
    """An unreferenced unreadable backup is deleted by name without unfreeze."""
    backup, context = _backup_with_context()
    context.locker = MagicMock()
    other = _readable_backup("other")
    context.backup_layout.get_backups.side_effect = _get_backups_with_unreadable(other)
    context.backup_layout.reload_backup.return_value = other

    deleted_name, msg = backup.delete("broken", purge_partial=False)

    assert deleted_name == "broken"
    assert msg and "SYSTEM UNFREEZE WITH NAME 'broken'" in msg

    context.backup_layout.reload_backup.assert_called_once_with(
        other, use_light_meta=False
    )
    context.backup_layout.delete_backup.assert_called_once()
    assert context.backup_layout.delete_backup.call_args.args[0] == "broken"
    context.backup_layout.delete_backup.call_args.args[1]()
    context.ch_ctl.system_unfreeze.assert_not_called()
    context.backup_layout.upload_backup_metadata.assert_not_called()


def test_delete_unreadable_backup_purges_partial_backups() -> None:
    """--purge-partial still deletes partial backups next to the unreadable one."""
    backup, context = _backup_with_context()
    context.locker = MagicMock()
    newest = _readable_backup("newest")
    partial = _readable_backup("partial")
    partial.state = BackupState.FAILED
    context.backup_layout.get_backups.side_effect = _get_backups_with_unreadable(
        newest, partial
    )
    context.backup_layout.reload_backup.side_effect = lambda meta, **_: meta

    with patch.object(backup, "_delete", return_value=("partial", None)) as delete:
        assert backup.delete("broken", purge_partial=True)[0] == "broken"

    delete.assert_called_once()
    assert delete.call_args.args[0] is partial
    assert context.backup_layout.delete_backup.call_args.args[0] == "broken"


def test_failed_partial_delete_keeps_unreadable_backup() -> None:
    """A partial backup that failed to delete must not be left linking to a removed one."""
    backup, context = _backup_with_context()
    context.locker = MagicMock()
    partial = _readable_backup("partial", link="broken")
    partial.state = BackupState.FAILED
    context.backup_layout.get_backups.side_effect = _get_backups_with_unreadable(
        _readable_backup("newest"), partial
    )
    context.backup_layout.reload_backup.side_effect = lambda meta, **_: meta

    with patch.object(backup, "_delete", side_effect=StorageError("deletion failed")):
        with pytest.raises(StorageError, match="deletion failed"):
            backup.delete("broken", purge_partial=True)

    context.backup_layout.delete_backup.assert_not_called()


def test_delete_unreadable_backup_with_references_fails() -> None:
    """An unreadable backup linked from another backup is kept."""
    backup, context = _backup_with_context()
    context.locker = MagicMock()
    other = _readable_backup("other", link="broken")
    context.backup_layout.get_backups.side_effect = _get_backups_with_unreadable(other)
    context.backup_layout.reload_backup.return_value = other

    with pytest.raises(ClickhouseBackupError, match="referenced by other backups"):
        backup.delete("broken", purge_partial=False)

    context.backup_layout.delete_backup.assert_not_called()


def test_delete_with_unreadable_full_metadata_without_references() -> None:
    """Unreadable full metadata falls back to light metadata if nothing links to it."""
    backup, context = _backup_with_context()
    light_metadata = Mock(name="old", state=BackupState.CREATED, copied_from=None)
    light_metadata.name = "old"
    light_metadata.cloud_storage.source = None
    context.backup_layout.get_backup.side_effect = StorageError("Unreadable full")

    result = backup._delete(light_metadata, {})  # pylint: disable=protected-access

    assert result == ("old", None)
    context.backup_layout.upload_backup_metadata.assert_called_once_with(
        light_metadata, light_only=True
    )
    context.backup_layout.delete_backup.assert_called_once()


def test_delete_with_unreadable_full_metadata_and_references_fails() -> None:
    """Shared parts cannot be told apart without full metadata."""
    backup, context = _backup_with_context()
    light_metadata = Mock(name="old", state=BackupState.CREATED)
    light_metadata.name = "old"
    context.backup_layout.get_backup.side_effect = StorageError("Unreadable full")

    with pytest.raises(StorageError, match="Unreadable full"):
        backup._delete(  # pylint: disable=protected-access
            light_metadata, {"db": {"table": {"part"}}}
        )

    context.backup_layout.delete_backup.assert_not_called()
    context.backup_layout.upload_backup_metadata.assert_not_called()


def _backup_metadata(
    copied_from: dict | None = None, cloud_storage_source: dict | None = None
) -> BackupMetadata:
    meta = BackupMetadata(
        name="backup1",
        version="1.0.0",
        ch_version="25.3.1.1",
        time_format="%Y-%m-%d %H:%M:%S %z",
        hostname="host1",
    )
    meta.copied_from = copied_from
    meta.cloud_storage.add_disk("s3")
    meta.cloud_storage.source = cloud_storage_source
    return meta


@pytest.mark.parametrize(
    ["copied_from", "unfreeze_expected"],
    [
        pytest.param(None, True, id="regular backup"),
        pytest.param(
            {"endpoint_url": "https://s3", "bucket": "b", "path_root": "ch_backup"},
            False,
            id="copied backup",
        ),
    ],
)
def test_delete_unfreezes_only_own_backups(
    copied_from: dict | None, unfreeze_expected: bool
) -> None:
    meta = _backup_metadata(copied_from=copied_from)
    context = MagicMock()
    context.backup_layout.get_backup.return_value = meta
    context.backup_layout.delete_backup.side_effect = lambda name, unfreeze: unfreeze()
    backup = ClickhouseBackup.__new__(ClickhouseBackup)
    backup.__dict__["_context"] = context

    assert backup._delete(meta, {}) == (  # pylint: disable=protected-access
        "backup1",
        None,
    )

    context.backup_layout.delete_backup.assert_called_once()
    assert context.backup_layout.delete_backup.call_args.args[0] == "backup1"
    if unfreeze_expected:
        context.ch_ctl.system_unfreeze.assert_called_once_with("backup1")
    else:
        context.ch_ctl.system_unfreeze.assert_not_called()


@pytest.mark.parametrize(
    ["cli_options", "expected"],
    [
        pytest.param(
            {},
            {
                "cloud_storage_source_bucket": "disk-copy",
                "cloud_storage_source_path": "copies/backup1",
                "cloud_storage_source_endpoint": "http://s3.test:9000",
            },
            id="location from metadata",
        ),
        pytest.param(
            {
                "cloud_storage_source_bucket": "cloud-storage",
                "cloud_storage_source_path": "data",
            },
            {
                "cloud_storage_source_bucket": "cloud-storage",
                "cloud_storage_source_path": "data",
                "cloud_storage_source_endpoint": None,
            },
            id="explicit location",
        ),
    ],
)
# pylint: disable=protected-access
def test_restore_cloud_storage_location(cli_options: dict, expected: dict) -> None:
    meta = _backup_metadata(
        cloud_storage_source={
            "endpoint_url": "http://s3.test:9000",
            "bucket": "disk-copy",
            "path": "copies/backup1",
        }
    )
    context = MagicMock()
    context.config = {"force_non_replicated": False}
    context.backup_layout.get_backup.return_value = meta
    backup = ClickhouseBackup.__new__(ClickhouseBackup)
    backup.__dict__["_context"] = context
    backup._check_zookeeper_for_restore = Mock()  # type: ignore[method-assign]
    backup._restore = Mock()  # type: ignore[method-assign]

    backup.restore(BackupSources(), "backup1", [], [], **cli_options)

    restore_kwargs = (
        backup._restore.call_args.kwargs
    )  # pylint: disable=protected-access
    assert {key: restore_kwargs[key] for key in expected} == expected


@pytest.mark.parametrize(
    "original_scheme,stored_scheme", [("http", "https"), ("https", "http")]
)
@pytest.mark.parametrize(
    "override",
    [
        None,
        "https://explicit.example:443",
        "http://explicit.example:9000",
        "explicit.example:8443",
    ],
)
# pylint: disable=protected-access
def test_restore_keeps_stored_endpoint_scheme(
    original_scheme: str, stored_scheme: str, override: str | None
) -> None:
    meta = _backup_metadata(
        cloud_storage_source={
            "endpoint_url": f"{stored_scheme}://copied.example:8443",
            "bucket": "copied-disk",
            "path": "copies/backup1",
        }
    )
    context = MagicMock()
    context.config = {"force_non_replicated": False}
    context.backup_layout.get_backup.return_value = meta
    backup = ClickhouseBackup.__new__(ClickhouseBackup)
    backup.__dict__["_context"] = context
    with (
        patch.object(backup, "_check_zookeeper_for_restore"),
        patch.object(backup, "_restore") as restore,
    ):
        backup.restore(
            BackupSources(), "backup1", [], [], cloud_storage_source_endpoint=override
        )
    options = restore.call_args.kwargs
    disks = ClickHouseTemporaryDisks(
        MagicMock(),
        MagicMock(),
        DEFAULT_CONFIG,  # type: ignore[arg-type]
        meta,
        options["cloud_storage_source_bucket"],
        options["cloud_storage_source_path"],
        options["cloud_storage_source_endpoint"],
        MagicMock(),
    )
    disks._disks = {
        "s3": {"endpoint": f"{original_scheme}://original.example/original/data/"}
    }
    expected = override or f"{stored_scheme}://copied.example:8443"
    if "://" not in expected:
        expected = f"{original_scheme}://{expected}"
    assert disks._source_disk_config(meta, "s3")["endpoint"] == (
        f"{expected}/copied-disk/copies/backup1/"
    )


def test_tables_left_without_data_are_reported() -> None:
    """The caller must learn that the backup holds incomplete data of tables."""
    backup, context = _backup_with_context()
    context.backup_meta.count_skipped_data_tables.return_value = (1, 2)

    message = backup._skipped_data_message()  # pylint: disable=protected-access

    assert message is not None
    assert "1 tables without data" in message
    assert "2 tables without some data parts" in message


def test_nothing_is_reported_when_all_data_is_backed_up() -> None:
    """A complete backup must not print anything about skipped data."""
    backup, context = _backup_with_context()
    context.backup_meta.count_skipped_data_tables.return_value = (0, 0)

    assert backup._skipped_data_message() is None  # pylint: disable=protected-access
