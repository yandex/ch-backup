"""Deletion batch validation, persisted checkpoints, and recovery."""

# pylint: disable=protected-access

from copy import deepcopy
from datetime import timedelta
from unittest.mock import MagicMock, patch

import pytest

from ch_backup.backup.deduplication import (
    collect_dedup_references_for_batch_backup_deletion,
)
from ch_backup.backup.metadata import (
    BackupMetadata,
    BackupState,
    PartMetadata,
)
from ch_backup.ch_backup import ClickhouseBackup
from ch_backup.exceptions import ConfigurationError, InvalidBackupStruct, StorageError
from ch_backup.storage.engine.s3 import S3StorageEngine


def test_mutation_renamed_child_protects_owners_cloud_part(cleanup, make_cloud_backup):
    """Deletion protects the stored name even when its child requests a later mutation."""
    c = cleanup
    c.meta.add_part(
        PartMetadata("db", "table", "all_1_1_0", "c", 1, [], False, disk_name="s3")
    )
    child = make_cloud_backup()
    child.name = "child"
    child.add_part(
        PartMetadata(
            "db",
            "table",
            "all_1_1_0_7",
            "c",
            1,
            [],
            False,
            disk_name="s3",
            link=c.meta.name,
            link_part_name="all_1_1_0",
        )
    )
    c.records[child.name] = child.dump_json()
    references = collect_dedup_references_for_batch_backup_deletion(
        c.layout, [child], {c.meta.name}
    )
    assert references[c.meta.name] == {"db": {"table": {"all_1_1_0"}}}
    # Retaining the owner requires no credentials for its external destination.
    c.meta.cloud_storage.source["endpoint_url"] = "https://unconfigured.test"
    c.records[c.meta.name] = c.meta.dump_json()
    _, _, plan = c.manager._prepare_deletion_batch([c.meta], references, True)[0]
    assert plan.delete_external_payload is None
    assert plan.delete_backup_cloud_files is None
    c.engine.delete_files.assert_not_called()
    c.loader.upload_file.assert_not_called()


def test_prepared_external_deletion_leaves_checkpoint_to_executor(cleanup):
    """Deleting payload must not silently change the metadata needed to record progress."""
    c = cleanup
    loaded, _, plan = c.manager._prepare_deletion_batch(
        [c.meta], {c.meta.name: {}}, True
    )[0]
    source = dict(loaded.cloud_storage.source)
    records = dict(c.records)

    assert plan.delete_external_payload is not None
    plan.delete_external_payload()

    assert c.objects == {"copies/backup1/foreign"}
    assert loaded.cloud_storage.source == source
    assert c.records == records


@pytest.mark.parametrize("retained_local_part", [False, True])
def test_external_cleanup_checkpoint_survives_interrupted_archive_removal(
    cleanup, make_cloud_backup, retained_local_part
):
    """Retry after archive removal must skip finished external cleanup and preserve local references."""
    c = cleanup
    if retained_local_part:
        c.meta.add_part(PartMetadata("db", "table", "all_9_9_0", "c", 1, [], False))
        child = make_cloud_backup()
        child.name = "child"
        child.cloud_storage.source = None
        child.add_part(
            PartMetadata(
                "db", "table", "all_9_9_0", "c", 1, [], False, link=c.meta.name
            )
        )
        c.records[child.name] = child.dump_json()
    c.records[c.meta.name] = c.meta.dump_json()
    interrupted = BaseException("archive removal interrupted")

    def remove_archives_and_interrupt(*_):
        c.archives.clear()
        raise interrupted

    with patch.object(
        c.layout,
        "delete_backup_cloud_files" if retained_local_part else "delete_backup",
        MagicMock(side_effect=remove_archives_and_interrupt),
    ):
        with pytest.raises(BaseException) as error:
            c.manager.delete(c.meta.name, False, delete_cloud_storage_data=True)
        assert error.value is interrupted
    saved = BackupMetadata.load_json(c.records[c.meta.name])
    assert saved.cloud_storage.source is None
    assert saved.state == BackupState.DELETING
    assert c.objects == {"copies/backup1/foreign"}
    reads = c.layout.download_cloud_storage_metadata_archive.call_count

    if retained_local_part:
        c.layout.delete_backup_cloud_files = MagicMock()
    result = c.manager.delete(c.meta.name, False, delete_cloud_storage_data=True)

    assert c.layout.download_cloud_storage_metadata_archive.call_count == reads
    if retained_local_part:
        assert result[0] is None
        retained = BackupMetadata.load_json(c.records[c.meta.name])
        assert retained.find_part("db", "table", "all_9_9_0") is not None
    else:
        assert result == (c.meta.name, None)


@pytest.mark.parametrize("operation", ["delete", "purge"])
@pytest.mark.parametrize("invalid", ["endpoint_url", "archive"])
def test_late_cloud_validation_failure_leaves_entire_batch_unchanged(
    cleanup, operation, invalid
):
    """
    Validating backups only as they are deleted could destroy the first backup
    before discovering an unsafe later target. Exercise destination rejection
    and missing archives.
    Both public operations must validate the complete batch before any mutation;
    individual destination rules are covered in test_destinations.py.
    """
    c = cleanup
    later = deepcopy(c.meta)
    later.name = "backup2"
    later.start_time -= timedelta(days=1)
    later.state = BackupState.PARTIALLY_DELETED
    later.cloud_storage.source["path"] = "copies/backup2"
    c.config["copy"]["cloud_storage"]["target"]["bucket"] = "payload"
    if invalid == "endpoint_url":
        later.cloud_storage.source[invalid] = "invalid"
    c.records[later.name] = later.dump_json()
    before = dict(c.records)
    with pytest.raises((ConfigurationError, InvalidBackupStruct)):
        if operation == "delete":
            c.manager.delete(
                "backup1", purge_partial=True, delete_cloud_storage_data=True
            )
        else:
            c.manager.purge(delete_cloud_storage_data=True)
    assert c.records == before
    c.engine.delete_files.assert_not_called()
    c.loader.upload_file.assert_not_called()
    c.layout.delete_backup.assert_not_called()
    c.layout.upload_backup_metadata.assert_not_called()
    c.context.ch_ctl.system_unfreeze.assert_not_called()


@pytest.mark.parametrize(
    "operation",
    ["delete", "purge"],
)
def test_whole_cleanup_reloads_retry_metadata_after_late_batch_failure(
    cleanup, operation
):
    """
    A later batch can fail after earlier objects are gone. Recreating the
    manager from saved metadata proves that retry does not depend on in-memory
    state or lose the archive or manifest needed to finish.
    """
    c = cleanup
    calls = []

    def delete(keys):
        calls.append(list(keys))
        if len(calls) == 2:
            raise StorageError("AccessDenied")
        c.objects.difference_update(keys)

    c.engine.delete_files.side_effect = delete
    with pytest.raises(StorageError, match="AccessDenied"):
        if operation == "delete":
            c.manager.delete(
                "backup1", purge_partial=False, delete_cloud_storage_data=True
            )
        else:
            c.manager.purge(delete_cloud_storage_data=True)
    failed = BackupMetadata.load_json(c.records["backup1"])
    assert failed.state == BackupState.FAILED
    assert failed.cloud_storage.source["path"] == "copies/backup1"
    assert c.objects == {"copies/backup1/c", "copies/backup1/foreign"}
    c.layout.delete_backup.assert_not_called()
    retry = ClickhouseBackup(c.config)
    retry.__dict__["_context"] = c.context
    assert retry.delete(
        "backup1", purge_partial=False, delete_cloud_storage_data=True
    ) == ("backup1", None)
    assert calls == [["copies/backup1/a", "copies/backup1/b"], ["copies/backup1/c"]] * 2
    assert c.objects == {"copies/backup1/foreign"}
    assert not c.records
    c.context.ch_ctl.system_unfreeze.assert_not_called()


@pytest.mark.parametrize("full_metadata", ["missing", "unreadable"])
def test_external_cleanup_can_finish_from_light_metadata(cleanup, full_metadata):
    """
    Losing full metadata should not strand an unreferenced copy when readable
    light metadata and disk archives still identify its payload. The retained
    foreign object checks that recovery stays selective.
    """
    c = cleanup

    def read(name, use_light_meta=False, **_):
        if use_light_meta:
            return BackupMetadata.load_json(c.records[name])
        if full_metadata == "unreadable":
            raise StorageError("unreadable full metadata")
        return None

    c.layout.get_backup.side_effect = read
    assert c.manager.delete(
        "backup1", purge_partial=False, delete_cloud_storage_data=True
    ) == ("backup1", None)
    assert not c.records
    assert c.objects == {"copies/backup1/foreign"}


@pytest.mark.parametrize("delete_unreadable", [False, True])
def test_external_cleanup_requires_readable_light_metadata(cleanup, delete_unreadable):
    """
    Without readable light metadata, cleanup cannot establish the copied
    payload destination. Refusing deletion preserves data instead of guessing a
    location.
    """
    c = cleanup
    c.context.lock_conf = {"zk_flock": True}
    c.layout.get_backup.side_effect = StorageError("unreadable")

    def unreadable_listing(*, unreadable, **_):
        unreadable["backup1"] = "unreadable"
        return []

    c.layout.get_backups.side_effect = unreadable_listing
    with pytest.raises(InvalidBackupStruct, match="readable"):
        c.manager.delete(
            "backup1",
            purge_partial=False,
            delete_cloud_storage_data=True,
            delete_unreadable=delete_unreadable,
        )
    c.engine.delete_files.assert_not_called()
    c.layout.delete_backup.assert_not_called()
    c.loader.upload_file.assert_not_called()


@pytest.mark.parametrize("original", [False, True])
def test_partial_cleanup_keeps_cloud_owner_until_final_deletion(cleanup, original):
    c = cleanup
    if original:
        c.meta.copied_from = None
        c.meta.cloud_storage.source = None
    for number, disk in enumerate(["s3", "s3", "default", "default"], 1):
        c.meta.add_part(
            PartMetadata(
                "db",
                "table",
                f"all_{number}_{number}_0",
                "c",
                10,
                [],
                False,
                disk_name=disk,
            )
        )
    c.meta.cloud_storage.add_disk("linked_only")
    c.meta.add_part(
        PartMetadata(
            "db",
            "table",
            "all_5_5_0",
            "c",
            10,
            [],
            False,
            disk_name="linked_only",
            link="other",
        )
    )
    refs = {"db": {"table": {"all_1_1_0", "all_3_3_0"}}}
    c.records[c.meta.name] = c.meta.dump_json()
    original_archives = deepcopy(c.archives)
    original_objects = set(c.objects)
    for _ in range(2):
        loaded, light, plan = c.manager._prepare_deletion_batch(
            [c.layout.get_backup("backup1")], {"backup1": refs}, True
        )[0]
        assert c.manager._execute_backup_deletion(loaded, refs, light, plan)[0] is None
        assert c.archives == original_archives
        assert c.objects == original_objects
        c.layout.download_cloud_storage_metadata_archive.assert_not_called()
        c.loader.upload_file.assert_not_called()
        c.context.ch_ctl.system_unfreeze.assert_not_called()
        result = BackupMetadata.load_json(c.records["backup1"])
        if not original:
            assert result.cloud_storage.disks == ["s3"]
        assert result.state == BackupState.PARTIALLY_DELETED
        assert [part.name for part in result.get_parts()] == ["all_1_1_0", "all_3_3_0"]
    loaded, light, plan = c.manager._prepare_deletion_batch(
        [result], {"backup1": {}}, True
    )[0]
    assert c.manager._execute_backup_deletion(loaded, {}, light, plan) == (
        "backup1",
        None,
    )
    if original:
        c.context.ch_ctl.system_unfreeze.assert_called_once_with("backup1")
        assert c.objects == original_objects
    else:
        assert c.objects == {"copies/backup1/foreign"}


def test_s3_per_key_error_keeps_cleanup_metadata_for_retry(cleanup):
    """
    An S3 deletion response can contain per-key failures without raising a
    transport error. The real adapter must expose those failures so cleanup
    retains its retry metadata instead of reporting success.
    """
    c = cleanup
    storage = deepcopy(c.config["storage"])
    storage["credentials"]["bucket"] = "payload"
    storage["retry_max_attempts"] = 1
    engine = S3StorageEngine(storage)
    client = MagicMock()
    client.delete_objects.return_value = {
        "Errors": [{"Key": "copies/backup1/a", "Code": "AccessDenied"}]
    }
    engine._s3_client_factory = MagicMock(
        create_s3_client=MagicMock(return_value=client)
    )
    c.engine_factory.return_value = engine
    with pytest.raises(StorageError, match="AccessDenied"):
        c.manager.delete("backup1", purge_partial=False, delete_cloud_storage_data=True)
    failed = BackupMetadata.load_json(c.records["backup1"])
    assert failed.state == BackupState.FAILED and failed.cloud_storage.source
    assert c.archives
    c.layout.delete_backup.assert_not_called()
    assert client.delete_objects.call_args.kwargs["Delete"]["Objects"] == [
        {"Key": "copies/backup1/a"},
        {"Key": "copies/backup1/b"},
    ]
