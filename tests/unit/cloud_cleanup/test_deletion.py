"""Deletion batch validation, persisted checkpoints, and recovery."""

# pylint: disable=protected-access

import json
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
    CopyScope,
    CopyScopeStatus,
    PartMetadata,
)
from ch_backup.ch_backup import ClickhouseBackup
from ch_backup.exceptions import ConfigurationError, InvalidBackupStruct, StorageError
from ch_backup.storage.engine.s3 import S3StorageEngine
from tests.unit.test_backup_copy import _object_storage_metadata


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
    plan = c.manager._prepare_backup_cloud_cleanup(
        c.meta, references[c.meta.name], True
    )
    assert plan.delete_external_payload is None
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
    before discovering an unsafe later target. Exercise destination rejection,
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
    "operation,incomplete_copy",
    [("delete", False), ("purge", False), ("delete", True)],
)
def test_whole_cleanup_reloads_retry_metadata_after_late_batch_failure(
    cleanup, messages, operation, incomplete_copy, seed_manifests
):
    """
    A later batch can fail after earlier objects are gone. Recreating the
    manager from saved metadata proves that retry does not depend on in-memory
    state or lose the archive or manifest needed to finish.
    """
    c = cleanup
    calls = []
    if incomplete_copy:
        c.meta.copy_scopes = {CopyScope.CLOUD_STORAGE: CopyScopeStatus.FAILED}
        c.meta.state = BackupState.FAILED
        c.records[c.meta.name] = c.meta.dump_json()
        c.archives.clear()
        seed_manifests(
            c, (c.meta.cloud_storage.source, ["copies/backup1/" + k for k in "abc"])
        )
        c.meta.cloud_storage.source = {
            "endpoint_url": None,
            "bucket": "source",
            "path": "original",
        }
        c.records[c.meta.name] = c.meta.dump_json()
    else:
        # A failed deletion preserves completed copy scopes. Retrying must read
        # the published archives even when the transfer manifest still exists.
        c.meta.copy_scopes = dict.fromkeys(CopyScope, CopyScopeStatus.COMPLETED)
        c.records[c.meta.name] = c.meta.dump_json()
        c.loader.download_file.side_effect = AssertionError(
            "Read a stale transfer manifest"
        )

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
    assert failed.cloud_storage.source["path"] == (
        "original" if incomplete_copy else "copies/backup1"
    )
    assert c.objects == {"copies/backup1/c", "copies/backup1/foreign"}
    output = "\n".join(record["message"] for record in messages)
    assert "confirmed=2" in output and "planned=3" in output
    assert "cleanup failed" in output and "cleanup completed" not in output
    c.layout.delete_backup.assert_not_called()
    if incomplete_copy:
        assert failed.copy_scopes[CopyScope.CLOUD_STORAGE] == CopyScopeStatus.FAILED
    retry = ClickhouseBackup(c.config)
    retry.__dict__["_context"] = c.context
    assert retry.delete(
        "backup1", purge_partial=False, delete_cloud_storage_data=True
    ) == ("backup1", None)
    assert calls == [["copies/backup1/a", "copies/backup1/b"], ["copies/backup1/c"]] * 2
    assert c.objects == {"copies/backup1/foreign"}
    assert not c.records
    if incomplete_copy:
        assert [call.args[0] for call in c.loader.download_file.call_args_list] == [
            "ch_backup/backup1/copy_context/cleanup/0.jsonl.gz"
        ] * 2
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


@pytest.mark.parametrize("interrupted", [False, True])
def test_cloud_reference_keeps_all_owner_data_until_final_explicit_cleanup(
    cleanup,
    interrupted,
    seed_manifests,
):
    """A retained cloud part keeps earlier attempts and every disk archive through partial deletion."""
    c = cleanup
    c.meta.cloud_storage.add_disk("other")
    for name, disk in (("all_1_1_0", "s3"), ("all_2_2_0", "other")):
        c.meta.add_part(
            PartMetadata("db", "table", name, "c", 1, [], False, disk_name=disk)
        )
    # An older owner can have a disk containing only links and no archive.
    c.meta.cloud_storage.add_disk("linked")
    c.meta.add_part(
        PartMetadata(
            "db",
            "table",
            "all_3_3_0",
            "c",
            1,
            [],
            False,
            disk_name="linked",
            link="source-owner",
        )
    )
    c.archives["ch_backup/backup1/disks/other/db/table.tar.gz"] = [
        ("all_2_2_0/data.bin", _object_storage_metadata(("other", 1)))
    ]
    c.objects.update({"copies/backup1/other", "old/backup1/attempt"})
    seed_manifests(
        c,
        (
            dict(c.meta.cloud_storage.source, path="old/backup1"),
            ["old/backup1/attempt"],
        ),
    )
    c.records[c.meta.name] = c.meta.dump_json()
    before = (deepcopy(c.archives), set(c.objects))
    refs = {"db": {"table": {"all_1_1_0"}}}
    backup, light, plan = c.manager._prepare_deletion_batch(
        [c.meta], {c.meta.name: refs}, True
    )[0]
    if interrupted:
        # Local cleanup may fail after the owner's last outgoing link is gone.
        with patch.object(
            c.manager, "_delete_detached_parts", side_effect=StorageError("interrupted")
        ):
            with pytest.raises(StorageError, match="interrupted"):
                c.manager._execute_backup_deletion(backup, refs, light, plan)
    else:
        assert c.manager._execute_backup_deletion(backup, refs, light, plan)[0] is None
    assert (c.archives, c.objects) == before
    c.layout.download_cloud_storage_metadata_archive.assert_not_called()
    c.engine.delete_files.assert_not_called()
    c.loader.upload_file.assert_not_called()
    partial = c.layout.get_backup(c.meta.name)
    assert set(partial.cloud_storage.disks) == {"s3", "other"}
    assert [part.name for part in partial.get_parts()] == ["all_1_1_0"]
    assert c.manager.delete(partial.name, False, delete_cloud_storage_data=True) == (
        partial.name,
        None,
    )
    assert c.objects == {"copies/backup1/foreign"}


@pytest.mark.parametrize("copied", [False, True])
def test_referenced_original_keeps_its_frozen_objects_until_final_deletion(
    cleanup, copied
):
    """Only a metadata-only original backup still depends on its frozen source objects."""
    c = cleanup
    c.meta.copied_from = None
    c.meta.cloud_storage.data_copied = copied
    c.meta.cloud_storage.source = None
    c.meta.add_part(
        PartMetadata("db", "table", "all_1_1_0", "c", 1, [], False, disk_name="s3")
    )
    c.records[c.meta.name] = c.meta.dump_json()
    refs = {"db": {"table": {"all_1_1_0"}}}
    backup, light, plan = c.manager._prepare_deletion_batch(
        [c.meta], {c.meta.name: refs}, True
    )[0]
    c.manager._execute_backup_deletion(backup, refs, light, plan)
    assert c.context.ch_ctl.system_unfreeze.call_count == int(copied)
    c.context.ch_ctl.system_unfreeze.reset_mock()
    c.manager.delete(c.meta.name, False)
    c.context.ch_ctl.system_unfreeze.assert_called_once_with(c.meta.name)


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


@pytest.mark.parametrize(
    "manifest",
    [
        None,
        {},
        1,
        "elsewhere/a",
        "copies/backup1/../a",
        "copies/backup1/./a",
        "unreadable",
    ],
)
def test_incomplete_copy_rejects_unsafe_or_unreadable_manifest(
    cleanup, manifest, seed_manifests
):
    """Validate the destination and every planned key before deleting any object."""
    c = cleanup
    c.meta.copy_scopes = {CopyScope.CLOUD_STORAGE: CopyScopeStatus.FAILED}
    c.meta.state = BackupState.FAILED
    seed_manifests(c, (c.meta.cloud_storage.source, [manifest]))
    if manifest == "unreadable":
        c.loader.download_file.side_effect = StorageError("missing manifest")
    with pytest.raises((ConfigurationError, StorageError)):
        c.manager._prepare_backup_cloud_cleanup(c.meta, {}, True)
    c.engine.delete_files.assert_not_called()
    c.layout.upload_backup_metadata.assert_not_called()


def test_purge_completed_copy_reclaims_keys_from_earlier_destination(
    cleanup, seed_manifests
):
    c = cleanup
    old = dict(c.meta.cloud_storage.source, path="old/backup1")
    seed_manifests(
        c, (old, ["old/backup1/x"]), (c.meta.cloud_storage.source, ["copies/backup1/a"])
    )
    c.objects.update({"old/backup1/x", "old/backup1/foreign"})
    c.manager.purge(delete_cloud_storage_data=True)
    assert c.objects == {"copies/backup1/foreign", "old/backup1/foreign"}
    assert not c.records


def test_cleanup_rejects_manifest_from_another_bucket(cleanup, seed_manifests):
    c = cleanup
    location = c.meta.cloud_storage.source
    seed_manifests(c, (location, ["copies/backup1/a"]))
    c.loader.download_data.return_value = json.dumps([dict(location, bucket="another")])
    with pytest.raises(ConfigurationError, match="outside the copy destination"):
        c.manager.delete("backup1", purge_partial=False, delete_cloud_storage_data=True)
    c.engine.delete_files.assert_not_called()
    c.layout.delete_backup.assert_not_called()
