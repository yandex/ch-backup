"""Deletion intent survives missing manifests and failed cleanup attempts."""

import json
from unittest.mock import MagicMock, patch

import pytest
from click import Group
from click.testing import CliRunner

from ch_backup.backup.deduplication import (
    collect_dedup_references_for_batch_backup_deletion,
)
from ch_backup.backup.metadata import BackupMetadata, CloudStorageMetadata, PartMetadata
from ch_backup.backup.metadata.cloud_storage_metadata import CloudStorageBackup
from ch_backup.cli import (
    adopt_cloud_storage_command,
    delete_command,
    list_command,
    purge_command,
)
from ch_backup.exceptions import StorageError
from tests.unit.test_disks import BACKUP_DISK_CLICKHOUSE_CONFIG, _make_temporary_disks
from tests.unit.test_layout import fallback_context_fixture


def clean_pending(layout, retained):
    """Collect references under the same contract as the deletion coordinator."""
    owners: dict[str, set[str]] = {}
    references = collect_dedup_references_for_batch_backup_deletion(
        layout, retained, {"old"}, owners
    )
    return layout.clean_pending_backup(
        "old", references["old"], owners.get("old", set())
    )


@pytest.fixture(name="marker_storage")
def marker_storage_fixture(pending_context):
    """Reuse the mutable store without any backup manifests or payload."""
    _, layout, storage, objects = pending_context
    objects.clear()
    return layout, storage, objects


@pytest.mark.parametrize("root", ["ch_backup", "/ch_backup"])
def test_retry_preserves_intent_and_last_complete_reference_snapshot(
    marker_storage, root
):
    """A new attempt without a completed scan cannot clear previous protection."""
    layout, _, _ = marker_storage
    getattr(layout, "_config")["path_root"] = root
    initial = layout.save_delete_pending("old", "missing_metadata")
    assert initial["referenced_by"] is initial["references_checked_at"] is None
    assert layout.get_pending_deletions() == {"old": initial}
    checked = layout.save_delete_pending("old", "retry", ["new", "another", "new"])
    retry = layout.save_delete_pending("old", "retry")
    assert retry["requested_at"] == initial["requested_at"]
    assert retry["reason"] == "missing_metadata"
    assert retry["referenced_by"] == ["another", "new"]
    assert retry["references_checked_at"] == checked["references_checked_at"]
    assert retry["state"] == "partially_deleted"
    empty = layout.save_delete_pending("old", "retry", [])
    assert empty["referenced_by"] == []
    assert empty["references_checked_at"] is not None


@pytest.mark.parametrize("bad", ["{invalid", "{}", "[]"])
def test_unreadable_marker_is_never_overwritten(marker_storage, bad):
    """An invalid intent is not treated as absence and replaced silently."""
    layout, storage, objects = marker_storage
    objects["ch_backup/old/delete_pending.json"] = bad
    with pytest.raises(StorageError):
        layout.save_delete_pending("old", "retry")
    storage.upload_data.assert_not_called()


@pytest.mark.parametrize("head_error", [False, True])
def test_marker_error_is_reported_by_list_and_blocks_delete(
    pending_context, head_error
):
    """Keep healthy backups visible and preserve all objects on a marker error."""
    backup, layout, storage, objects = pending_context
    marker = "ch_backup/old/delete_pending.json"
    if head_error:

        def path_exists(path):
            """Simulate a failed S3 HEAD for the target marker."""
            if path == marker:
                raise StorageError("HEAD failed")
            return path in objects

        storage.path_exists.side_effect = path_exists
    else:
        objects[marker] = "{"
    result = CliRunner().invoke(
        Group(commands=[list_command]),
        ["list", "-av", "--format", "json"],
        obj={"backup": backup},
    )
    assert result.exit_code == 0, result.output
    rows = {row["name"]: row for row in json.loads(result.stdout)}
    assert rows["new"]["state"] == "created"
    assert rows["old"]["metadata_error"] == "Failed to read deletion marker for old"
    with pytest.raises(StorageError, match="Failed to read deletion marker for old"):
        layout.get_backups(unreadable={}, strict_inventory=True)
    before = objects.copy()
    with pytest.raises(StorageError, match="Failed to read deletion marker for old"):
        backup.purge(delete_unreadable=True)
    with pytest.raises(StorageError, match="Failed to read deletion marker for old"):
        backup.delete("new", False, delete_unreadable=True)
    assert objects == before


def test_marker_survives_cleanup_failure_and_is_deleted_last(marker_storage):
    """A failed wait leaves intent available for an idempotent retry."""
    layout, storage, objects = marker_storage
    layout.save_delete_pending("old", "missing_metadata", ["new"])
    objects["ch_backup/old/data.bin"] = "data"
    storage.wait.side_effect = [RuntimeError("delete failed"), None]
    with pytest.raises(StorageError):
        layout.delete_backup("old", MagicMock())
    storage.delete_files.assert_called_once_with(
        remote_paths=["ch_backup/old/data.bin"], is_async=True
    )
    assert layout.get_delete_pending("old")["referenced_by"] == ["new"]
    layout.delete_backup("old", MagicMock())
    assert storage.delete_files.call_args.args[0] == [
        "ch_backup/old/delete_pending.json"
    ]


def test_failed_marker_write_prevents_deletion(pending_context):
    """Delete must stop before changing payload when its intent cannot be saved."""
    backup, layout, storage, objects = pending_context
    del objects["ch_backup/old/backup_struct.json"]
    del objects["ch_backup/old/backup_light_struct.json"]
    before = objects.copy()
    storage.upload_data.side_effect = RuntimeError("S3 unavailable")
    with pytest.raises(StorageError, match="Failed to save"):
        backup.delete("old", False, delete_unreadable=True)
    storage.upload_data.assert_called_once()
    storage.delete_files.assert_not_called()
    assert layout.get_delete_pending("old") is None
    assert objects == before


def test_pending_cleanup_uses_current_references_and_keeps_whole_shared_part(
    pending_context,
):
    """A stale marker snapshot must not delete a newly referenced part."""
    _, layout, storage, objects = pending_context
    layout.save_delete_pending("old", "metadata_unavailable", [])
    del objects["ch_backup/old/backup_struct.json"]
    del objects["ch_backup/old/backup_light_struct.json"]
    objects["ch_backup/old/cloud_storage/disk/shared"] = "data"
    objects["ch_backup/old/disks/disk.tar"] = "metadata"
    objects["ch_backup/old/data/db/table/all_1_1_0/extra.bin"] = "data"
    retained = layout.get_backup("new", use_light_meta=True)
    assert retained is not None
    assert clean_pending(layout, [retained]) is False
    storage.delete_files.assert_called_once_with(
        remote_paths=["ch_backup/old/data/db/table/all_2_2_0/data.bin"], is_async=True
    )
    assert layout.get_delete_pending("old")["referenced_by"] == ["new"]
    assert "ch_backup/old/cloud_storage/disk/shared" in objects
    assert "ch_backup/old/disks/disk.tar" in objects
    assert "ch_backup/old/data/db/table/all_1_1_0/extra.bin" in objects
    storage.delete_files.reset_mock()
    marker = layout.get_delete_pending("old")

    def download(path, **_kwargs):
        """Fail only the retained manifest, keeping the deletion intent readable."""
        if path == "ch_backup/new/backup_struct.json":
            raise StorageError("retained full temporarily unavailable")
        return objects[path]

    storage.download_data.reset_mock()
    storage.download_data.side_effect = download
    with pytest.raises(StorageError):
        clean_pending(layout, [retained])
    storage.download_data.assert_any_call(
        "ch_backup/new/backup_struct.json", encoding=None
    )
    storage.delete_files.assert_not_called()
    assert layout.get_delete_pending("old") == marker


@pytest.mark.parametrize("mutated", [False, True])
def test_pending_cleanup_protects_escaped_and_legacy_paths(marker_storage, mutated):
    """Protect both layouts without relying on directory existence probes."""
    layout, storage, objects = marker_storage
    getattr(layout, "_config")["path_root"] = "/ch_backup"
    layout.save_delete_pending("old", "missing_metadata")
    objects.update(
        {
            "ch_backup/old/data/db/my%2Dtable/all_1_1_0/data.bin": "data",
            "ch_backup/old/data/db/my-table/all_1_1_0/data.bin": "data",
            "ch_backup/old/data/db/my%2Dtable/all_2_2_0/data.bin": "data",
        }
    )
    part = PartMetadata(
        database="db",
        table="my-table",
        name="all_1_1_0_42" if mutated else "all_1_1_0",
        checksum="x",
        size=1,
        files=["data.bin"],
        tarball=False,
        link="old",
        link_part_name="all_1_1_0" if mutated else None,
    )
    retained = MagicMock(spec=BackupMetadata)
    retained.name = "new"
    retained.get_tables.return_value = [MagicMock(get_parts=lambda: [part])]
    with patch.object(layout, "reload_backup", return_value=retained):
        assert not clean_pending(layout, [retained])
    storage.delete_files.assert_called_once_with(
        remote_paths=["ch_backup/old/data/db/my%2Dtable/all_2_2_0/data.bin"],
        is_async=True,
    )


def test_pending_cleanup_rejects_unknown_cloud_ownership(marker_storage):
    """No references do not prove ownership of a sanitized cloud prefix."""
    layout, storage, objects = marker_storage
    marker = layout.save_delete_pending("old", "missing_metadata", ["new"])
    objects["ch_backup/old/cloud_storage/disk/shared-object"] = "data"
    with pytest.raises(StorageError, match="ownership"):
        clean_pending(layout, [])
    storage.delete_files.assert_not_called()
    assert layout.get_delete_pending("old") == marker


@pytest.fixture(name="pending_context")
def pending_context_fixture():
    """Run actual delete and purge against a mutable object inventory."""
    backup, layout, storage, objects = getattr(
        fallback_context_fixture, "__wrapped__"
    )()
    context = getattr(backup, "_context")
    context.lock_conf = {"zk_flock": True}

    def list_dir(path, recursive=False, absolute=False):
        """Match the storage boundary, including immediate-child discovery."""
        prefix = path.strip("/") + "/"
        keys = [key for key in objects if key.startswith(prefix)]
        if recursive:
            return keys if absolute else [key[len(prefix) :] for key in keys]
        return sorted({key[len(prefix) :].split("/", 1)[0] for key in keys})

    def delete_files(remote_paths, **_kwargs):
        """Apply deletions so subsequent attempts see actual remaining objects."""
        for key in remote_paths:
            objects.pop(key.lstrip("/"), None)

    storage.list_dir.side_effect = list_dir
    storage.delete_files.side_effect = delete_files
    return backup, layout, storage, objects


@pytest.mark.parametrize("corrupt", [False, True])
def test_delete_pending_lifecycle_preserves_dependency_until_last_reference(
    pending_context, corrupt
):
    """Delete excess A data, retain B's part, then finish A after B disappears."""
    backup, layout, _, objects = pending_context
    for filename in ("backup_struct.json", "backup_light_struct.json"):
        key = f"ch_backup/old/{filename}"
        if corrupt:
            objects[key] = "broken JSON"
        else:
            del objects[key]
    with pytest.raises(StorageError, match="--delete-unreadable"):
        backup.delete("old", False)
    assert layout.get_delete_pending("old") is None
    assert backup.delete("old", False, delete_unreadable=True)[0] is None
    marker = layout.get_delete_pending("old")
    assert marker["referenced_by"] == ["new"]
    assert marker["reason"] == ("corrupt_metadata" if corrupt else "missing_metadata")
    assert "ch_backup/old/data/db/table/all_1_1_0/data.bin" in objects
    assert "ch_backup/old/data/db/table/all_2_2_0/data.bin" not in objects
    assert [meta.name for meta in layout.get_backups(True)] == ["new"]
    with pytest.raises(StorageError, match="pending deletion"):
        backup.get("old")
    backup.delete("new", False)
    getattr(backup, "_context").config.update(retain_count=None, retain_time={})
    assert backup.purge()[0] == ["old"]
    assert not objects


def test_returning_manifests_do_not_cancel_intent(pending_context):
    """Recovered full and light do not make a devalued backup selectable again."""
    backup, layout, _, objects = pending_context
    layout.save_delete_pending("old", "metadata_unavailable")
    assert [meta.name for meta in backup.list()] == ["new"]
    with pytest.raises(StorageError, match="pending deletion"):
        layout.get_backup("old")
    assert backup.delete("old", False)[0] is None
    assert objects["ch_backup/old/backup_struct.json"]
    assert "ch_backup/old/data/db/table/all_1_1_0/data.bin" in objects
    assert "ch_backup/old/data/db/table/all_2_2_0/data.bin" not in objects
    assert layout.get_delete_pending("old")["referenced_by"] == ["new"]


def test_pending_policy_requires_distributed_writer_lock(pending_context):
    """No destructive policy runs without the shared cross-host writer lock."""
    backup, _, storage, objects = pending_context
    getattr(backup, "_context").lock_conf = {"zk_flock": False}
    del objects["ch_backup/old/backup_struct.json"]
    del objects["ch_backup/old/backup_light_struct.json"]
    with pytest.raises(StorageError, match="lock.zk_flock"):
        backup.delete("old", False, delete_unreadable=True)
    storage.upload_data.assert_not_called()
    storage.delete_files.assert_not_called()


def test_cloud_claim_cannot_adopt_legacy_objects_or_conflicting_owner(pending_context):
    """Only an empty prefix can acquire ownership before any cloud write."""
    _, layout, _, objects = pending_context
    objects["ch_backup/old/cloud_storage/disk/legacy"] = "data"
    with pytest.raises(StorageError, match="adopt-cloud-storage"):
        layout.claim_cloud_storage("old")
    assert not layout.cloud_storage_owned("old")
    layout.claim_cloud_storage("fresh-backup")
    assert layout.cloud_storage_owned("fresh-backup")
    with pytest.raises(StorageError, match="ownership"):
        layout.claim_cloud_storage("fresh_backup")


def test_cloud_cleanup_removes_claimed_payload_only_after_links_disappear(
    pending_context,
):
    """A retained reference protects the entire cloud group until a fresh empty scan."""
    backup, layout, _, objects = pending_context
    layout.claim_cloud_storage("old")
    objects["ch_backup/old/cloud_storage/disk/shared"] = "data"
    objects["ch_backup/old/disks/disk.tar"] = "metadata"
    layout.save_delete_pending("old", "missing_metadata")
    backup.delete("old", False)
    assert "ch_backup/old/cloud_storage/disk/shared" in objects
    backup.delete("new", False)
    assert backup.delete("old", False)[0] == "old"
    assert not objects


@pytest.mark.parametrize("nonempty", [False, True])
def test_strict_inventory_ignores_only_empty_prefixes(pending_context, nonempty):
    """Empty directory prefixes left by storage do not block deletion."""
    _, layout, _, objects = pending_context
    if nonempty:
        objects["ch_backup/deleted/metadata/table.sql"] = "metadata"
    unreadable: dict[str, str] = {}
    with patch.object(layout, "get_backup_names", return_value=["deleted"]):
        assert not layout.get_backups(unreadable=unreadable, strict_inventory=True)
    assert unreadable == ({"deleted": "missing_metadata"} if nonempty else {})


def test_strict_inventory_requires_proof_for_cloud_companions(pending_context):
    """A similar name and cloud-only contents cannot hide unknown outgoing links."""
    _, layout, _, objects = pending_context
    raw = json.loads(objects["ch_backup/old/backup_struct.json"])
    raw["meta"]["name"] = "x-y"
    objects["ch_backup/x-y/backup_struct.json"] = json.dumps(raw)
    objects["ch_backup/x_y/disks/disk.tar"] = "metadata"
    unreadable: dict[str, str] = {}
    layout.get_backups(True, unreadable=unreadable, strict_inventory=True)
    assert unreadable == {"x_y": "missing_metadata"}
    del objects["ch_backup/x_y/disks/disk.tar"]
    layout.claim_cloud_storage("x-y")
    objects["ch_backup/x_y/disks/disk.tar"] = "metadata"
    unreadable.clear()
    layout.get_backups(True, unreadable=unreadable, strict_inventory=True)
    assert not unreadable


@pytest.mark.parametrize("encrypted", [False, True])
def test_linked_cloud_restore_uses_descriptor_after_both_manifests_are_lost(
    pending_context, encrypted
):
    """Linked restore needs cloud settings, never a fabricated full manifest."""
    _, layout, _, objects = pending_context
    metadata = layout.get_backup("old")
    assert metadata is not None
    metadata.cloud_storage = CloudStorageMetadata(
        encryption=encrypted,
        compression=True,
        data_copied=True,
        disks=["object_storage"],
    )
    layout.upload_backup_metadata(metadata)
    del objects["ch_backup/old/backup_struct.json"]
    del objects["ch_backup/old/backup_light_struct.json"]
    layout.save_delete_pending("old", "missing_metadata")
    part = PartMetadata(
        "db1",
        "table1",
        "all_1_1_0",
        "x",
        1,
        ["data.bin"],
        True,
        link="old",
        disk_name="object_storage",
    )
    manager = _make_temporary_disks(
        BACKUP_DISK_CLICKHOUSE_CONFIG,
        cloud_storage_disks=["object_storage"],
        data_copied=True,
        parts=[part],
    )
    setattr(manager, "_backup_layout", layout)
    source, disk, _ = next(getattr(manager, "_linked_backups_data")())
    assert isinstance(source, CloudStorageBackup)
    assert source.cloud_storage.encrypted is encrypted
    assert source.cloud_storage.compressed is True
    assert disk == "object_storage"


def test_cloud_descriptor_is_published_before_created_manifests(pending_context):
    """A descriptor write failure prevents publishing a completed backup."""

    _, layout, storage, _ = pending_context
    metadata = layout.get_backup("old")
    assert metadata is not None
    metadata.cloud_storage = CloudStorageMetadata(data_copied=True, disks=["s3"])
    storage.upload_data.side_effect = StorageError("descriptor upload failed")
    with pytest.raises(StorageError):
        layout.upload_backup_metadata(metadata)
    storage.upload_data.assert_called_once()
    assert storage.upload_data.call_args.kwargs["remote_path"].endswith(
        "cloud_storage_info.json"
    )


@pytest.mark.parametrize("field", ["link", "link_part_name"])
@pytest.mark.parametrize("bad", [0, False, [], {}, ".", ".."])
def test_malformed_retained_links_never_authorize_cleanup(pending_context, field, bad):
    """False-like malformed reference fields must not become an empty reference map."""
    backup, layout, storage, objects = pending_context
    del objects["ch_backup/old/backup_struct.json"]
    del objects["ch_backup/old/backup_light_struct.json"]
    key = "ch_backup/new/backup_struct.json"
    raw = json.loads(objects[key])
    raw["databases"]["db"]["tables"]["table"]["parts"]["all_1_1_0"][field] = bad
    objects[key] = json.dumps(raw)
    with pytest.raises(StorageError, match="references"):
        backup.delete("old", False, delete_unreadable=True)
    assert layout.get_delete_pending("old") is None
    storage.delete_files.assert_not_called()


@pytest.mark.parametrize("command", [["delete", "old"], ["purge"]])
def test_cli_policy_lists_intents_and_purge_partial_finishes_them(
    pending_context, command
):
    """Exercise policy flags, diagnostic listing, force safety, and retry via actual CLI."""
    backup, _, _, objects = pending_context
    del objects["ch_backup/old/backup_struct.json"]
    del objects["ch_backup/old/backup_light_struct.json"]
    app = Group(commands=[delete_command, purge_command, list_command])
    runner = CliRunner()
    result = runner.invoke(app, ["delete", "old", "--force"], obj={"backup": backup})
    assert result.exit_code != 0
    result = runner.invoke(
        app, command + ["--delete-unreadable"], obj={"backup": backup}
    )
    assert result.exit_code == 0, result.output
    result = runner.invoke(
        app, ["list", "-a", "-v", "--format", "json"], obj={"backup": backup}
    )
    assert result.exit_code == 0, result.output
    pending = next(
        record for record in json.loads(result.stdout) if record["name"] == "old"
    )
    assert pending["state"] == "partially_deleted"
    assert pending["delete_pending"]["referenced_by"] == ["new"]
    assert backup.purge()[0] == []
    result = runner.invoke(
        app, ["delete", "new", "--purge-partial"], obj={"backup": backup}
    )
    assert result.exit_code == 0, result.output
    assert not objects


def test_batch_reads_retained_full_once_for_all_pending_backups(pending_context):
    """A complete shared scan protects all targets without reloading per target."""
    backup, layout, storage, _ = pending_context
    for name in ("old", "another"):
        layout.save_delete_pending(name, "missing_metadata")
    storage.download_data.reset_mock()
    backup.purge()
    full_reads = [
        call.args[0]
        for call in storage.download_data.call_args_list
        if call.args[0].endswith("backup_struct.json")
    ]
    assert full_reads == ["ch_backup/new/backup_struct.json"]
    assert layout.get_delete_pending("old")["referenced_by"] == ["new"]
    assert layout.get_delete_pending("another") is None


@pytest.mark.parametrize("name", ["plain", "with-dash"])
def test_cloud_cleanup_retry_keeps_intent_and_deletes_payload_once(
    pending_context, name
):
    """One cleanup pass handles both cloud layouts and retains intent on wait failure."""
    _, layout, storage, objects = pending_context
    layout.claim_cloud_storage(name)
    cloud = f"{layout.get_cloud_storage_path(name)}/cloud_storage/disk/object"
    objects[cloud] = "data"
    layout.save_delete_pending(name, "missing_metadata")
    storage.wait.side_effect = [RuntimeError("delete failed"), None]
    with pytest.raises(StorageError):
        layout.clean_pending_backup(name, {}, set())
    assert layout.get_delete_pending(name) is not None
    assert layout.cloud_storage_owned(name)
    assert layout.clean_pending_backup(name, {}, set())
    deleted = [
        key
        for call in storage.delete_files.call_args_list
        for key in call.kwargs.get("remote_paths", call.args[0] if call.args else [])
    ]
    assert deleted.count(cloud) == 1
    assert deleted[-1] == f"ch_backup/{name}/delete_pending.json"


@pytest.fixture(name="legacy_cloud")
def legacy_cloud_fixture(pending_context):
    """Represent the old dashed-name layout without an ownership claim."""
    backup, layout, storage, objects = pending_context
    meta = layout.get_backup("old")
    assert meta is not None
    objects.clear()
    meta.name = "legacy-backup"
    meta.cloud_storage = CloudStorageMetadata(disks=["s3"], data_copied=True)
    for light, filename in (
        (False, "backup_struct.json"),
        (True, "backup_light_struct.json"),
    ):
        objects[f"ch_backup/legacy-backup/{filename}"] = meta.dump_json(light=light)
    objects["ch_backup/legacy_backup/disks/s3.tar"] = "metadata"
    objects["ch_backup/legacy_backup/cloud_storage/s3/object"] = "payload"
    return backup, layout, storage, objects


def test_explicit_legacy_adoption_unblocks_delete(legacy_cloud):
    """An operator can migrate a known legacy owner without changing its payload."""
    backup, layout, _, objects = legacy_cloud
    before = objects.copy()
    with pytest.raises(StorageError, match="legacy_backup"):
        backup.delete("legacy-backup", False)
    runner = CliRunner()
    app = Group(commands=[adopt_cloud_storage_command])
    result = runner.invoke(
        app, ["adopt-cloud-storage", "legacy-backup"], obj={"backup": backup}
    )
    assert result.exit_code != 0
    assert objects == before
    result = runner.invoke(
        app,
        ["adopt-cloud-storage", "legacy-backup", "--confirm-owner"],
        obj={"backup": backup},
    )
    assert result.exit_code == 0, result.output
    assert layout.cloud_storage_owned("legacy-backup")
    assert all(objects[key] == value for key, value in before.items())
    backup.adopt_cloud_storage("legacy-backup")
    assert backup.delete("legacy-backup", False)[0] == "legacy-backup"
    assert not objects


@pytest.mark.parametrize(
    "obstacle", ["full", "collision", "pending", "owner", "lock", "write"]
)
def test_legacy_adoption_refuses_unknown_or_conflicting_ownership(
    legacy_cloud, obstacle
):
    """Failed validation or persistence never grants ownership or removes data."""
    backup, layout, storage, objects = legacy_cloud
    if obstacle == "full":
        del objects["ch_backup/legacy-backup/backup_struct.json"]
    elif obstacle == "collision":
        objects["ch_backup/legacy_backup/backup_struct.json"] = "unreadable"
    elif obstacle == "pending":
        layout.save_delete_pending("legacy-backup", "missing_metadata")
    elif obstacle == "owner":
        objects["ch_backup/legacy_backup/.cloud_storage_owner.json"] = json.dumps(
            {"version": 1, "backup_name": "legacy_backup"}
        )
    elif obstacle == "lock":
        getattr(backup, "_context").lock_conf = {"zk_flock": False}
    else:
        storage.upload_data.side_effect = StorageError("S3 unavailable")
    before = objects.copy()
    with pytest.raises(StorageError):
        backup.adopt_cloud_storage("legacy-backup")
    assert objects == before
    storage.delete_files.assert_not_called()


@pytest.mark.parametrize("name", ["plain", "with-dash"])
@pytest.mark.parametrize("whole", [False, True])
def test_cloud_deletion_requires_ownership_before_touching_payload(
    pending_context, name, whole
):
    """Both deletion paths preserve unknown cloud data, including same-name prefixes."""
    _, layout, storage, objects = pending_context
    objects[f"ch_backup/{name}/data/part/data.bin"] = "local"
    cloud = layout.get_cloud_storage_data_path(name) + "/s3/object"
    objects[cloud] = "cloud"
    before = objects.copy()
    unfreeze = MagicMock()
    with pytest.raises(StorageError, match="not owned"):
        if whole:
            layout.delete_backup(name, unfreeze)
        else:
            layout.delete_backup_cloud_files(name)
    assert objects == before
    storage.delete_files.assert_not_called()
    unfreeze.assert_not_called()
