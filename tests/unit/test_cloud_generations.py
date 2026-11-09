"""Generation publication must preserve the representation used by old children."""

import copy

# These tests adapt in-memory transport and exercise the public copy flows.
# pylint: disable=protected-access
from unittest.mock import patch

import pytest

from ch_backup.backup.layout import BackupLayout
from ch_backup.backup.metadata import BackupMetadata, BackupState
from ch_backup.encryption import get_cloud_storage_data_encryption_metadata
from ch_backup.exceptions import StorageError
from tests.unit.test_backup_copy import (
    UnavailableLayout,
    _backup,
    _cloud_storage_context,
    _linked_cloud_copy_context,
    _object_storage_metadata,
    _part,
    _with_files,
)
from tests.unit.test_layout import make_layout


def _generation_context():
    ctx = _cloud_storage_context("owner")
    ctx.target_layout.store = ctx.store
    table = ctx.layout.backups["owner"].get_table("db1", "t1")
    table.raw_metadata["parts"].update(
        {"all_1_1_0": {**_part(), "disk_name": "s3", "tarball": False}}
    )
    ctx.layout.cloud_storage_files["ch_backup/owner/disks/s3/db1/t1.tar.gz"] = (
        ctx.layout.cloud_storage_files["ch_backup/owner/disks/s3/db1/t1.tar.gz"][:2]
    )
    ctx.target_config["copy"]["cloud_storage"]["encrypt_data"] = True
    return ctx


def test_generation_metadata_survives_full_and_light_roundtrip():
    backup = _generation_context().layout.backups["owner"]
    backup.cloud_generation = ".cloud_generation_" + "1" * 32
    backup.generation_of = "original"
    for light in (False, True):
        loaded = BackupMetadata.load(backup.dump(light=light))
        assert loaded.cloud_generation == backup.cloud_generation
        assert loaded.generation_of == "original"


def test_current_generation_check_uses_full_publication_manifest():
    """A failed light-metadata write must not make the previous generation current again."""
    copier = _generation_context().make_copier()
    generation, published = _backup("generation"), _backup("owner")
    generation.generation_of = published.name
    published.cloud_generation = generation.name
    previous = copy.deepcopy(published)
    previous.cloud_generation = "previous"

    def read(_name, use_light_meta=False):
        return previous if use_light_meta else published

    with patch.object(copier._target_layout, "get_backup", side_effect=read):
        assert copier._is_current_cloud_generation(generation)
        generation.name = "previous"
        assert not copier._is_current_cloud_generation(generation)


def test_owner_recopy_keeps_previous_generation_and_key():
    ctx = _generation_context()
    ctx.copy(["owner"])
    original = copy.deepcopy(ctx.target_layout.backups["owner"])
    old_generation = original.cloud_generation
    assert original.get_parts()[0].link == old_generation
    old_owner = copy.deepcopy(ctx.target_layout.backups[old_generation])
    old_paths = {entry.dst_path for entry in ctx.target_layout.copied}
    ctx.target_layout.copied.clear()
    ctx.target_config["cloud_storage"]["data_encryption"]["key"] = "b" * 32
    ctx.target_config["copy"]["resume"] = False
    ctx.copy(["owner"])
    current = ctx.target_layout.backups["owner"]
    assert current.cloud_generation != old_generation
    assert ctx.target_layout.backups[old_generation].dump() == old_owner.dump()
    assert (
        current.cloud_storage.data_encryption != old_owner.cloud_storage.data_encryption
    )
    assert all(
        entry.dst_path not in old_paths
        for entry in ctx.target_layout.copied
        if entry.dst_storage["credentials"]["bucket"] == "disk-copy"
    )


def test_failed_replacement_keeps_published_manifest():
    ctx = _generation_context()
    ctx.copy(["owner"])
    published = copy.deepcopy(ctx.target_layout.backups["owner"])
    ctx.target_config["copy"]["resume"] = False
    ctx.target_config["cloud_storage"]["data_encryption"]["key"] = "b" * 32
    ctx.target_layout.copy_error = RuntimeError("interrupted replacement")
    with pytest.raises(Exception, match="interrupted replacement|Failed to copy"):
        ctx.copy(["owner"])
    assert ctx.target_layout.backups["owner"].dump() == published.dump()


def test_separate_scopes_reuse_pending_generation():
    ctx = _generation_context()
    ctx.copy(["owner"], scopes=["files"])
    pending = ctx.target_layout.backups["owner"].cloud_generation
    assert pending
    ctx.copy(["owner"], scopes=["cloud_storage"])
    published = ctx.target_layout.backups["owner"]
    assert published.state == BackupState.CREATED
    assert published.cloud_generation == pending
    assert (
        published.cloud_storage.data_encryption
        == get_cloud_storage_data_encryption_metadata(ctx.target_config["encryption"])
    )


def test_pending_policy_change_does_not_overwrite_previous_attempt():
    ctx = _generation_context()
    ctx.target_layout.object_errors["data/xyz/bbb"] = RuntimeError("failed")
    with pytest.raises(RuntimeError):
        ctx.copy(["owner"])
    previous = ctx.target_layout.backups["owner"].cloud_generation
    ctx.target_layout.object_errors.clear()
    ctx.target_config["cloud_storage"]["data_encryption"]["key"] = "b" * 32
    ctx.copy(["owner"])
    assert ctx.target_layout.backups["owner"].cloud_generation != previous
    assert previous in ctx.target_layout.backups


def test_generation_records_do_not_appear_in_user_listings():

    ctx = _generation_context()
    ctx.copy(["owner"])
    layout = make_layout()
    with (
        patch.object(
            layout, "get_backup_names", side_effect=ctx.target_layout.get_backup_names
        ),
        patch.object(layout, "get_backup", side_effect=ctx.target_layout.get_backup),
    ):
        assert [backup.name for backup in layout.get_backups()] == ["owner"]
        assert len(layout.get_backups(include_generations=True)) == 2


def test_legacy_owner_snapshot_is_not_redirected_by_publication():

    ctx = _generation_context()
    original = ctx.layout.backups["owner"]
    original.cloud_storage.source = {
        "endpoint_url": None,
        "bucket": "old",
        "path": "old/owner",
    }
    layout = make_layout()
    storage = getattr(layout, "_storage_loader")
    snapshots: dict[str, str] = {}
    storage.path_exists.side_effect = lambda path: path in snapshots
    storage.upload_data.side_effect = lambda data, remote_path: snapshots.setdefault(
        remote_path, data
    )
    storage.download_data.side_effect = lambda path, **_: snapshots[path]
    layout.preserve_legacy_cloud_owner(original)
    original.cloud_storage.source = {
        "endpoint_url": None,
        "bucket": "new",
        "path": "new/generation",
    }
    layout.preserve_legacy_cloud_owner(original)
    owner = layout.get_backup("owner", for_reference=True)
    assert owner is not None and owner.cloud_storage.source is not None
    assert owner.cloud_storage.source["bucket"] == "old"


def test_full_cloud_copy_rejects_inconsistent_owner_encryption():
    ctx = _linked_cloud_copy_context()
    child = ctx.layout.backups["child"]
    owner = ctx.layout.backups["owner"]
    child.cloud_storage.data_copied = owner.cloud_storage.data_copied = True
    owner.cloud_storage.source = None
    owner.cloud_storage.data_encryption = get_cloud_storage_data_encryption_metadata(
        ctx.config["encryption"]
    )
    with pytest.raises(Exception, match="encryption metadata differ"):
        ctx.copy(["child"])
    assert not ctx.target_layout.copied


@pytest.mark.parametrize("encrypted_source", [False, True])
def test_staged_owner_payload_is_forwarded_unchanged(encrypted_source):
    ctx = _linked_cloud_copy_context()
    owner = ctx.layout.backups["owner"]
    child = ctx.layout.backups["child"]
    owner.cloud_storage.data_copied = True
    owner.cloud_storage.source = None
    if encrypted_source:
        encryption = get_cloud_storage_data_encryption_metadata(
            ctx.config["encryption"]
        )
        owner.cloud_storage.data_encryption = encryption
        child.cloud_storage.data_encryption = encryption
        ctx.target_config["cloud_storage"]["data_encryption"]["key"] = None
    else:
        ctx.target_config["copy"]["cloud_storage"]["encrypt_data"] = True
    prefix = "ch_backup/owner/cloud_storage/s3"
    ctx.layout.cloud_storage_files["ch_backup/owner/disks/s3/db1/t1.tar.gz"] = [
        (
            "store/abc/uuid/all_1_1_0/data.bin",
            _object_storage_metadata((prefix + "/xyz/data", 100), version=5),
        )
    ]
    _with_files(
        ctx,
        "owner",
        {
            "disks/s3/db1/t1.tar.gz": 50,
            "cloud_storage/s3/xyz/data": 140 if encrypted_source else 100,
        },
    )
    ctx.copy(["child"], scopes=["files"])
    staged = [
        entry
        for entry in ctx.target_layout.copied
        if entry.src_path == prefix + "/xyz/data"
    ]
    assert len(staged) == 1
    assert not staged[0].encrypt
    assert staged[0].dst_size == (140 if encrypted_source else 100)
    ctx.layout = UnavailableLayout()  # type: ignore[assignment]
    ctx.target_layout.source = None
    ctx.target_layout.copied.clear()
    ctx.copy(["child"], scopes=["cloud_storage"])
    payload = [
        entry
        for entry in ctx.target_layout.copied
        if entry.src_path == staged[0].dst_path
    ]
    assert len(payload) == 1
    assert payload[0].encrypt is not encrypted_source
    assert payload[0].dst_size == 140


def test_interrupted_generation_cleanup_uses_planned_keys_before_archives_exist():
    """
    Copy can fail after writing payload but before publishing disk archives.
    Cleanup needs the copy manifest at that point, including planned keys whose
    writes did not complete.
    """
    ctx = _generation_context()
    ctx.target_layout.object_errors["data/xyz/bbb"] = StorageError("interrupted")
    with pytest.raises(StorageError, match="interrupted"):
        ctx.copy(["owner"])
    pending = ctx.target_layout.backups[
        ctx.target_layout.backups["owner"].cloud_generation
    ]
    assert pending.state == BackupState.CREATING
    prefix = pending.cloud_storage.source["path"]
    assert not any(
        pending.name in path for path in ctx.target_layout.cloud_storage_files
    )
    keys = BackupLayout.collect_cloud_object_keys_for_deletion(
        ctx.target_layout, pending, prefix
    )
    assert keys == [prefix + "/xyz/aaa", prefix + "/xyz/bbb"]
    objects = ctx.store.bucket("disk-copy")
    assert prefix + "/xyz/aaa" in objects
    assert prefix + "/xyz/bbb" not in objects
