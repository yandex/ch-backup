"""Generation publication must preserve the representation used by old children."""

import copy

# These tests adapt in-memory transport and exercise the public copy flows.
# pylint: disable=protected-access
from unittest.mock import patch

import pytest

from ch_backup.backup.copier import _cloud_part_reuse_key
from ch_backup.backup.deduplication import (
    collect_dedup_references_for_batch_backup_deletion,
)
from ch_backup.backup.layout import BackupLayout
from ch_backup.backup.metadata import BackupMetadata, BackupState
from ch_backup.encryption import (
    get_cloud_storage_data_encryption_config,
    get_cloud_storage_data_encryption_metadata,
    get_encryption,
)
from ch_backup.exceptions import ClickhouseBackupError, StorageError
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


def _add_same_source(ctx, name):

    backup = copy.deepcopy(ctx.layout.backups["owner"])
    backup.name = name
    ctx.layout.backups[name] = backup
    ctx.layout.cloud_storage_files[f"ch_backup/{name}/disks/s3/db1/t1.tar.gz"] = list(
        ctx.layout.cloud_storage_files["ch_backup/owner/disks/s3/db1/t1.tar.gz"]
    )
    _with_files(ctx, name, {"disks/s3/db1/t1.tar.gz": 50})


@pytest.mark.parametrize("difference", [None, "key", "chunk_size", "plaintext"])
def test_reuse_requires_requested_encryption_representation(difference):
    ctx = _generation_context()
    _add_same_source(ctx, "child")
    ctx.copy(["owner"])
    owner_generation = ctx.target_layout.backups["owner"].cloud_generation
    if difference == "key":
        ctx.target_config["cloud_storage"]["data_encryption"]["key"] = "b" * 32
    elif difference == "chunk_size":
        ctx.target_config["cloud_storage"]["data_encryption"]["chunk_size"] = 128
    elif difference == "plaintext":
        ctx.target_config["copy"]["cloud_storage"]["encrypt_data"] = False
    ctx.target_layout.copied.clear()
    ctx.copy(["child"])
    child = ctx.target_layout.backups["child"]
    assert child.get_parts()[0].link == (
        child.cloud_generation if difference else owner_generation
    )
    payload = [
        entry
        for entry in ctx.target_layout.copied
        if entry.dst_storage["credentials"]["bucket"] == "disk-copy"
    ]
    assert bool(payload) == bool(difference)


@pytest.mark.parametrize("interrupted", [False, True])
def test_child_decrypts_original_bytes_after_owner_replacement(interrupted):

    ctx = _generation_context()
    _add_same_source(ctx, "child")
    ctx.target_config["cloud_storage"]["data_encryption"]["key"] = "a" * 32
    plaintext = {"data/xyz/aaa": b"A" * 1000, "data/xyz/bbb": b"B" * 10}
    ciphertext = {}
    copy_metadata = ctx.target_layout._copy_content

    def transfer(entry):
        copy_metadata(entry)
        if entry.dst_storage["credentials"]["bucket"] == "disk-copy":
            settings = get_cloud_storage_data_encryption_config(ctx.target_config)
            ciphertext[entry.dst_path] = get_encryption(
                settings["type"], settings
            ).encrypt(plaintext[entry.src_path])
            if interrupted and settings["key"] == "b" * 32:
                raise RuntimeError("transfer interrupted")

    with patch.object(ctx.target_layout, "_copy_content", side_effect=transfer):
        ctx.copy(["owner"])
        ctx.copy(["child"])
        child = copy.deepcopy(ctx.target_layout.backups["child"])
        old_name = child.get_parts()[0].link
        old_owner = copy.deepcopy(ctx.target_layout.backups[old_name])
        old_key = old_owner.cloud_storage.source["path"] + "/xyz/aaa"
        old_bytes = ciphertext[old_key]
        ctx.target_config["cloud_storage"]["data_encryption"]["key"] = "b" * 32
        ctx.target_config["copy"]["resume"] = False
        if interrupted:
            with pytest.raises(RuntimeError, match="transfer interrupted"):
                ctx.copy(["owner"])
        else:
            ctx.copy(["owner"])
    assert ctx.target_layout.backups["child"].dump() == child.dump()
    assert ctx.target_layout.backups[old_name].dump() == old_owner.dump()
    assert ciphertext[old_key] == old_bytes
    assert (
        get_encryption("nacl", {"key": "a" * 32}).decrypt(old_bytes)
        == plaintext["data/xyz/aaa"]
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


def test_retained_children_protect_only_their_generation():

    ctx = _generation_context()
    _add_same_source(ctx, "child")
    ctx.copy(["owner", "child"])
    child = ctx.target_layout.backups["child"]
    original = child.get_parts()[0].link
    ctx.target_config["cloud_storage"]["data_encryption"]["key"] = "b" * 32
    ctx.target_config["copy"]["resume"] = False
    ctx.copy(["owner"])
    replacement = ctx.target_layout.backups["owner"].cloud_generation
    ctx.target_layout.reload_backup = lambda backup, **_: ctx.target_layout.get_backup(
        backup.name
    )
    refs = collect_dedup_references_for_batch_backup_deletion(
        ctx.target_layout, [child], {original, replacement, "owner"}
    )
    assert refs[original]["db1"]["t1"] == {"all_1_1_0"}
    assert not refs[replacement]
    assert not refs["owner"]
    assert not collect_dedup_references_for_batch_backup_deletion(
        ctx.target_layout, [], {original}
    )[original]


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


def test_failed_publication_cannot_offer_a_retryable_owner():

    ctx = _generation_context()
    _add_same_source(ctx, "child")
    upload = ctx.target_layout.upload_backup_metadata

    def fail_publication(backup):
        if backup.name == "owner" and backup.state == BackupState.CREATED:
            raise RuntimeError("public manifest failed")
        upload(backup)

    with patch.object(
        ctx.target_layout, "upload_backup_metadata", side_effect=fail_publication
    ):
        with pytest.raises(RuntimeError, match="public manifest failed"):
            ctx.copy(["owner"])
    unpublished = ctx.target_layout.backups["owner"].cloud_generation
    ctx.copy(["child"])
    assert ctx.target_layout.backups["child"].get_parts()[0].link != unpublished
    ctx.copy(["owner"])
    assert ctx.target_layout.backups["owner"].cloud_generation != unpublished


def test_same_policy_restart_replaces_own_generation():
    ctx = _generation_context()
    _add_same_source(ctx, "child")
    ctx.copy(["owner"])
    original = ctx.target_layout.backups["owner"].cloud_generation
    ctx.target_config["copy"]["resume"] = False
    ctx.target_layout.copied.clear()
    ctx.copy(["owner"])
    current = ctx.target_layout.backups["owner"].cloud_generation
    assert current != original
    assert ctx.target_layout.backups["owner"].get_parts()[0].link == current
    assert any(entry.encrypt for entry in ctx.target_layout.copied)
    ctx.copy(["child"])
    assert ctx.target_layout.backups["child"].get_parts()[0].link == current


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


@pytest.mark.parametrize(
    "field,value,equal",
    [
        ("name", "all_1_1_0_7", True),
        ("database", "other", False),
        ("table", "other", False),
        ("disk_name", "other", False),
        ("name", "all_1_2_0", False),
        ("name", "all_1_1_1", False),
        ("encrypted", False, False),
    ],
)
def test_cloud_reuse_requires_part_address_and_format(field, value, equal):
    """
    Equal object fingerprints do not make parts from different tables, block
    ranges or storage formats interchangeable. Mutation-only renaming remains
    eligible for reuse.
    """
    part = _generation_context().layout.backups["owner"].get_parts()[0]
    original = _cloud_part_reuse_key(part, "uuid", "fingerprint", None)
    if field in ("name", "database", "table"):
        setattr(part, field, value)
    else:
        setattr(part.raw_metadata, field, value)
    assert (
        _cloud_part_reuse_key(part, "uuid", "fingerprint", None) == original
    ) is equal


@pytest.mark.parametrize("field", ["version", "type", "key_id", "chunk_size"])
def test_cloud_reuse_requires_complete_encryption_descriptor(field):
    """
    A reference to ciphertext is useful only with a compatible representation.
    Comparing every descriptor field and rejecting missing fields prevents an
    unknown representation from being treated as compatible.
    """
    ctx = _generation_context()
    part = ctx.layout.backups["owner"].get_parts()[0]
    representation = get_cloud_storage_data_encryption_metadata(
        ctx.config["encryption"]
    )
    original = _cloud_part_reuse_key(part, None, "f", representation)
    different = {**representation, field: "different"}
    assert _cloud_part_reuse_key(part, None, "f", different) != original
    del different[field]
    with pytest.raises(ClickhouseBackupError, match="Incomplete"):
        _cloud_part_reuse_key(part, None, "f", different)


def test_unparseable_target_candidate_cannot_authorize_cloud_reuse():
    """
    A stored candidate with an unparsable name cannot establish mutation
    equivalence. Copy must materialize a complete replacement instead of linking
    to that candidate or publishing a reference without payload.
    """
    ctx = _generation_context()
    _add_same_source(ctx, "child")
    ctx.copy(["owner"])
    generation = ctx.target_layout.backups[
        ctx.target_layout.backups["owner"].cloud_generation
    ]
    parts = generation.get_table("db1", "t1").raw_metadata["parts"]
    parts["broken"] = parts.pop("all_1_1_0")
    ctx.target_layout.copied.clear()
    assert ctx.copy(["child"]) == (["child"], [])
    child = ctx.target_layout.backups["child"]
    assert child.state == BackupState.CREATED
    assert child.cloud_generation != generation.name
    replacement = ctx.target_layout.backups[child.cloud_generation]
    assert replacement.state == BackupState.CREATED
    assert replacement.generation_of == "child"
    assert child.get_parts()[0].resolve_stored_reference("child") == (
        replacement.name,
        "db1",
        "t1",
        "all_1_1_0",
    )
    assert replacement.get_parts()[0].link is None
    assert {
        (entry.src_path, entry.dst_path)
        for entry in ctx.target_layout.copied
        if entry.dst_storage["credentials"]["bucket"] == "disk-copy"
    } == {
        ("data/xyz/aaa", f"copies/{replacement.name}/xyz/aaa"),
        ("data/xyz/bbb", f"copies/{replacement.name}/xyz/bbb"),
    }
    assert {
        path
        for path in ctx.store.bucket("disk-copy")
        if path.startswith(f"copies/{replacement.name}/")
    } == {
        f"copies/{replacement.name}/xyz/aaa",
        f"copies/{replacement.name}/xyz/bbb",
    }


@pytest.mark.parametrize(
    "difference", [None, "size", "inline", "filename", "endpoint", "override"]
)
def test_cloud_fingerprint_tracks_objects_instead_of_archive_encoding(difference):
    """
    Archive encoding and entry order can change without changing the files.
    Reuse should survive those changes but stop when file content or source
    provenance changes, including an explicit override of a saved fingerprint.
    """
    ctx = _generation_context()
    ctx.target_config["copy"]["cloud_storage"]["encrypt_data"] = False
    _add_same_source(ctx, "child")
    ctx.copy(["owner"])
    owner = ctx.target_layout.backups["owner"].cloud_generation
    archive = "ch_backup/child/disks/s3/db1/t1.tar.gz"
    files = [
        (
            "store/abc/uuid/all_1_1_0/checksums.txt",
            _object_storage_metadata(("data/xyz/bbb", 10), version=5),
        ),
        (
            "store/abc/uuid/all_1_1_0/data.bin",
            _object_storage_metadata(
                ("data/xyz/aaa", 999 if difference == "size" else 1000), version=5
            ),
        ),
    ]
    if difference == "inline":
        files[1] = (files[1][0], files[1][1][:-1] + b"inline\n")
    elif difference == "filename":
        files[1] = (files[1][0].replace("data.bin", "other.bin"), files[1][1])
    elif difference == "endpoint":
        ctx.ch_config.config["storage_configuration"]["disks"]["s3"][
            "endpoint"
        ] = "http://another:9000/cloud-storage/data/"
    elif difference == "override":
        raw = (
            ctx.layout.backups["child"]
            .get_table("db1", "t1")
            .raw_metadata["parts"]["all_1_1_0"]
        )
        raw["cloud_storage_fingerprint"] = (
            ctx.target_layout.backups[owner]
            .get_table("db1", "t1")
            .raw_metadata["parts"]["all_1_1_0"]["cloud_storage_fingerprint"]
        )
        ctx.target_config["copy"]["cloud_storage"]["source"]["path"] = "other"
    ctx.layout.cloud_storage_files[archive] = files
    ctx.target_layout.copied.clear()
    ctx.copy(["child"])
    child = ctx.target_layout.backups["child"]
    assert child.get_parts()[0].link == (
        child.cloud_generation if difference else owner
    )
    payload = [
        entry
        for entry in ctx.target_layout.copied
        if entry.dst_storage["credentials"]["bucket"] == "disk-copy"
    ]
    assert bool(payload) is bool(difference)
    if difference == "override":
        assert {entry.src_path for entry in payload} == {
            "other/xyz/aaa",
            "other/xyz/bbb",
        }
