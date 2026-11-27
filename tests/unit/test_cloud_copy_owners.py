"""Cloud copies must preserve the representation used by their children."""

import copy
from concurrent.futures import BrokenExecutor
from datetime import datetime, timedelta, timezone

# These tests adapt in-memory transport and exercise the public copy flows.
# pylint: disable=protected-access
from unittest.mock import patch

import pytest

from ch_backup.backup.copier import BackupCopier, _cloud_part_reuse_key
from ch_backup.backup.layout import BackupLayout
from ch_backup.backup.metadata import BackupState
from ch_backup.encryption import (
    get_cloud_storage_data_encryption_metadata,
)
from ch_backup.exceptions import ClickhouseBackupError, StorageError
from tests.unit.test_backup_copy import (
    FakeEngine,
    UnavailableLayout,
    _cloud_storage_context,
    _linked_cloud_copy_context,
    _object_storage_metadata,
    _part,
    _with_files,
)


def _owner_context():
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
    ctx.target_config["copy"]["deduplication_age_limit"] = {"days": 7}
    return ctx


@pytest.fixture(autouse=True)
def command_clock(monkeypatch):
    now = datetime(2026, 1, 8, tzinfo=timezone.utc)
    monkeypatch.setattr("ch_backup.backup.copier.utcnow", lambda: now)


@pytest.mark.parametrize(
    "window,age,state,expected_link",
    [
        (0, 0, BackupState.CREATED, None),
        (7, 7, BackupState.CREATED, "owner"),
        (7, 8, BackupState.CREATED, None),
        (7, -1, BackupState.CREATED, None),
        (7, 1, BackupState.PARTIALLY_DELETED, None),
    ],
)
def test_independent_cloud_owner_window(window, age, state, expected_link):
    ctx = _owner_context()
    _add_same_source(ctx, "child")
    ctx.copy(["owner"])
    owner = ctx.target_layout.backups["owner"]
    owner.start_time += timedelta(days=7 - age)
    owner.state = state
    ctx.target_config["copy"]["deduplication_age_limit"] = {"days": window}
    with patch.object(
        ctx.target_layout, "get_backup", wraps=ctx.target_layout.get_backup
    ) as reads:
        ctx.copy(["child"])
    assert ctx.target_layout.backups["child"].get_parts()[0].link == expected_link
    full_reads = [
        c
        for c in reads.call_args_list
        if c.args == ("owner",) and not c.kwargs.get("use_light_meta")
    ]
    # The files index also considers partially deleted backups inside the window.
    assert len(full_reads) == int(window > 0 and 0 <= age <= window)


@pytest.mark.parametrize("window", [0, 1])
def test_existing_cloud_link_survives_outside_discovery_window(window):
    ctx = _linked_cloud_copy_context()
    ctx.target_layout.backups["owner"] = copy.deepcopy(ctx.layout.backups["owner"])
    ctx.target_config["copy"]["deduplication_age_limit"] = {"days": window}
    ctx.copy(["child"])
    part = ctx.target_layout.backups["child"].get_parts()[0]
    assert (part.link, part.link_part_name) == ("owner", "all_1_1_0")


@pytest.mark.parametrize("broken_executor", [False, True])
def test_optional_cloud_candidate_error(broken_executor, monkeypatch):
    ctx = _owner_context()
    ctx.copy(["owner"])
    _add_same_source(ctx, "child")
    failure = StorageError("candidate unavailable")
    if broken_executor:
        failure.__cause__ = BrokenExecutor("pool unavailable")
    original = BackupCopier._remember_cloud_storage_parts

    def index(self, backup):
        if backup.name == "owner":
            raise failure
        original(self, backup)

    monkeypatch.setattr(BackupCopier, "_remember_cloud_storage_parts", index)
    if broken_executor:
        with pytest.raises(StorageError, match="candidate unavailable"):
            ctx.copy(["child"])
    else:
        ctx.copy(["child"])
        assert ctx.target_layout.backups["child"].get_parts()[0].link is None


def test_completed_owner_keeps_original_objects_and_encryption_on_restart():
    ctx = _owner_context()
    _add_same_source(ctx, "child")
    ctx.copy(["owner", "child"])
    assert ctx.target_layout.backups["child"].get_parts()[0].link == "owner"
    original = ctx.target_layout.backups["owner"].dump()
    ctx.target_layout.copied.clear()
    ctx.target_config["cloud_storage"]["data_encryption"]["key"] = "b" * 32
    ctx.target_config["copy"]["resume"] = False
    ctx.target_config["copy"]["cloud_storage"]["target"]["bucket"] = None
    assert ctx.copy(["owner"]) == ([], ["owner"])
    assert ctx.target_layout.backups["owner"].dump() == original
    assert not ctx.target_layout.copied


@pytest.mark.parametrize(
    "state", [BackupState.PARTIALLY_DELETED, BackupState.DELETING, BackupState.FAILED]
)
def test_restart_preserves_published_owner_during_deletion(state):
    ctx = _owner_context()
    _add_same_source(ctx, "child")
    ctx.copy(["owner", "child"])
    ctx.target_layout.backups["owner"].state = state
    ctx.target_layout.copied.clear()
    ctx.target_config["copy"]["resume"] = False
    with pytest.raises(ClickhouseBackupError, match="deletion"):
        ctx.copy(["owner"])
    assert not ctx.target_layout.copied
    assert ctx.target_layout.backups["child"].get_parts()[0].link == "owner"


def test_lost_publication_response_preserves_completed_cloud_backup():
    ctx = _owner_context()
    upload = ctx.target_layout.upload_backup_metadata

    def lose_response(backup):
        upload(backup)
        if backup.state == BackupState.CREATED:
            raise StorageError("response lost")

    with patch.object(
        ctx.target_layout, "upload_backup_metadata", side_effect=lose_response
    ):
        with pytest.raises(StorageError, match="response lost"):
            ctx.copy(["owner"])
    assert ctx.target_layout.backups["owner"].state == BackupState.CREATED
    assert ctx.copy(["owner"]) == ([], ["owner"])


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
    ctx = _owner_context()
    _add_same_source(ctx, "child")
    ctx.copy(["owner"])
    if difference == "key":
        ctx.target_config["cloud_storage"]["data_encryption"]["key"] = "b" * 32
    elif difference == "chunk_size":
        ctx.target_config["cloud_storage"]["data_encryption"]["chunk_size"] = 128
    elif difference == "plaintext":
        ctx.target_config["copy"]["cloud_storage"]["encrypt_data"] = False
    ctx.target_layout.copied.clear()
    ctx.copy(["child"])
    child = ctx.target_layout.backups["child"]
    assert child.get_parts()[0].link == (None if difference else "owner")
    payload = [
        entry
        for entry in ctx.target_layout.copied
        if entry.dst_storage["credentials"]["bucket"] == "disk-copy"
    ]
    assert bool(payload) == bool(difference)


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
def test_staged_owner_payload_is_forwarded_unchanged(
    encrypted_source: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Stage shared payload once and preserve its bytes across metadata batches.

    Force separate batches for two files referencing the same object. The files
    scope must deduplicate those references without encrypting staged payload;
    the later cloud scope applies the requested encryption exactly once.
    """
    monkeypatch.setattr(
        "ch_backup.storage.metadata_archive.METADATA_ARCHIVE_BATCH_BYTES", 1
    )
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
            "store/abc/uuid/all_1_1_0/" + name,
            _object_storage_metadata((prefix + "/xyz/data", 100), version=5),
        )
        for name in ("data.bin", "checksums.txt")
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


def test_interrupted_copy_cleanup_uses_planned_keys_before_archives_exist():
    """
    Copy can fail after writing payload but before publishing disk archives.
    Cleanup needs the copy manifest at that point, including planned keys whose
    writes did not complete.
    """
    ctx = _owner_context()
    ctx.target_layout.object_errors["data/xyz/bbb"] = StorageError("interrupted")
    with pytest.raises(StorageError, match="interrupted"):
        ctx.copy(["owner"])
    pending = ctx.target_layout.backups["owner"]
    assert pending.state == BackupState.FAILED
    prefix = "copies/owner"
    assert not any(
        pending.name in path for path in ctx.target_layout.cloud_storage_files
    )
    manifest = BackupLayout.read_cloud_copy_manifest(ctx.target_layout, pending.name)
    assert len(manifest) == 1
    location, keys = manifest[0]
    assert (location["bucket"], location["path"]) == ("disk-copy", prefix)
    assert list(keys) == [prefix + "/xyz/aaa", prefix + "/xyz/bbb"]
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
    part = _owner_context().layout.backups["owner"].get_parts()[0]
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
    ctx = _owner_context()
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
    ctx = _owner_context()
    _add_same_source(ctx, "child")
    ctx.copy(["owner"])
    owner = ctx.target_layout.backups["owner"]
    parts = owner.get_table("db1", "t1").raw_metadata["parts"]
    parts["broken"] = parts.pop("all_1_1_0")
    ctx.target_layout.copied.clear()
    assert ctx.copy(["child"]) == (["child"], [])
    child = ctx.target_layout.backups["child"]
    assert child.state == BackupState.CREATED
    replacement = child
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
    "difference",
    [
        None,
        "size",
        "inline",
        "filename",
        "endpoint",
        "override",
        "duplicate",
        "empty_duplicate",
    ],
)
def test_cloud_fingerprint_tracks_objects_instead_of_archive_encoding(difference):
    """
    Archive encoding and entry order can change without changing the files.
    Reuse should survive those changes but stop when file content or source
    provenance changes, including an explicit override of a saved fingerprint.
    """
    ctx = _owner_context()
    ctx.target_config["copy"]["cloud_storage"]["encrypt_data"] = False
    _add_same_source(ctx, "child")
    ctx.copy(["owner"])
    owner = "owner"
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
    elif difference == "duplicate":
        files.append(files[0])
    elif difference == "empty_duplicate":
        files.insert(0, (files[0][0], b""))
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
    assert child.get_parts()[0].link == (None if difference else owner)
    payload = [
        entry
        for entry in ctx.target_layout.copied
        if entry.dst_storage["credentials"]["bucket"] == "disk-copy"
    ]
    assert bool(payload) is bool(difference)
    if difference in ("duplicate", "empty_duplicate", "inline"):
        assert child.get_parts()[0].cloud_storage_fingerprint is None
    if difference == "override":
        assert {entry.src_path for entry in payload} == {
            "other/xyz/aaa",
            "other/xyz/bbb",
        }


def test_named_backup_claim_precedes_cloud_payload_transfer():
    ctx = _owner_context()
    claimed: list[str] = []
    transfer = ctx.target_layout.copy_objects

    def check_claim(entries, **kwargs):
        assert claimed == ["owner"]
        return transfer(entries, **kwargs)

    with (
        patch.object(
            ctx.target_layout, "claim_cloud_storage", side_effect=claimed.append
        ),
        patch.object(ctx.target_layout, "copy_objects", side_effect=check_claim),
    ):
        ctx.copy(["owner"])


def test_retry_that_links_to_another_owner_keeps_abandoned_upload_for_explicit_cleanup():
    ctx = _owner_context()
    _add_same_source(ctx, "B")
    ctx.target_layout.object_errors["data/xyz/bbb"] = StorageError("interrupted")
    with pytest.raises(StorageError, match="interrupted"):
        ctx.copy(["owner"])
    ctx.target_layout.object_errors.clear()
    ctx.copy(["B"])
    objects = ctx.store.bucket("disk-copy")
    assert "copies/owner/xyz/aaa" in objects
    assert "copies/B/xyz/aaa" in objects
    with patch.object(
        FakeEngine, "delete_files", side_effect=AssertionError("unexpected delete")
    ):
        ctx.copy(["owner"])

    assert ctx.target_layout.backups["owner"].get_parts()[0].link == "B"
    assert "copies/owner/xyz/aaa" in objects
    assert {"copies/B/xyz/aaa", "copies/B/xyz/bbb"} <= objects.keys()
    manifests = ctx.target_layout.read_cloud_copy_manifest("owner")
    assert len(manifests) == 1
    location, keys = manifests[0]
    assert location["path"] == "copies/owner"
    assert set(keys) == {"copies/owner/xyz/aaa", "copies/owner/xyz/bbb"}


def test_retry_merges_planned_keys_by_destination_before_transfer():
    ctx = _owner_context()
    archive = "ch_backup/owner/disks/s3/db1/t1.tar.gz"
    target = ctx.target_config["copy"]["cloud_storage"]["target"]
    for path, key in [("original", "first"), ("second", "other"), ("original", "last")]:
        target["path"] = path
        ctx.layout.cloud_storage_files[archive] = [
            ("all_1_1_0/data.bin", _object_storage_metadata((f"xyz/{key}", 100))),
        ]
        ctx.target_layout.object_errors[f"data/xyz/{key}"] = StorageError("interrupted")
        with pytest.raises(StorageError, match="interrupted"):
            ctx.copy(["owner"])
    groups = ctx.target_layout.read_cloud_copy_manifest("owner")
    assert len(groups) == 2
    assert {location["path"]: set(keys) for location, keys in groups} == {
        "original/owner": {"original/owner/xyz/first", "original/owner/xyz/last"},
        "second/owner": {"second/owner/xyz/other"},
    }


@pytest.mark.parametrize("lost_ack", [False, True])
def test_retry_after_cleanup_location_publication_failure(lost_ack):
    ctx = _owner_context()
    upload = FakeEngine.upload_data

    def fail_location(engine, data, path):
        if path.endswith("/cleanup/locations.json"):
            if lost_ack:
                upload(engine, data, path)
            raise StorageError("location write failed")
        return upload(engine, data, path)

    with patch.object(FakeEngine, "upload_data", fail_location):
        with pytest.raises(StorageError, match="location write failed"):
            ctx.copy(["owner"])
    assert not ctx.store.bucket("disk-copy")
    ctx.copy(["owner"])
    manifests = ctx.target_layout.read_cloud_copy_manifest("owner")
    assert len(manifests) == 1
    location, keys = manifests[0]
    assert location["path"] == "copies/owner"
    assert set(keys) == {"copies/owner/xyz/aaa", "copies/owner/xyz/bbb"}
