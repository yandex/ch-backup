"""Order independence and memory bounds for cloud part fingerprints."""

import hashlib
import json
import tracemalloc

import pytest

from tests.unit.test_backup_copy import (
    _backup,
    _cloud_storage_context,
    _object_storage_metadata,
    _part,
)

# pylint: disable=protected-access

_PART_PATH = "store/abc/uuid/all_1_1_0/"


def _context():
    ctx = _cloud_storage_context("owner")
    ctx.layout.backups["owner"] = _backup(
        "owner",
        parts={"all_1_1_0": {**_part(), "disk_name": "s3"}},
        cloud_storage_disks=["s3"],
    )
    return ctx


@pytest.mark.parametrize("reverse,batch_size", [(True, 1), (False, 8)])
def test_fingerprint_ignores_member_order(monkeypatch, reverse, batch_size):
    ctx = _context()
    copier = ctx.make_copier()
    backup = ctx.layout.backups["owner"]
    part = backup.get_parts()[0]
    files = [
        (_PART_PATH + name, _object_storage_metadata(*objects, version=4))
        for name, objects in [
            ("z", [("key", 1)]),
            ("é", [("key", 1)]),
            ("\n", [("key", 1)]),
            ("file", []),
            ("multi", [("a", 2), ("b", 3)]),
        ]
    ]
    references = [
        copier._cloud_file_reference(backup, part, name.removeprefix(_PART_PATH), data)
        for name, data in files
    ]
    total = sum(
        int(
            hashlib.sha256(json.dumps(ref, separators=(",", ":")).encode()).hexdigest(),
            16,
        )
        for ref in references
    )
    expected = f"v2:{total % (1 << 256):064x}"

    class Archive:
        def read_batch(self, continuation=None, *, include_empty=False):
            assert include_empty
            start = continuation or 0
            end = start + batch_size
            return files[start:end], end if end < len(files) else None

    if reverse:
        files.reverse()
    monkeypatch.setattr(
        ctx.layout, "download_cloud_storage_metadata_archive", lambda *_args: Archive()
    )
    assert (
        copier._cloud_storage_part_fingerprints(backup).get(part.logical_identity)
        == expected
    )


def test_one_large_part_uses_bounded_python_memory(monkeypatch):
    ctx = _context()
    backup = ctx.layout.backups["owner"]
    copier = ctx.make_copier()
    identity = backup.get_parts()[0].logical_identity
    data = _object_storage_metadata(("x" * 4096, 1), version=4)

    class Archive:
        def read_batch(self, continuation=None, *, include_empty=False):
            assert include_empty
            start = continuation or 0
            end = start + 16
            return [(_PART_PATH + str(i), data) for i in range(start, end)], (
                end if end < 2048 else None
            )

    monkeypatch.setattr(
        ctx.layout, "download_cloud_storage_metadata_archive", lambda *_args: Archive()
    )
    tracemalloc.start()
    try:
        fingerprint = copier._cloud_storage_part_fingerprints(backup).get(identity)
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    assert fingerprint and fingerprint.startswith("v2:")
    # The part has 8 MiB of object keys. Hash it in bounded batches.
    assert peak < 4 * 1024 * 1024


def test_malformed_member_invalidates_only_its_part():
    ctx = _context()
    backup = ctx.layout.backups["owner"]
    table = backup.get_table("db1", "t1")
    table.raw_metadata["parts"]["all_2_2_0"] = {**_part(), "disk_name": "s3"}
    data = _object_storage_metadata(("object", 10))
    ctx.layout.cloud_storage_files["ch_backup/owner/disks/s3/db1/t1.tar.gz"] = [
        (_PART_PATH + "valid", data),
        ("store/abc/uuid/all_2_2_0/healthy", data),
        (_PART_PATH + "broken", b"malformed"),
        (_PART_PATH + "later", data),
    ]
    fingerprints = ctx.make_copier()._cloud_storage_part_fingerprints(backup)
    assert fingerprints.get(("db1", "t1", "all_1_1_0")) is None
    assert fingerprints.get(("db1", "t1", "all_2_2_0"))
