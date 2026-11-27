"""Cleanup planning memory and temporary archive ownership."""

# pylint: disable=protected-access

import tracemalloc
from copy import deepcopy

import pytest

from ch_backup.backup.deduplication import (
    collect_dedup_references_for_batch_backup_deletion,
)
from ch_backup.backup.metadata import (
    PartMetadata,
)
from ch_backup.exceptions import StorageError
from ch_backup.storage.metadata_archive import spool_metadata
from tests.unit.test_backup_copy import _object_storage_metadata


@pytest.mark.parametrize("mode", ["whole", "manifest"])
def test_cleanup_bounds_metadata_and_key_memory(cloud_layout, mode, seed_manifests):
    """Planning must not retain archive contents or all object keys in Python."""
    c = cloud_layout
    c.meta.add_part(
        PartMetadata("db", "table", "all_1_1_0", "c", 1, [], False, disk_name="s3")
    )
    if mode == "manifest":
        keys = (f"copies/backup1/{index:08d}-" + "x" * 200 for index in range(20000))
        seed_manifests(c, (c.meta.cloud_storage.source, keys))
    else:
        archive = spool_metadata(
            (
                f"db/table/all_{1 + index % 2}_{1 + index % 2}_0/file-{index}",
                _object_storage_metadata((f"{index:08d}-" + "x" * 200, 1)),
            )
            for index in range(20000)
        )
        c.layout.download_cloud_storage_metadata_archive.side_effect = None
        c.layout.download_cloud_storage_metadata_archive.return_value = archive
    tracemalloc.start()
    try:
        if mode == "manifest":
            prepared = c.layout.read_cloud_copy_manifest(c.meta.name)
        else:
            prepared = c.layout.collect_cloud_object_keys_for_deletion(
                c.meta, "copies/backup1"
            )
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    assert prepared is not None
    assert peak < 6 * 1024 * 1024, f"Cleanup planning allocated {peak} bytes"


@pytest.mark.parametrize("unreadable", [False, True])
def test_retained_references_protect_stored_parts_and_fail_closed(cleanup, unreadable):
    """A failed retained manifest cannot authorize deletion of shared stored parts."""
    c = cleanup
    children = []
    for name in ("child1", "child2"):
        child = deepcopy(c.meta)
        child.name = name
        child.add_part(
            PartMetadata(
                "db",
                "table",
                "all_1_1_0_7",
                "c",
                1,
                [],
                False,
                link=c.meta.name,
                link_part_name="all_1_1_0",
            )
        )
        children.append(child)
    c.layout.reload_backup.side_effect = [
        children[0],
        StorageError("unreadable retained metadata") if unreadable else children[1],
    ]
    if unreadable:
        with pytest.raises(StorageError, match="unreadable retained metadata"):
            collect_dedup_references_for_batch_backup_deletion(
                c.layout, children, {c.meta.name}
            )
    else:
        references = collect_dedup_references_for_batch_backup_deletion(
            c.layout, children, {c.meta.name}
        )
        assert references[c.meta.name] == {"db": {"table": {"all_1_1_0"}}}
    c.layout.upload_backup_metadata.assert_not_called()
    c.layout.delete_backup.assert_not_called()
