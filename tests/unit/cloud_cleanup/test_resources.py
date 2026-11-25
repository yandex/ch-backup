"""Memory use while planning whole-owner cleanup."""

# pylint: disable=protected-access

import tracemalloc

from ch_backup.backup.metadata import (
    PartMetadata,
)
from ch_backup.storage.metadata_archive import spool_metadata
from tests.unit.test_backup_copy import _object_storage_metadata


def test_cleanup_bounds_metadata_and_key_memory(cloud_layout):
    """Planning must not retain archive contents or all object keys in Python."""
    c = cloud_layout
    c.meta.add_part(
        PartMetadata("db", "table", "all_1_1_0", "c", 1, [], False, disk_name="s3")
    )
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
        prepared = c.layout.collect_cloud_object_keys_for_deletion(
            c.meta, "copies/backup1"
        )
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    assert prepared is not None
    assert peak < 6 * 1024 * 1024, f"Cleanup planning allocated {peak} bytes"
