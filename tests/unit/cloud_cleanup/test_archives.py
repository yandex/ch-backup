"""Archive boundaries and object selection for whole-owner cleanup."""

# pylint: disable=protected-access

from typing import cast
from unittest.mock import MagicMock

import pytest

from ch_backup.backup.metadata import (
    PartMetadata,
)
from ch_backup.exceptions import ConfigurationError
from tests.unit.test_backup_copy import _object_storage_metadata
from tests.unit.test_layout import make_layout


@pytest.mark.parametrize("disk_name", ["", ".", "..", "../other", "/other", "s3/other"])
def test_archive_listing_rejects_disk_path_components(disk_name):
    layout = make_layout()
    loader = cast(MagicMock, layout._storage_loader)
    with pytest.raises(ConfigurationError, match="disk name"):
        layout.list_cloud_storage_metadata_archives("backup1", disk_name, False, "all")
    loader.path_exists.assert_not_called()
    loader.list_dir.assert_not_called()


@pytest.mark.parametrize(
    "archive",
    [
        "ch_backup/other/disks/s3/table.tar",
        "ch_backup/backup1/disks/s3/../../other.tar",
    ],
)
def test_archive_listing_rejects_paths_outside_disk_directory(archive):
    layout = make_layout()
    loader = cast(MagicMock, layout._storage_loader)
    loader.path_exists.return_value = False
    loader.list_dir.return_value = [archive]
    with pytest.raises(ConfigurationError, match="archive.*outside"):
        layout.list_cloud_storage_metadata_archives("backup1", "s3", False, "all")


@pytest.mark.parametrize("version", [3, 5], ids=["relative-keys", "full-keys"])
def test_cleanup_selects_recorded_keys_and_skips_link_only_disks(cloud_layout, version):
    """
    A prefix can contain unrelated data, and a linked disk can have no local
    archive. Selecting recorded payload keys across both metadata formats
    avoids broad deletion and false missing-archive failures.
    """
    c = cloud_layout
    prefix = c.meta.cloud_storage.source["path"]
    c.meta.cloud_storage.add_disk("linked")
    c.meta.get_table("db", "table").add_part(
        PartMetadata(
            "db",
            "table",
            "all_2_2_0",
            "c",
            1,
            [],
            False,
            disk_name="linked",
            link="other",
        )
    )
    key = prefix + "/a" if version == 5 else "a"
    archive = next(iter(c.archives))
    c.archives[archive] = [
        ("data.bin", _object_storage_metadata((key, 1), (key, 1), version=version)),
        ("frozen_metadata.txt", b"not an object descriptor"),
    ]
    assert list(c.layout.collect_cloud_object_keys_for_deletion(c.meta, prefix)) == [
        prefix + "/a"
    ]
    c.engine.delete_files.assert_not_called()


def test_final_cleanup_scans_existing_archives_on_link_only_disks(cloud_layout):
    """A partial owner can retain archives after its unreferenced part records disappear."""
    c = cloud_layout
    c.meta.get_table("db", "table").add_part(
        PartMetadata(
            "db", "table", "all_2_2_0", "c", 1, [], False, disk_name="s3", link="other"
        )
    )
    assert list(
        c.layout.collect_cloud_object_keys_for_deletion(c.meta, "copies/backup1")
    ) == ["copies/backup1/a", "copies/backup1/b", "copies/backup1/c"]


def test_cleanup_rejects_object_keys_outside_payload_prefix(cloud_layout):
    """An archive cannot authorize deletion outside this backup's payload."""
    c = cloud_layout
    archive = next(iter(c.archives))
    c.archives[archive] = [
        ("data.bin", _object_storage_metadata(("outside/a", 1), version=5))
    ]
    with pytest.raises(ConfigurationError, match="outside"):
        c.layout.collect_cloud_object_keys_for_deletion(c.meta, "copies/backup1")
    c.engine.delete_files.assert_not_called()
