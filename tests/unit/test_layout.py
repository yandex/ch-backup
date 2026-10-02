"""Unit tests for backup layout cloud metadata path selection."""

import copy
import os
from collections import Counter
from unittest.mock import MagicMock, patch

from ch_backup.backup.layout import BackupLayout
from ch_backup.backup.metadata.table_metadata import TableMetadata
from ch_backup.clickhouse.models import Disk, Table
from ch_backup.config import DEFAULT_CONFIG


def make_layout() -> BackupLayout:
    """Helper: build a BackupLayout with a mocked storage loader."""
    config: dict = copy.deepcopy(DEFAULT_CONFIG)
    config["backup"]["path_root"] = "ch_backup"
    with (
        patch("ch_backup.backup.layout.StorageLoader"),
        patch("ch_backup.backup.layout.get_encryption") as get_encryption,
    ):
        get_encryption.return_value.metadata_size.return_value = 0
        layout = BackupLayout(config)  # type: ignore[arg-type]
    setattr(layout, "_storage_loader", MagicMock())
    return layout


class TestCloudStorageMetadataRemotePaths:
    """Tests for filtered cloud metadata remote path selection."""

    # pylint: disable=protected-access

    def test_prefers_old_style_and_filters_exact_per_table_paths(self):
        layout = make_layout()

        backup_name = "backup"
        source_disk_name = "s3"
        tables = [
            TableMetadata("db1", "table1", "MergeTree", None),
            TableMetadata("db1", "table2", "MergeTree", None),
            TableMetadata("db2", "table3", "MergeTree", None),
        ]

        backup_path = layout.get_backup_path(backup_name)
        old_style_path = f"{backup_path}/disks/{source_disk_name}.tar"
        expected_paths = [
            f"{backup_path}/disks/{source_disk_name}/db1/table1.tar",
            f"{backup_path}/disks/{source_disk_name}/db2/table3.tar",
        ]

        layout._storage_loader.path_exists.side_effect = {old_style_path: False}.get  # type: ignore[attr-defined]
        layout._storage_loader.list_dir.side_effect = lambda _disk_path, **_kwargs: [  # type: ignore[attr-defined]
            *expected_paths,
            f"{backup_path}/disks/{source_disk_name}/db2/table4.tar",
        ]

        remote_paths = layout._get_cloud_storage_metadata_remote_paths(
            backup_name,
            source_disk_name,
            compression=False,
            desired_tables=tables,
        )

        assert Counter(remote_paths) == Counter(expected_paths)


class TestCloudStorageMetadataUpload:
    """Tests for reading cloud storage metadata from a given disk."""

    # pylint: disable=protected-access
    # The storage loader is a mock, pylint infers the class it replaces.
    # pylint: disable=no-member

    _BACKUP_NAME = "20260101T000000"

    @staticmethod
    def _make_layout() -> tuple[BackupLayout, MagicMock]:
        """Helper: build a BackupLayout and expose its storage loader."""
        layout = make_layout()
        return layout, layout._storage_loader  # type: ignore[return-value]

    def _make_backup_meta(self) -> MagicMock:
        backup_meta = MagicMock()
        backup_meta.get_sanitized_name.return_value = self._BACKUP_NAME
        backup_meta.cloud_storage.compressed = False
        backup_meta.cloud_storage.encrypted = False
        return backup_meta

    @staticmethod
    def _make_table(disk: Disk) -> Table:
        return Table(
            "db1",
            "table1",
            "MergeTree",
            [disk],
            [os.path.join(disk.path, "store/abc/abcdef")],
            "",
            "",
            "some-uuid",
        )

    def test_has_frozen_cloud_storage_data(self):
        """
        Frozen data is looked up in the shadow directory of the given disk.
        """
        layout, _ = self._make_layout()
        disk = Disk("s3", "/var/lib/clickhouse/disks/s3/", "s3")
        table = self._make_table(disk)

        with patch("ch_backup.backup.layout.dir_is_empty") as dir_is_empty:
            dir_is_empty.return_value = False
            assert layout.has_frozen_cloud_storage_data(
                self._make_backup_meta(), disk, table
            )

        dir_is_empty.assert_called_once_with(
            f"/var/lib/clickhouse/disks/s3/shadow/{self._BACKUP_NAME}/store/abc/abcdef",
            ["frozen_metadata.txt"],
        )

    def test_upload_reads_files_from_source_disk(self):
        """
        With source_disk set, metadata is read from it while the tarball still
        lands under the name of the original disk.
        """
        layout, loader = self._make_layout()
        disk = Disk("s3", "/var/lib/clickhouse/disks/s3/", "s3")
        source_disk = Disk("s3_backup", "/var/lib/clickhouse/disks/s3_backup/", "s3")
        table = self._make_table(disk)

        with patch("ch_backup.backup.layout.dir_is_empty", return_value=False):
            layout.upload_cloud_storage_metadata(
                self._make_backup_meta(), disk, table, source_disk=source_disk
            )

        call = loader.upload_files_tarball_scan.call_args.kwargs
        assert call["dir_path"] == (
            f"/var/lib/clickhouse/disks/s3_backup/shadow/{self._BACKUP_NAME}"
            "/store/abc/abcdef"
        )
        assert call["remote_path"] == (
            f"ch_backup/{self._BACKUP_NAME}/disks/s3/db1/table1.tar"
        )
        assert call["tar_base_dir"] == "store/abc/abcdef"

    def test_upload_reads_files_from_the_disk_itself_by_default(self):
        """
        Without source_disk the behaviour must stay as it was.
        """
        layout, loader = self._make_layout()
        disk = Disk("s3", "/var/lib/clickhouse/disks/s3/", "s3")
        table = self._make_table(disk)

        with patch("ch_backup.backup.layout.dir_is_empty", return_value=False):
            layout.upload_cloud_storage_metadata(self._make_backup_meta(), disk, table)

        call = loader.upload_files_tarball_scan.call_args.kwargs
        assert call["dir_path"] == (
            f"/var/lib/clickhouse/disks/s3/shadow/{self._BACKUP_NAME}/store/abc/abcdef"
        )


class TestCloudStorageMetadataLookup:
    """Tests for the lookup of disk metadata of a table in a backup."""

    # pylint: disable=protected-access

    @staticmethod
    def _probed_paths(layout: BackupLayout) -> list:
        """Helper: return paths the lookup checked for existence."""
        path_exists = layout._storage_loader.path_exists
        return [call.args[0] for call in path_exists.call_args_list]  # type: ignore[attr-defined]

    def test_dashed_backup_name_is_looked_up_under_the_sanitized_path(self):
        """
        Disk metadata is uploaded under the sanitized backup name, so looking it
        up by the raw name would never find it and deduplication would be lost.
        """
        layout = make_layout()
        layout._storage_loader.path_exists.return_value = False  # type: ignore[attr-defined]

        layout.has_cloud_storage_metadata("my-backup", "db1", "table1", "s3")

        assert self._probed_paths(layout) == [
            "ch_backup/my_backup/disks/s3/db1/table1.tar.gz",
            "ch_backup/my_backup/disks/s3/db1/table1.tar",
        ]

    def test_metadata_is_found_when_stored_uncompressed(self):
        """
        Either extension means the metadata is there.
        """
        layout = make_layout()
        layout._storage_loader.path_exists.side_effect = {  # type: ignore[attr-defined]
            "ch_backup/my_backup/disks/s3/db1/table1.tar": True
        }.get

        assert layout.has_cloud_storage_metadata("my-backup", "db1", "table1", "s3")

    def test_missing_metadata_is_reported(self):
        """
        A backup without the metadata of the table cannot be linked to.
        """
        layout = make_layout()
        layout._storage_loader.path_exists.return_value = False  # type: ignore[attr-defined]

        assert not layout.has_cloud_storage_metadata("my-backup", "db1", "table1", "s3")


class TestCloudStorageDataDeletion:
    """Tests for deletion of cloud storage data of a backup."""

    # pylint: disable=protected-access

    @staticmethod
    def _make_layout() -> tuple[BackupLayout, MagicMock]:
        """Helper: build a BackupLayout that records paths passed for deletion."""
        layout = make_layout()
        layout._storage_loader.list_dir.side_effect = lambda path, **_kwargs: [  # type: ignore[attr-defined]
            f"{path}/object"
        ]
        delete_files = MagicMock()
        setattr(layout, "_delete_files", delete_files)
        return layout, delete_files

    def test_delete_backup_with_dashed_name_deletes_cloud_storage_data(self):
        """
        ClickHouse writes cloud storage data under the sanitized backup name, so
        deleting the backup path alone would leave the data in the bucket.
        """
        layout, delete_files = self._make_layout()

        layout.delete_backup("my-backup")

        assert delete_files.call_args.args[0] == [
            "ch_backup/my-backup/object",
            "ch_backup/my_backup/disks/object",
            "ch_backup/my_backup/cloud_storage/object",
        ]

    def test_delete_backup_keeps_a_backup_named_as_the_sanitized_path(self):
        """
        Names differing only in '-' share the sanitized path, so listing it as a
        whole would delete metadata and data parts of the other backup.
        """
        layout = make_layout()
        layout._storage_loader.list_dir.side_effect = lambda path, **_kwargs: {  # type: ignore[attr-defined]
            "ch_backup/my-backup": ["ch_backup/my-backup/backup_struct.json"],
            "ch_backup/my_backup": [
                "ch_backup/my_backup/backup_struct.json",
                "ch_backup/my_backup/data/db/table/part.tar",
                "ch_backup/my_backup/disks/s3/db/table.tar",
            ],
            "ch_backup/my_backup/disks": ["ch_backup/my_backup/disks/s3/db/table.tar"],
        }.get(
            path, []
        )
        delete_files = MagicMock()
        setattr(layout, "_delete_files", delete_files)

        layout.delete_backup("my-backup")

        assert delete_files.call_args.args[0] == [
            "ch_backup/my-backup/backup_struct.json",
            "ch_backup/my_backup/disks/s3/db/table.tar",
        ]

    def test_delete_backup_without_dashes_deletes_the_path_once(self):
        """
        Both names match, the backup path must not be listed twice.
        """
        layout, delete_files = self._make_layout()

        layout.delete_backup("20260101T000000")

        assert delete_files.call_args.args[0] == ["ch_backup/20260101T000000/object"]

    def test_delete_cloud_storage_data_keeps_the_rest_of_the_backup(self):
        """
        Partially deleted backup keeps data parts shared with newer backups,
        while its cloud storage data is deleted whole, under both names.
        """
        layout, delete_files = self._make_layout()

        layout.delete_cloud_storage_data("my-backup")

        assert delete_files.call_args.args[0] == [
            "ch_backup/my_backup/disks/object",
            "ch_backup/my_backup/cloud_storage/object",
        ]
