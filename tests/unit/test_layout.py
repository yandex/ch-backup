"""Unit tests for backup layout cloud metadata path selection."""

from unittest.mock import MagicMock, patch

from ch_backup.backup.layout import BackupLayout
from ch_backup.backup.metadata.table_metadata import TableMetadata
from ch_backup.config import DEFAULT_CONFIG


class TestCloudStorageMetadataRemotePaths:
    """Tests for filtered cloud metadata remote path selection."""

    # pylint: disable=protected-access

    def test_prefers_old_style_and_filters_exact_per_table_paths(self):
        with (
            patch("ch_backup.backup.layout.StorageLoader"),
            patch("ch_backup.backup.layout.get_encryption") as get_encryption,
        ):
            get_encryption.return_value.metadata_size.return_value = 0
            layout = BackupLayout(DEFAULT_CONFIG)  # type: ignore[arg-type]
        layout._storage_loader = MagicMock()
        layout._config["path_root"] = "ch_backup"

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
        existing_paths = {
            old_style_path: False,
            **{path: True for path in expected_paths},
        }
        existing_paths[f"{backup_path}/disks/{source_disk_name}/db1/table2.tar"] = False

        layout._storage_loader.path_exists.side_effect = existing_paths.get

        remote_paths = layout._get_cloud_storage_metadata_remote_paths(
            backup_name,
            source_disk_name,
            compression=False,
            tables_needed=tables,
        )

        assert list(remote_paths) == expected_paths
        layout._storage_loader.list_dir.assert_not_called()
