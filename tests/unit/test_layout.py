"""Unit tests for backup layout cloud metadata path selection."""

from collections import Counter
from unittest.mock import MagicMock, call, patch

import pytest

from ch_backup.backup.layout import BackupLayout
from ch_backup.backup.metadata.table_metadata import TableMetadata
from ch_backup.config import DEFAULT_CONFIG
from ch_backup.exceptions import StorageError


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

        layout._storage_loader.path_exists.side_effect = {old_style_path: False}.get
        layout._storage_loader.list_dir.side_effect = lambda _disk_path, **_kwargs: [
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


def test_prefix_delete_keeps_light_metadata_until_retry_succeeds() -> None:
    layout = BackupLayout.__new__(BackupLayout)
    layout._config = {"path_root": "backups"}  # pylint: disable=protected-access
    layout._storage_loader = MagicMock()  # pylint: disable=protected-access
    storage = layout._storage_loader  # pylint: disable=protected-access
    storage.list_dir.return_value = [
        "backups/old/data.tar",
        "backups/old/backup_struct.json",
        "backups/old/backup_light_struct.json",
    ]
    storage.wait.side_effect = [RuntimeError("async deletion failed"), None]
    unfreeze = MagicMock()

    with pytest.raises(StorageError):
        layout.delete_backup("old", unfreeze)

    storage.delete_files.assert_called_once_with(
        remote_paths=["backups/old/data.tar"], is_async=True
    )
    unfreeze.assert_not_called()

    layout.delete_backup("old", unfreeze)

    unfreeze.assert_called_once_with()
    assert storage.delete_files.call_args_list[-2:] == [
        call(["backups/old/backup_struct.json"], is_async=False),
        call(["backups/old/backup_light_struct.json"], is_async=False),
    ]
