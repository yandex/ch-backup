"""
Deduplication unit tests.
"""

from unittest.mock import MagicMock, Mock

from ch_backup.backup.deduplication import _populate_dedup_info
from ch_backup.backup.metadata import BackupState, PartMetadata
from ch_backup.clickhouse.models import Database

DEDUP_BACKUP_NAME = "20181016T210300"


def _make_backup(data_copied: bool, parts: list[PartMetadata]) -> MagicMock:
    """Helper: mock metadata of a backup to deduplicate against."""
    table = MagicMock()
    table.name = "table1"
    table.engine = "MergeTree"
    table.get_parts.return_value = parts

    backup = MagicMock()
    backup.name = DEDUP_BACKUP_NAME
    backup.hostname = "clickhouse01.test_net_711"
    backup.state = BackupState.CREATED
    backup.cloud_storage.disks = ["s3"]
    backup.cloud_storage.data_copied = data_copied
    backup.get_databases.return_value = ["db1"]
    backup.get_tables.return_value = [table]
    return backup


def _collected_dedup_info(data_copied: bool, disk_name: str) -> Mock:
    """Helper: collect deduplication info of a single part of a backup."""
    part = PartMetadata(
        database="db1",
        table="table1",
        name="all_1_1_0",
        checksum="checksum",
        size=1024,
        files=["checksums.txt"],
        tarball=True,
        disk_name=disk_name,
    )
    backup = _make_backup(data_copied, [part])

    context = Mock()
    context.config = {"deduplication_batch_size": 500}
    context.backup_meta.hostname = backup.hostname
    context.backup_layout.reload_backup.return_value = backup

    _populate_dedup_info(
        context,
        [backup],
        [Database("db1", "Atomic", "/var/lib/clickhouse/metadata/db1.sql", None, None)],
    )
    return context.ch_ctl.insert_deduplication_info


def test_cloud_storage_part_without_copied_data_is_not_a_candidate():
    """
    Data of such a part stays in the bucket of the source installation, there
    is nothing in the backup to link to.
    """
    insert = _collected_dedup_info(data_copied=False, disk_name="s3")

    insert.assert_not_called()


def test_cloud_storage_part_with_copied_data_is_a_candidate():
    """
    Data of such a part is stored in the backup and can be reused.
    """
    insert = _collected_dedup_info(data_copied=True, disk_name="s3")

    insert.assert_called_once()


def test_local_part_of_a_backup_with_cloud_storage_is_a_candidate():
    """
    Copying of cloud storage data says nothing about parts on local disks.
    """
    insert = _collected_dedup_info(data_copied=False, disk_name="default")

    insert.assert_called_once()
