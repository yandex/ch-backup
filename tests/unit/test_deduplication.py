"""
Deduplication unit tests.
"""

from unittest.mock import MagicMock, Mock

import pytest

from ch_backup.backup.deduplication import (
    PartDedupInfo,
    _populate_dedup_info,
    deduplicate_parts,
)
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
    context.config = {
        "deduplication_batch_size": 500,
        "deduplication_verify_links": True,
    }
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


@pytest.mark.parametrize("verify_links", [True, False])
def test_linked_part_is_collected_unverified(verify_links):
    """
    Data of a linked part may have been lost since the backup holding the link
    was checked, so it has to be checked again before it is reused, unless
    the check is turned off.
    """
    linked_part = PartMetadata(
        database="db1",
        table="table1",
        name="all_1_1_0",
        checksum="checksum",
        size=1024,
        files=["checksums.txt"],
        tarball=True,
        disk_name="default",
        link="20181015T210300",
    )
    backup = _make_backup(data_copied=False, parts=[linked_part])
    source_backup = _make_backup(data_copied=False, parts=[])
    source_backup.name = "20181015T210300"

    context = Mock()
    context.config = {
        "deduplication_batch_size": 500,
        "deduplication_verify_links": verify_links,
    }
    context.backup_meta.hostname = backup.hostname
    context.backup_layout.reload_backup.side_effect = lambda b, **_: b

    _populate_dedup_info(
        context,
        [backup, source_backup],
        [Database("db1", "Atomic", "/var/lib/clickhouse/metadata/db1.sql", None, None)],
    )

    expected = PartDedupInfo(
        database="db1",
        table="table1",
        name="all_1_1_0",
        backup_name="20181015T210300",
        checksum="checksum",
        size=1024,
        files=["checksums.txt"],
        tarball=True,
        disk_name="default",
        verified=not verify_links,
        encrypted=linked_part.encrypted,
    )
    context.ch_ctl.insert_deduplication_info.assert_called_once_with(
        [expected.to_sql()]
    )


def _dedup_row(name: str) -> dict:
    """Helper: a row of deduplication info matching a frozen part."""
    return {
        "name": name,
        "backup_name": DEDUP_BACKUP_NAME,
        "link_part_name": "",
        "checksum": "checksum",
        "size": 1024,
        "files": ["checksums.txt"],
        "tarball": True,
        "disk_name": "default",
        "verified": False,
        "encrypted": False,
    }


def test_table_is_listed_once_for_all_batches():
    """
    Data of a table in a source backup is listed once and reused by the next
    batches of the same table.
    """
    context = Mock()
    context.ch_ctl.get_deduplication_info.side_effect = [
        [_dedup_row("all_1_1_0")],
        [_dedup_row("all_2_2_0")],
    ]
    layout = context.backup_layout
    layout.get_table_data_files.return_value = {
        "all_1_1_0": {"all_1_1_0.tar": 1},
        "all_2_2_0": {"all_2_2_0.tar": 2},
    }
    layout.check_data_part_files.return_value = True
    stored_files: dict = {}

    deduplicated = [
        deduplicate_parts(context, "db1", "table1", {}, stored_files) for _ in range(2)
    ]

    assert [list(parts) for parts in deduplicated] == [["all_1_1_0"], ["all_2_2_0"]]
    layout.get_table_data_files.assert_called_once_with(
        DEDUP_BACKUP_NAME, "db1", "table1"
    )
    assert [c.args[1] for c in layout.check_data_part_files.call_args_list] == [
        {"all_1_1_0.tar": 1},
        {"all_2_2_0.tar": 2},
    ]
