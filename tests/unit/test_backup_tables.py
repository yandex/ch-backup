from typing import List
from unittest.mock import Mock, patch

import pytest

from ch_backup.backup.deduplication import DedupInfo
from ch_backup.backup.metadata.backup_metadata import BackupMetadata
from ch_backup.backup_context import BackupContext
from ch_backup.clickhouse.models import Database, Table
from ch_backup.config import DEFAULT_CONFIG
from ch_backup.logic.table import TableBackup

UUID = "fa8ff291-1922-4b7f-afa7-06633d5e16ae"


@pytest.mark.parametrize(
    "mtime, backups_expected",
    [([1689000195.8, 1689000195.8], 1), ([1689000195.8, 1689000200.1], 0)],
)
def test_backup_table_skipping_if_metadata_updated_during_backup(
    mtime: List[float], backups_expected: int
) -> None:
    table_name = "table1"
    db_name = "db1"
    creation_statement = (
        f"ATTACH TABLE db1.table1 UUID '{UUID}' (date Date) ENGINE = MergeTree();"
    )

    # Prepare involved data objects
    context = BackupContext(DEFAULT_CONFIG)  # type: ignore[arg-type]
    db = Database(db_name, "MergeTree", "/var/lib/clickhouse/metadata/db1.sql")
    dedup_info = DedupInfo()
    table_backup = TableBackup()
    backup_meta = BackupMetadata(
        name="20181017T210300",
        path="ch_backup/20181017T210300",
        version="1.0.100",
        ch_version="19.1.16",
        time_format="%Y-%m-%dT%H:%M:%S%Z",
        hostname="clickhouse01.test_net_711",
    )

    backup_meta.add_database(db)
    context.backup_meta = backup_meta

    # Mock external interactions
    clickhouse_ctl_mock = Mock()
    clickhouse_ctl_mock.get_tables.return_value = [
        Table(
            db_name,
            table_name,
            "MergeTree",
            [],
            [],
            "/var/lib/clickhouse/metadata/db1/table1.sql",
            "",
            UUID,
        ),
    ]
    clickhouse_ctl_mock.get_disks.return_value = {}
    context.ch_ctl = clickhouse_ctl_mock

    context.backup_layout = Mock()

    read_bytes_mock = Mock(return_value=creation_statement.encode())

    # Backup table
    with patch("os.path.getmtime", side_effect=mtime), patch(
        "ch_backup.logic.table.Path", read_bytes=read_bytes_mock
    ):
        table_backup.backup(
            context, [db], {db_name: [table_name]}, dedup_info, schema_only=False
        )

    assert len(context.backup_meta.get_tables(db_name)) == backups_expected
    assert clickhouse_ctl_mock.remove_freezed_data.call_count == 1
