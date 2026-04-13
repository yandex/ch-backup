from dataclasses import dataclass
from typing import List
from unittest.mock import Mock, patch

import pytest

from ch_backup.backup.metadata.backup_metadata import BackupMetadata
from ch_backup.backup_context import BackupContext
from ch_backup.clickhouse.models import Database, Table
from ch_backup.config import DEFAULT_CONFIG
from ch_backup.logic.table import TableBackup

UUID = "fa8ff291-1922-4b7f-afa7-06633d5e16ae"


@dataclass
class FakeStatResult:
    st_mtime_ns: int
    st_ctime_ns: int


@pytest.mark.parametrize(
    "fake_stats, backups_expected",
    [
        (
            [
                FakeStatResult(
                    st_mtime_ns=16890001958000000, st_ctime_ns=16890001958000000
                ),
                FakeStatResult(
                    st_mtime_ns=16890001958000000, st_ctime_ns=16890001958000000
                ),
            ],
            1,
        ),
        (
            [
                FakeStatResult(
                    st_mtime_ns=16890001958000000, st_ctime_ns=16890001958000000
                ),
                FakeStatResult(
                    st_mtime_ns=16890001958000111, st_ctime_ns=16890001958000000
                ),
            ],
            0,
        ),
        (
            [
                FakeStatResult(
                    st_mtime_ns=16890001958000000, st_ctime_ns=16890001958000000
                ),
                FakeStatResult(
                    st_mtime_ns=16890001958000000, st_ctime_ns=16890001958000111
                ),
            ],
            0,
        ),
    ],
)
def test_backup_table_skipping_if_metadata_updated_during_backup(
    fake_stats: List[FakeStatResult], backups_expected: int
) -> None:
    table_name = "table1"
    db_name = "db1"
    creation_statement = (
        f"ATTACH TABLE db1.table1 UUID '{UUID}' (date Date) ENGINE = MergeTree();"
    )

    # Prepare involved data objects
    context = BackupContext(DEFAULT_CONFIG)  # type: ignore[arg-type]
    db = Database(
        db_name, "MergeTree", "/var/lib/clickhouse/metadata/db1.sql", None, None
    )
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
    with (
        patch("os.stat", side_effect=fake_stats),
        patch("ch_backup.logic.table.Path", read_bytes=read_bytes_mock),
    ):
        table_backup.backup(
            context,
            [db],
            {db_name: [table_name]},
            schema_only=False,
            multiprocessing_config=DEFAULT_CONFIG["multiprocessing"],  # type: ignore
        )

    assert len(context.backup_meta.get_tables(db_name)) == backups_expected
    # One call after each table and one after database is backuped
    assert clickhouse_ctl_mock.remove_freezed_data.call_count == 2
