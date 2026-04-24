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


_STAT_UNCHANGED = FakeStatResult(
    st_mtime_ns=16890001958000000, st_ctime_ns=16890001958000000
)
_STAT_MTIME_CHANGED = FakeStatResult(
    st_mtime_ns=16890001958000111, st_ctime_ns=16890001958000000
)
_STAT_CTIME_CHANGED = FakeStatResult(
    st_mtime_ns=16890001958000000, st_ctime_ns=16890001958000111
)


@pytest.mark.parametrize(
    "fake_stats, backups_expected_db1, backups_expected_db2",
    [
        # Metadata unchanged in both dbs -> both tables backed up
        ([_STAT_UNCHANGED, _STAT_UNCHANGED, _STAT_UNCHANGED, _STAT_UNCHANGED], 1, 1),
        # db1 table mtime changed after freeze -> db1 skipped, db2 backed up
        (
            [_STAT_UNCHANGED, _STAT_UNCHANGED, _STAT_MTIME_CHANGED, _STAT_UNCHANGED],
            0,
            1,
        ),
        # db1 table ctime changed after freeze -> db1 skipped, db2 backed up
        (
            [_STAT_UNCHANGED, _STAT_UNCHANGED, _STAT_CTIME_CHANGED, _STAT_UNCHANGED],
            0,
            1,
        ),
        # EXCHANGE TABLES between db1 and db2: db1 backed up normally,
        # db2 table ctime changed (EXCHANGE happened) -> db2 skipped
        (
            [_STAT_UNCHANGED, _STAT_UNCHANGED, _STAT_UNCHANGED, _STAT_CTIME_CHANGED],
            1,
            0,
        ),
    ],
)
def test_backup_table_skipping_if_metadata_updated_during_backup(
    fake_stats: List[FakeStatResult],
    backups_expected_db1: int,
    backups_expected_db2: int,
) -> None:
    table_name = "table1"
    db1_name = "db1"
    db2_name = "db2"
    creation_statement = f"ATTACH TABLE {db1_name}.{table_name} UUID '{UUID}' (date Date) ENGINE = MergeTree();"

    # Prepare involved data objects
    context = BackupContext(DEFAULT_CONFIG)  # type: ignore[arg-type]
    db1 = Database(
        db1_name, "Atomic", "/var/lib/clickhouse/metadata/db1.sql", None, None
    )
    db2 = Database(
        db2_name, "Atomic", "/var/lib/clickhouse/metadata/db2.sql", None, None
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

    backup_meta.add_database(db1)
    backup_meta.add_database(db2)
    context.backup_meta = backup_meta

    # Mock external interactions
    # Each database has its own metadata path (EXCHANGE TABLES swaps inodes, not paths)
    tables_by_db = {
        db1_name: [
            Table(
                db1_name,
                table_name,
                "MergeTree",
                [],
                [],
                f"/var/lib/clickhouse/metadata/{db1_name}/{table_name}.sql",
                "",
                UUID,
            )
        ],
        db2_name: [
            Table(
                db2_name,
                table_name,
                "MergeTree",
                [],
                [],
                f"/var/lib/clickhouse/metadata/{db2_name}/{table_name}.sql",
                "",
                UUID,
            )
        ],
    }
    clickhouse_ctl_mock = Mock()
    clickhouse_ctl_mock.get_tables.side_effect = lambda db_name, *a, **kw: tables_by_db[
        db_name
    ]
    clickhouse_ctl_mock.get_disks.return_value = {}
    context.ch_ctl = clickhouse_ctl_mock

    context.backup_layout = Mock()

    read_bytes_mock = Mock(return_value=creation_statement.encode())

    with (
        patch("os.stat", side_effect=fake_stats),
        patch("ch_backup.logic.table.Path", read_bytes=read_bytes_mock),
    ):
        table_backup.backup(
            context,
            [db1, db2],
            {db1_name: [table_name], db2_name: [table_name]},
            schema_only=False,
            multiprocessing_config=DEFAULT_CONFIG["multiprocessing"],  # type: ignore
        )

    assert len(context.backup_meta.get_tables(db1_name)) == backups_expected_db1
    assert len(context.backup_meta.get_tables(db2_name)) == backups_expected_db2
    # One call after each table and one after each database is backed up
    assert clickhouse_ctl_mock.remove_freezed_data.call_count == 4
