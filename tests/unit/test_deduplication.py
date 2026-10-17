"""
Deduplication unit tests.
"""

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, Mock

import pytest
from loguru import logger

from ch_backup.backup.deduplication import (
    PartDedupInfo,
    _populate_dedup_info,
    collect_dedup_info,
    deduplicate_parts,
)
from ch_backup.backup.metadata import (
    BackupMetadata,
    BackupState,
    PartMetadata,
    TableMetadata,
)
from ch_backup.clickhouse.models import Database
from ch_backup.config import DEFAULT_CONFIG

DEDUP_BACKUP_NAME = "20181016T210300"


@pytest.fixture(name="window_backups")
def fixture_window_backups():
    database = Database("db1", "Atomic", "/metadata/db1.sql", None, None)
    backups = []
    for day in [4, 3, 2, 1, 0, -1, -6, -7]:
        backup = BackupMetadata(
            name=f"day-{day}",
            version="1",
            ch_version="25.1",
            time_format=DEFAULT_CONFIG["backup"]["time_format"],
            hostname="clickhouse01",
        )
        backup.start_time = datetime(2026, 10, 14, tzinfo=timezone.utc) + timedelta(
            days=day
        )
        backup.add_database(database)
        backup.add_table(TableMetadata("db1", "table1", "MergeTree", None))
        backup.add_part(
            PartMetadata(
                database="db1",
                table="table1",
                name=f"all_{day + 8}_{day + 8}_0",
                checksum="checksum",
                size=1024,
                files=["checksums.txt"],
                tarball=True,
                disk_name="default",
            )
        )
        backups.append(backup)
    return database, backups


@pytest.fixture(name="window_context")
def fixture_window_context():
    context = Mock()
    context.config = dict(DEFAULT_CONFIG["backup"])
    context.backup_meta.schema_only = False
    context.backup_meta.hostname = "clickhouse01"
    context.backup_layout.reload_backup.side_effect = lambda backup, **_: backup
    return context


@pytest.mark.parametrize(
    "now_day,start_time,expected",
    [
        pytest.param(15, None, [1, 0, -1, -6], id="unset"),
        pytest.param(15, 14, [1, 0], id="one-day-window"),
        pytest.param(17, 14, [3, 2, 1, 0], id="three-day-window"),
        pytest.param(21, 14, [4, 3, 2, 1, 0], id="limit-reached"),
        pytest.param(24, 14, [4, 3], id="sliding-window"),
    ],
)
def test_collect_dedup_info_window(
    monkeypatch, window_context, window_backups, *, now_day, start_time, expected
):
    database, backups = window_backups
    clock = Mock(return_value=datetime(2026, 10, now_day, tzinfo=timezone.utc))
    monkeypatch.setattr("ch_backup.backup.deduplication.utcnow", clock)
    backups = [backup for backup in backups if backup.start_time <= clock.return_value]
    if start_time is not None:
        window_context.config["deduplication_start_time"] = (
            f"2026-10-{start_time:02d}T00:00:00Z"
        )

    collect_dedup_info(window_context, [database], backups)

    rows = [
        row
        for call in window_context.ch_ctl.insert_deduplication_info.call_args_list
        for row in call.args[0]
    ]
    assert [row.split(",")[3] for row in rows] == [f"'day-{day}'" for day in expected]
    clock.assert_called_once_with()


@pytest.mark.parametrize(
    "start_time,expected",
    [
        (None, "2026-10-08T00:00:00+00:00 (determined by age_limit)"),
        ("2026-10-01T00:00:00Z", "2026-10-08T00:00:00+00:00 (determined by age_limit)"),
        (
            "2026-10-14T00:00:00Z",
            "2026-10-14T00:00:00+00:00 (determined by start_time)",
        ),
    ],
)
def test_collect_dedup_info_logs_lower_bound(
    monkeypatch, window_context, start_time, expected
):
    window_context.config["deduplication_start_time"] = start_time
    monkeypatch.setattr(
        "ch_backup.backup.deduplication.utcnow",
        lambda: datetime(2026, 10, 15, tzinfo=timezone.utc),
    )
    messages: list[str] = []
    sink = logger.add(messages.append, format="{message}", level="INFO")
    try:
        collect_dedup_info(window_context, [], [])
    finally:
        logger.remove(sink)

    assert [message.strip() for message in messages] == [
        f"Deduplication window lower bound: {expected}"
    ]


@pytest.mark.parametrize("microseconds,eligible", [(-1, False), (0, True), (1, True)])
def test_collect_dedup_info_inclusive_start_boundary(
    monkeypatch, window_context, window_backups, microseconds, eligible
):
    database, backups = window_backups
    start_time = datetime(2026, 10, 14, tzinfo=timezone.utc)
    window_context.config["deduplication_start_time"] = start_time
    monkeypatch.setattr(
        "ch_backup.backup.deduplication.utcnow",
        lambda: datetime(2026, 10, 15, tzinfo=timezone.utc),
    )
    backup = backups[0]
    backup.start_time = start_time + timedelta(microseconds=microseconds)

    collect_dedup_info(window_context, [database], [backup])

    assert (
        bool(window_context.ch_ctl.insert_deduplication_info.call_args_list) is eligible
    )


def test_collect_dedup_info_excludes_links_to_before_start(
    monkeypatch, window_context, window_backups
):
    database, backups = window_backups
    window_context.config["deduplication_start_time"] = datetime(
        2026, 10, 14, tzinfo=timezone.utc
    )
    monkeypatch.setattr(
        "ch_backup.backup.deduplication.utcnow",
        lambda: datetime(2026, 10, 19, tzinfo=timezone.utc),
    )
    table = backups[0].get_table("db1", "table1")
    part = table.get_parts()[0]
    table.raw_metadata["parts"][part.name]["link"] = "day--1"

    collect_dedup_info(window_context, [database], backups)

    rows = [
        row
        for call in window_context.ch_ctl.insert_deduplication_info.call_args_list
        for row in call.args[0]
    ]
    assert [row.split(",")[3] for row in rows] == [
        "'day-3'",
        "'day-2'",
        "'day-1'",
        "'day-0'",
    ]


def test_collect_dedup_info_completed_backup_links_and_early_exit(
    monkeypatch, window_context, window_backups
):
    database, backups = window_backups
    window_context.config["deduplication_start_time"] = datetime(
        2026, 10, 14, tzinfo=timezone.utc
    )
    monkeypatch.setattr(
        "ch_backup.backup.deduplication.utcnow",
        lambda: datetime(2026, 10, 19, tzinfo=timezone.utc),
    )
    for backup in backups:
        backup.state = BackupState.CREATED

    newest = backups[0]
    # Original sources lie exactly at the lower bound and one day before it.
    for source in backups[4:6]:
        part = source.get_table("db1", "table1").get_parts()[0]
        newest.add_part(
            PartMetadata(
                database="db1",
                table="table1",
                name=part.name,
                checksum=part.checksum,
                size=part.size,
                files=part.files,
                tarball=part.tarball,
                disk_name=part.disk_name,
                link=source.name,
            )
        )

    collect_dedup_info(window_context, [database], backups)

    rows = [
        row
        for call in window_context.ch_ctl.insert_deduplication_info.call_args_list
        for row in call.args[0]
    ]
    assert {(row.split(",")[2], row.split(",")[3]) for row in rows} == {
        ("'all_12_12_0'", "'day-4'"),
        ("'all_8_8_0'", "'day-0'"),
    }
    window_context.backup_layout.reload_backup.assert_called_once_with(
        newest, use_light_meta=False
    )


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
