import copy
from typing import List
from unittest.mock import Mock, patch

from ch_backup.backup.metadata.backup_metadata import BackupMetadata
from ch_backup.backup.metadata.part_metadata import PartMetadata
from ch_backup.backup.metadata.table_metadata import TableMetadata
from ch_backup.backup_context import BackupContext
from ch_backup.clickhouse.models import Database
from ch_backup.logic.upload_part_observer import UploadPartObserver
from tests.unit.utils import parametrize

UUID = "fa8ff291-1922-4b7f-afa7-06633d5e16ae"
DB_NAME = "test_db"
TABLE_NAME = "test_table"
ENGINE = "MergeTree"
BACKUP_NAME = "TestBackup"
BACKUP_META = BackupMetadata(
    name=BACKUP_NAME,
    path=f"ch_backup/{BACKUP_NAME}",
    version="1.0.100",
    ch_version="19.1.16",
    time_format="%Y-%m-%dT%H:%M:%S%Z",
    hostname="clickhouse01.test_net_711",
)
DB = Database(
    DB_NAME, ENGINE, f"/var/lib/clickhouse/metadata/{DB_NAME}.sql", None, None
)


@parametrize(
    {
        "id": "One part before interval",
        "args": {
            "times": [0, 1],
            "part_names": ["1"],
            "interval": 2,
            "expected_upload_metadata": 0,
        },
    },
    {
        "id": "One part after interval",
        "args": {
            "times": [0, 2],
            "part_names": ["1"],
            "interval": 1,
            "expected_upload_metadata": 1,
        },
    },
    {
        "id": "One before. One after",
        "args": {
            "times": [0, 1, 10],
            "part_names": ["1", "2"],
            "interval": 5,
            "expected_upload_metadata": 1,
        },
    },
    {
        "id": "Two parts after interval",
        "args": {
            "times": [0, 1, 10],
            "part_names": ["1", "2"],
            "interval": 1,
            "expected_upload_metadata": 2,
        },
    },
    {
        "id": "Mix",
        "args": {
            "times": [0, 1, 2, 10, 20],
            "part_names": ["1", "2", "3", "4"],
            "interval": 5,
            "expected_upload_metadata": 2,
        },
    },
)
def test_observer(
    times: List[int],
    part_names: List[str],
    interval: int,
    expected_upload_metadata: int,
) -> None:
    config = {"backup": {"update_metadata_interval": interval}}

    backup_meta = copy.deepcopy(BACKUP_META)
    backup_meta.add_database(DB)

    context = BackupContext(config)  # type: ignore[arg-type]
    context.backup_meta = backup_meta

    # Add table metadata to backup metadata
    context.backup_meta.add_table(TableMetadata(DB_NAME, TABLE_NAME, ENGINE, UUID))

    context.backup_layout = Mock()

    with patch("time.time", side_effect=times):
        observer = UploadPartObserver(context)

        for name in part_names:
            part = PartMetadata(
                DB_NAME,
                TABLE_NAME,
                name,
                "AABBCCDD",
                1000,
                ["column1.idx"],
                True,
                None,
                None,
            )
            observer(part)

    assert (
        context.backup_layout.upload_backup_metadata.call_count
        == expected_upload_metadata
    )
    assert len(context.backup_meta.get_parts()) == len(part_names)
