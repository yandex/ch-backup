"""
Unit tests for backup deduplication logic.
"""

from typing import Any, Dict
from unittest.mock import MagicMock

from ch_backup.backup.deduplication import deduplicate_parts
from ch_backup.backup.metadata.table_metadata import PartInfo, split_part_name
from tests.unit.utils import assert_equal, parametrize


def _make_context(check_data_part_result: bool = True) -> MagicMock:
    """Helper: build a mocked BackupContext."""
    context = MagicMock()
    context.backup_layout.check_data_part.return_value = check_data_part_result
    return context


def _make_existing_part(
    name: str, current_name: str, checksum: str = "abc123", verified: bool = True
) -> Dict[str, Any]:
    """Helper: build a row mimicking the SQL response from get_deduplication_info."""
    return {
        "name": name,
        "current_name": current_name,
        "checksum": checksum,
        "size": 1024,
        "backup_name": "backup1",
        "files": ["data.bin", "count.txt"],
        "tarball": False,
        "disk_name": "default",
        "encrypted": False,
        "verified": verified,
    }


# ---------------------------------------------------------------------------
# Tests for split_part_name
# ---------------------------------------------------------------------------


@parametrize(
    {
        "id": "Non-mutated part",
        "args": {
            "part_name": "all_1_1_0",
            "expected": PartInfo(
                partition_id="all",
                min_block_num=1,
                max_block_num=1,
                level=0,
                mutation=0,
            ),
        },
    },
    {
        "id": "Mutated part",
        "args": {
            "part_name": "all_1_1_0_2",
            "expected": PartInfo(
                partition_id="all",
                min_block_num=1,
                max_block_num=1,
                level=0,
                mutation=2,
            ),
        },
    },
    {
        "id": "Partitioned part",
        "args": {
            "part_name": "20230601_5_10_2",
            "expected": PartInfo(
                partition_id="20230601",
                min_block_num=5,
                max_block_num=10,
                level=2,
                mutation=0,
            ),
        },
    },
    {
        "id": "Partitioned mutated part",
        "args": {
            "part_name": "20230601_5_10_2_7",
            "expected": PartInfo(
                partition_id="20230601",
                min_block_num=5,
                max_block_num=10,
                level=2,
                mutation=7,
            ),
        },
    },
)
def test_split_part_name(part_name, expected):
    actual = split_part_name(part_name)
    assert_equal(actual, expected)


# ---------------------------------------------------------------------------
# Tests for deduplicate_parts
# ---------------------------------------------------------------------------


@parametrize(
    {
        "id": "Identical part name is deduplicated",
        "args": {
            "stored_name": "all_1_1_0",
            "current_name": "all_1_1_0",
            "should_deduplicate": True,
            "expected_link_name": None,
        },
    },
    {
        "id": "Mutated part is deduplicated with link_name set",
        "args": {
            "stored_name": "all_1_1_0",
            "current_name": "all_1_1_0_2",
            "should_deduplicate": True,
            "expected_link_name": "all_1_1_0",
        },
    },
    {
        "id": "Bumped mutation version is deduplicated",
        "args": {
            "stored_name": "all_1_1_0_1",
            "current_name": "all_1_1_0_2",
            "should_deduplicate": True,
            "expected_link_name": "all_1_1_0_1",
        },
    },
    {
        "id": "Different block numbers are NOT deduplicated",
        "args": {
            "stored_name": "all_1_1_0",
            "current_name": "all_2_2_0",
            "should_deduplicate": False,
            "expected_link_name": None,
        },
    },
    {
        "id": "Different partitions are NOT deduplicated",
        "args": {
            "stored_name": "20230601_1_1_0",
            "current_name": "20230602_1_1_0",
            "should_deduplicate": False,
            "expected_link_name": None,
        },
    },
    {
        "id": "Different merge levels are NOT deduplicated",
        "args": {
            "stored_name": "all_1_1_0",
            "current_name": "all_1_1_1",
            "should_deduplicate": False,
            "expected_link_name": None,
        },
    },
)
def test_deduplicate_parts(
    stored_name, current_name, should_deduplicate, expected_link_name
):
    context = _make_context()
    context.ch_ctl.get_deduplication_info.return_value = [
        _make_existing_part(name=stored_name, current_name=current_name)
    ]
    frozen_parts = {current_name: MagicMock(name=current_name, checksum="abc123")}

    result = deduplicate_parts(context, "default", "test", frozen_parts)  # type: ignore[arg-type]

    if should_deduplicate:
        assert current_name in result, f"Expected '{current_name}' to be deduplicated"
        assert_equal(result[current_name].name, current_name)
        assert_equal(result[current_name].link_part_name, expected_link_name)
    else:
        assert (
            current_name not in result
        ), f"Expected '{current_name}' NOT to be deduplicated"


def test_invalid_part_skips_deduplication():
    """If the data part is invalid in remote storage, deduplication must be skipped."""
    context = _make_context(check_data_part_result=False)
    context.ch_ctl.get_deduplication_info.return_value = [
        _make_existing_part(
            name="all_1_1_0",
            current_name="all_1_1_0_2",
            verified=False,  # Force the storage check
        )
    ]
    frozen_parts = {"all_1_1_0_2": MagicMock(name="all_1_1_0_2", checksum="abc123")}

    result = deduplicate_parts(context, "default", "test", frozen_parts)  # type: ignore[arg-type]

    assert "all_1_1_0_2" not in result
