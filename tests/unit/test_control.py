import errno
from types import SimpleNamespace
from unittest.mock import call, mock_open, patch

import pytest

from ch_backup.clickhouse import control
from ch_backup.clickhouse.control import (
    ClickhouseCTL,
    _format_string_array,
    _get_part_checksum,
    _parse_version,
)
from ch_backup.clickhouse.models import Disk, Table
from ch_backup.exceptions import ClickhouseBackupError
from tests.unit.utils import parametrize


@parametrize(
    {
        "id": "empty list",
        "args": {
            "value": [],
            "result": "[]",
        },
    },
    {
        "id": "single-item list",
        "args": {
            "value": ["value"],
            "result": "['value']",
        },
    },
    {
        "id": "multi-item list",
        "args": {
            "value": ["value1", "value2"],
            "result": "['value1','value2']",
        },
    },
    {
        "id": "escaping",
        "args": {
            "value": ["`for`.bar"],
            "result": r"['\`for\`.bar']",
        },
    },
)
def test_format_string_array(value, result):
    assert _format_string_array(value) == result


@pytest.mark.parametrize(
    "version,expected",
    [
        ("25.10", [25, 10]),
        ("25.10.2.65", [25, 10, 2, 65]),
        ("25.10.2.65.dev", [25, 10, 2, 65]),
        ("25.10.2.65-dev.1", [25, 10, 2, 65]),
    ],
)
def test_parse_version(version: str, expected: list[int]) -> None:
    assert _parse_version(version) == expected


@pytest.mark.parametrize("failure_stage", ["open", "read"])
@pytest.mark.parametrize(
    "error_number,failures,attempts",
    [
        (errno.EIO, 0, 1),
        (errno.EIO, 1, 2),
        (errno.EIO, 2, 3),
        (errno.EIO, 3, 3),
        (errno.ENOENT, 1, 1),
        (errno.EACCES, 1, 1),
    ],
)
def test_get_part_checksum_errors(failure_stage, error_number, failures, attempts):
    part_path = "/shadow/backup/data/db/table/all_1_1_0"
    checksum_path = part_path + "/checksums.txt"
    error = OSError(error_number, "checksum read failed", checksum_path)
    open_mock = mock_open(read_data=b"abc")
    if failure_stage == "open":
        open_mock.side_effect = [error] * failures + [open_mock.return_value]
    else:
        open_mock.return_value.read.side_effect = [error] * failures + [b"abc"]

    with patch.object(control, "open", open_mock, create=True):
        if failures and (error_number != errno.EIO or failures >= 3):
            with pytest.raises(OSError) as caught:
                _get_part_checksum(part_path)
            assert caught.value is error
        else:
            assert _get_part_checksum(part_path) == "900150983cd24fb0d6963f7d28e17f72"

    assert open_mock.call_args_list == [call(checksum_path, "rb")] * attempts
    if failure_stage == "read":
        assert open_mock.return_value.read.call_count == attempts
        assert open_mock.return_value.__exit__.call_count == attempts


@pytest.mark.parametrize("error_number", [errno.EIO, errno.ENOENT, errno.EACCES])
def test_scan_frozen_parts_propagates_checksum_error(error_number):
    shadow_path = "/disk/shadow/backup/data/db/table"
    part_path = shadow_path + "/all_1_1_0"
    checksum_path = part_path + "/checksums.txt"
    # Read errors can lack a filename; the scan must supply it.
    error = OSError(error_number, "checksum read failed")
    entry = SimpleNamespace(name="all_1_1_0", path=part_path)
    open_mock = mock_open()
    open_mock.return_value.read.side_effect = error

    with (
        patch.object(control.os.path, "exists", return_value=True),
        patch.object(control.os, "scandir", return_value=[entry]),
        patch.object(control, "open", open_mock, create=True),
        pytest.raises(ClickhouseBackupError) as caught,
    ):
        list(
            ClickhouseCTL.scan_frozen_parts(
                Table.make_dummy("db", "table"),
                Disk("default", "/disk", "Local"),
                "/disk/data/db/table",
                "backup",
            )
        )

    assert checksum_path in str(caught.value)
    assert "db.table" in str(caught.value)
    assert caught.value.__cause__ is error
