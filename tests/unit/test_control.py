import pytest

from ch_backup.clickhouse.control import (
    _fix_create_statement,
    _format_string_array,
    _parse_version,
)
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


def test_fix_create_statement_top_keyword():
    broken = (
        "ATTACH VIEW test_4 UUID '1c465905-164d-400b-8a39-ff382c92fb81' "
        "(c String) AS WITH A AS (SELECT TOP AS c FROM default.test_3) "
        "SELECT c FROM A"
    )
    fixed = _fix_create_statement(broken)

    assert "`TOP`" in fixed
    assert "SELECT `TOP` AS c" in fixed


def test_fix_create_statement_no_change_when_valid():
    valid = (
        "ATTACH VIEW test_4 UUID '1c465905-164d-400b-8a39-ff382c92fb81' "
        "(c String) AS SELECT c FROM default.test_3"
    )
    fixed = _fix_create_statement(valid)

    assert fixed == valid
