from ch_backup.clickhouse.control import _format_string_array, _parse_version
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


@parametrize(
    {
        "id": "release version",
        "args": {
            "value": "25.10.2.65",
            "result": [25, 10, 2, 65],
        },
    },
    {
        "id": "development version",
        "args": {
            "value": "25.10.2.65-dev.1",
            "result": [25, 10, 2, 65],
        },
    },
)
def test_parse_version(value, result):
    assert _parse_version(value) == result
