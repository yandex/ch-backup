from unittest import mock

import pytest

from ch_backup.clickhouse.control import (
    ClickhouseCTL,
    _format_string_array,
    _parse_version,
)
from ch_backup.clickhouse.models import Table
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


class TestCreateTable:
    """
    Tests for the settings sent along with a create statement on restore.
    """

    _PLAIN_STATEMENT = (
        "CREATE TABLE db1.table1 (id UInt64) ENGINE = MergeTree ORDER BY id"
    )

    @staticmethod
    def _make_ctl() -> tuple[ClickhouseCTL, mock.Mock]:
        """Helper: build a control object with ClickHouse mocked."""
        config = {
            "data_path": "/tmp",
            "timeout": 10,
            "freeze_timeout": 10,
            "unfreeze_timeout": 10,
            "restore_replica_timeout": 10,
            "drop_replica_timeout": 10,
        }
        with (
            mock.patch("ch_backup.clickhouse.control.ClickhouseClient") as client_cls,
            mock.patch.object(ClickhouseCTL, "get_disks", return_value={}),
        ):
            client = client_cls.return_value
            client.query.return_value = "26.5.1.1"
            client.settings = {}
            ch_ctl = ClickhouseCTL(config, {}, {})

        client.query.reset_mock()
        return ch_ctl, client

    @staticmethod
    def _table(create_statement: str) -> Table:
        return Table("db1", "table1", "MergeTree", [], [], "", create_statement, None)

    def test_unique_key_table_gets_the_experimental_setting(self) -> None:
        """
        ClickHouse gates the clause on CREATE, which is how tables of a replicated
        database are restored.
        """
        ch_ctl, client = self._make_ctl()
        statement = f"{self._PLAIN_STATEMENT} UNIQUE KEY id"

        ch_ctl.create_table(self._table(statement))

        client.query.assert_called_once_with(
            statement, settings={"allow_experimental_unique_key": 1}
        )

    def test_plain_table_is_created_without_the_setting(self) -> None:
        """
        Builds without the clause have no such setting, so it is not sent for
        tables that do not need it.
        """
        ch_ctl, client = self._make_ctl()

        ch_ctl.create_table(self._table(self._PLAIN_STATEMENT))

        client.query.assert_called_once_with(self._PLAIN_STATEMENT, settings=None)
