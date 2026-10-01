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
            "refreshable_views_stop_timeout": 10,
            "drop_replica_timeout": 10,
        }
        with (
            mock.patch("ch_backup.clickhouse.control.ClickhouseClient") as client_cls,
            mock.patch.object(ClickhouseCTL, "get_disks", return_value={}),
        ):
            client = client_cls.return_value
            client.query.return_value = "26.5.1.1"
            ch_ctl = ClickhouseCTL(config, {}, {})

        client.query.reset_mock()
        return ch_ctl, client

    @staticmethod
    def _table(create_statement: str) -> Table:
        return Table("db1", "table1", "MergeTree", [], [], "", create_statement, None)

    @pytest.mark.parametrize(
        "clause,settings",
        [
            (" UNIQUE KEY id", {"allow_experimental_unique_key": 1}),
            ("", None),
        ],
        ids=["unique key table", "plain table"],
    )
    def test_experimental_setting_is_sent_only_for_a_unique_key_table(
        self, clause: str, settings: dict | None
    ) -> None:
        """
        ClickHouse gates the clause on CREATE, which is how tables of a replicated
        database are restored. Other tables must not get the setting, as builds
        without the clause do not have it.
        """
        ch_ctl, client = self._make_ctl()
        statement = f"{self._PLAIN_STATEMENT}{clause}"

        ch_ctl.create_table(self._table(statement))

        client.query.assert_called_once_with(statement, settings=settings)


def _make_clickhouse_ctl(version: str = "23.12.1") -> ClickhouseCTL:
    ctl = ClickhouseCTL.__new__(ClickhouseCTL)
    ctl._ch_version = version  # pylint: disable=protected-access
    ctl._timeout = 42  # pylint: disable=protected-access
    ctl._refreshable_views_stop_timeout = 42  # pylint: disable=protected-access
    ctl._ch_client = mock.Mock()  # pylint: disable=protected-access
    ctl._ch_client.settings = {}  # pylint: disable=protected-access
    return ctl


def test_refreshable_views_are_resumed_after_restore() -> None:
    ctl = _make_clickhouse_ctl()
    ctl._ch_client.query.return_value = {"data": []}  # pylint: disable=protected-access

    with ctl.stop_refreshable_materialized_views_for_restore(["db"]):
        assert (  # pylint: disable=protected-access
            ctl._ch_client.settings["stop_refreshable_materialized_views_on_startup"]
            == 1
        )

    query_mock = ctl._ch_client.query  # pylint: disable=protected-access
    assert query_mock.call_count == 3
    stop_query = query_mock.call_args_list[0].args[0]
    wait_query = query_mock.call_args_list[1].args[0]
    start_query = query_mock.call_args_list[2].args[0]
    assert stop_query == "SYSTEM STOP VIEWS"
    assert "FROM system.view_refreshes" in wait_query
    assert "status = 'Running'" in wait_query
    assert start_query == "SYSTEM START VIEWS"
    assert (  # pylint: disable=protected-access
        "stop_refreshable_materialized_views_on_startup" not in ctl._ch_client.settings
    )


def test_restore_failure_is_preserved_when_starting_views_fails() -> None:
    ctl = _make_clickhouse_ctl()
    ctl._ch_client.query.side_effect = [  # pylint: disable=protected-access
        {"data": []},
        {"data": []},
        RuntimeError("start views failed"),
    ]

    with mock.patch("ch_backup.clickhouse.control.logging.exception") as log:
        with pytest.raises(RuntimeError, match="restore failed"):
            with ctl.stop_refreshable_materialized_views_for_restore(["db"]):
                raise RuntimeError("restore failed")

    query_mock = ctl._ch_client.query  # pylint: disable=protected-access
    assert query_mock.call_args_list[0].args[0] == "SYSTEM STOP VIEWS"
    assert query_mock.call_args_list[-1].args[0] == "SYSTEM START VIEWS"
    log.assert_called_once()


def test_stop_refreshable_views_on_startup_is_scoped_to_restore() -> None:
    ctl = _make_clickhouse_ctl()
    ctl._ch_client.settings[  # pylint: disable=protected-access
        "stop_refreshable_materialized_views_on_startup"
    ] = 0

    with (
        ctl._stop_refreshable_materialized_views_on_startup()
    ):  # pylint: disable=protected-access
        assert (  # pylint: disable=protected-access
            ctl._ch_client.settings["stop_refreshable_materialized_views_on_startup"]
            == 1
        )

    assert (  # pylint: disable=protected-access
        ctl._ch_client.settings["stop_refreshable_materialized_views_on_startup"] == 0
    )
