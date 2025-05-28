"""
Unit test for util module.
"""

from pathlib import Path

import pytest

from ch_backup.clickhouse.models import Table
from ch_backup.exceptions import ClickhouseBackupError
from ch_backup.util import (
    compare_schema,
    get_table_zookeeper_paths,
    is_equal_s3_endpoints,
    list_dir_files,
    replace_macros,
    retry,
    scan_dir_files,
    strip_query,
)

from . import ExpectedException, UnexpectedException
from .utils import parametrize


class TestStripQuery:
    """
    Tests for strip_query() function.
    """

    def test_query_without_newlines(self) -> None:
        input_query = "SELECT 42 FROM {db_name}.{table_name}"
        expected = "SELECT 42 FROM {db_name}.{table_name}"
        assert strip_query(input_query) == expected

    def test_query_with_newlines(self) -> None:
        input_query = """
            SHOW TABLES
            FROM {db_name}
            FORMAT JSON
        """
        expected = "SHOW TABLES FROM {db_name} FORMAT JSON"
        assert strip_query(input_query) == expected


@retry(ExpectedException, max_attempts=3, max_interval=0.1)
def unreliable_function(context: dict) -> None:
    context["attempts"] = context.get("attempts", 0) + 1

    if context.get("failure_count", 0) >= context["attempts"]:
        raise context.get("exception", ExpectedException)()


class TestRetry:
    """
    Tests for retry() function.
    """

    def test_function_succeeds(self):
        context: dict = {}
        unreliable_function(context)
        assert context["attempts"] == 1

    def test_function_raises_once(self):
        context = {"failure_count": 1}
        unreliable_function(context)
        assert context["attempts"] == 2

    def test_function_always_raises(self):
        context = {"failure_count": 3}
        with pytest.raises(ExpectedException):
            unreliable_function(context)
        assert context["attempts"] == 3

    def test_function_raises_unexpected_exception(self):
        context = {"failure_count": 3, "exception": UnexpectedException}
        with pytest.raises(UnexpectedException):
            unreliable_function(context)
        assert context["attempts"] == 1


_test_tables = {
    "valid_test": "CREATE TABLE valid_test (id Int32)"
    "ENGINE ReplicatedMergeTree('/clickhouse/tables/shard1/valid_test', '{replica}') "
    "PARTITION BY (id) ORDER BY (id)",
    "valid_legacy": "CREATE TABLE legacy_test (id Date) ENGINE"
    "ReplicatedMergeTree('/clickhouse/tables/shard1/valid_legacy', '{replica}', id, id, id, 8192)",
    "valid_summing_test": "CREATE TABLE valid_summing_test (id Int32)"
    "ENGINE ReplicatedMergeTree('/clickhouse/tables/shard1/valid_summing_test', '{replica}') "
    "PARTITION BY (id) ORDER BY (id)",
    "valid_summing_legacy": "CREATE TABLE legacy_summing_test (id Date) ENGINE ReplicatedSummingMergeTree("
    "'/clickhouse/tables/shard1/valid_summing_legacy', '{replica}', id, id, id, 8192)",
    "valid_with_spaces ": "CREATE TABLE valid_with_spaces (id Int32)"
    "ENGINE ReplicatedMergeTree('/clickhouse/tables/shard1/valid_with_spaces ', '{replica}') "
    "PARTITION BY (id) ORDER BY (id)",
    "invalid_test": "CREATE TABLE invalid_test (id Int32) ENGINE MergeTree() PARTITION BY (id) ORDER BY (id)",
    # valid path for clickhouse
    "invalid_with_quotes_test": "CREATE TABLE valid_test (id Int32)"
    "ENGINE ReplicatedMergeTree('/clickhouse/tables/sh'a'rd1/valid_test', '{replica}') "
    "PARTITION BY (id) ORDER BY (id)",
}


def get_table(name):
    return _test_tables[name]


class TestGetZooKeeperPath:
    """
    Tests for get_zookeeper_paths() function.
    """

    @staticmethod
    def _make_table(name, create_statement):
        return Table("default", name, "", [], [], "", create_statement, "")

    def test_valid_sql(self):
        actual = get_table_zookeeper_paths(
            self._make_table(name, value)
            for name, value in _test_tables.items()
            if name.startswith("valid_")
        )
        assert actual == [
            (
                self._make_table(table, _test_tables[table]),
                f"/clickhouse/tables/shard1/{table}",
            )
            for table in (
                "valid_test",
                "valid_legacy",
                "valid_summing_test",
                "valid_summing_legacy",
                "valid_with_spaces ",
            )
        ]

    def test_invalid_sql(self):
        with pytest.raises(ClickhouseBackupError):
            get_table_zookeeper_paths(
                [self._make_table("invalid_test", "invalid_test")]
            )


@parametrize(
    {
        "id": "identical MergeTree tables",
        "args": {
            "schema_a": "CREATE TABLE database.test (`date` Date, `value` UInt32) "
            "ENGINE = MergeTree() ORDER BY date",
            "schema_b": "CREATE TABLE database.test (`date` Date, `value` UInt32) "
            "ENGINE = MergeTree() ORDER BY date",
            "result": True,
        },
    },
    {
        "id": "unequal MergeTree tables with mismatched column types",
        "args": {
            "schema_a": "CREATE TABLE database.test (`date` Date, `value` UInt32) "
            "ENGINE = MergeTree() ORDER BY date",
            "schema_b": "CREATE TABLE database.test (`date` Date, `value` UInt64) "
            "ENGINE = MergeTree() ORDER BY date",
            "result": False,
        },
    },
    {
        "id": "identical Distributed tables",
        "args": {
            "schema_a": "CREATE TABLE database.test (`date` Date, `value` UInt32) "
            "ENGINE = Distributed('foo-foo', 'database_bar', 'table_biz')",
            "schema_b": "CREATE TABLE database.test (`date` Date, `value` UInt32) "
            "ENGINE = Distributed('foo-foo', 'database_bar', 'table_biz')",
            "result": True,
        },
    },
    {
        "id": "equal Distributed tables with syntax differences in quotas",
        "args": {
            "schema_a": "CREATE TABLE database.test (`date` Date, `value` UInt32) "
            "ENGINE = Distributed('foo-foo', 'database_bar', 'table_biz')",
            "schema_b": "CREATE TABLE database.test (`date` Date, `value` UInt32) "
            "ENGINE = Distributed('foo-foo', database_bar, table_biz)",
            "result": True,
        },
    },
    {
        "id": "equal Distributed tables with syntax differences in quotas - 1",
        "args": {
            "schema_a": "CREATE TABLE database.test (`date` Date, `value` UInt32) "
            "ENGINE = Distributed('foo-foo', 'database_bar', 'table_biz')",
            "schema_b": "CREATE TABLE database.test (`date` Date, `value` UInt32) "
            "ENGINE = Distributed('foo-foo', database_bar, table_biz)",
            "result": True,
        },
    },
    {
        "id": "equal Distributed tables with syntax differences in quotas - 2",
        "args": {
            "schema_a": "CREATE TABLE test_db.table_all (`a` UInt32) "
            "ENGINE = Distributed('{cluster}', 'test_db', 'table')",
            "schema_b": "ATTACH TABLE `test_db`.`table_all` (`a` UInt32) "
            "ENGINE = Distributed('{cluster}', 'test_db', 'table')",
            "result": True,
        },
    },
    {
        "id": "equal Distributed tables with syntax differences in quotas - 3",
        "args": {
            "schema_a": "CREATE TABLE `test_db`.table_all (`a` UInt32) "
            "ENGINE = Distributed('{cluster}', 'test_db', 'table')",
            "schema_b": "ATTACH TABLE `test_db`.`table_all` (`a` UInt32) "
            "ENGINE = Distributed('{cluster}', 'test_db', 'table')",
            "result": True,
        },
    },
    {
        "id": "unequal Distributed tables with mismatched database names",
        "args": {
            "schema_a": "CREATE TABLE database.test (`date` Date, `value` UInt32) "
            "ENGINE = Distributed('foo-foo', 'database_bar', 'table_biz')",
            "schema_b": "CREATE TABLE database.test (`date` Date, `value` UInt32) "
            "ENGINE = Distributed('foo-foo', database_biz, table_biz)",
            "result": False,
        },
    },
    {
        "id": "unequal Distributed tables with mismatched cluster names",
        "args": {
            "schema_a": "CREATE TABLE database.test (`date` Date, `value` UInt32) "
            "ENGINE = Distributed('foo-foo', 'database_bar', 'table_biz')",
            "schema_b": "CREATE TABLE database.test (`date` Date, `value` UInt32) "
            "ENGINE = Distributed('bar-bar', database_bar, table_biz)",
            "result": False,
        },
    },
    {
        "id": "equal Distributed tables with sharding key and syntax differences in quotas",
        "args": {
            "schema_a": "CREATE TABLE database.test (`date` Date, `value` UInt32) "
            "ENGINE = Distributed('foo-foo', 'database_bar', 'table_biz', shard_key, policy_name)",
            "schema_b": "CREATE TABLE database.test (`date` Date, `value` UInt32) "
            "ENGINE = Distributed('foo-foo', database_bar, table_biz, shard_key, policy_name)",
            "result": True,
        },
    },
    {
        "id": "equal Distributed tables with SETTINGS clause and syntax differences in quotas",
        "args": {
            "schema_a": "CREATE TABLE database.test (`date` Date, `value` UInt32) "
            "ENGINE = Distributed('foo-foo', 'database_bar', 'table_biz', shard_key, policy_name)",
            "schema_b": "CREATE TABLE database.test (`date` Date, `value` UInt32) "
            "ENGINE = Distributed('foo-foo', database_bar, table_biz, shard_key, policy_name)",
            "result": True,
        },
    },
    {
        "id": "unequal Distributed tables with mismatched settings",
        "args": {
            "schema_a": "CREATE TABLE database.test (`date` Date, `value` UInt32) "
            "ENGINE = Distributed('foo-foo', 'database_bar', 'table_biz', shard_key, policy_name) SETTINGS a=b",
            "schema_b": "CREATE TABLE database.test (`date` Date, `value` UInt32) "
            "ENGINE = Distributed('foo-foo', database_bar, table_biz, shard_key, policy_name) SETTINGS c=b",
            "result": False,
        },
    },
    {
        "id": "equal Distributed tables with and without UUID clause",
        "args": {
            "schema_a": "CREATE TABLE test_db.table_all (`a` UInt32) "
            "ENGINE = Distributed('{cluster}', 'test_db', 'table')",
            "schema_b": "ATTACH TABLE `test_db`.`table_all` UUID 'f8456d96-67fe-45bf-9126-61f83c4b2682' "
            "(`a` UInt32) ENGINE = Distributed('{cluster}', 'test_db', 'table')",
            "result": True,
        },
    },
    {
        "id": "equal tables with projection and differences in function name case",
        "args": {
            "schema_a": "CREATE TABLE test_db.table_01 (`date` Date, `n` Int32, "
            "PROJECTION test_proj (SELECT n, COUNT(*) AS count GROUP BY n)) "
            "ENGINE = MergeTree PARTITION BY date ORDER BY date SETTINGS index_granularity = 8192",
            "schema_b": "CREATE TABLE test_db.table_01 (`date` Date, `n` Int32, "
            "PROJECTION test_proj (SELECT n, count(*) AS count GROUP BY n)) "
            "ENGINE = MergeTree PARTITION BY date ORDER BY date SETTINGS index_granularity = 8192",
            "result": True,
        },
    },
    {
        "id": "equal tables with Null engine and differences in formatting",
        "args": {
            "schema_a": "CREATE TABLE test_db.test "
            "(`a` UInt32, `b` Date, `c` DateTime, `d` String, `e` UInt8, `f` UInt8) "
            "ENGINE = Null",
            "schema_b": "CREATE TABLE `test_db`.`test` "
            "(`a` UInt32, `b` Date, `c` DateTime, `d` String, `e` UInt8, `f` UInt8) "
            "ENGINE = Null",
            "result": True,
        },
    },
    {
        "id": "equal materialized views with syntax differences in quotas",
        "args": {
            "schema_a": "CREATE MATERIALIZED VIEW test_db.mview TO test_db.base (`number` UInt64) "
            "AS SELECT * FROM system.numbers LIMIT 10",
            "schema_b": "ATTACH MATERIALIZED VIEW `test_db`.`mview` TO test_db.base (`number` UInt64) "
            "AS SELECT * FROM system.numbers LIMIT 10",
            "result": True,
        },
    },
)
def test_compare_schema(schema_a, schema_b, result):
    assert compare_schema(schema_a, schema_b) == result


class TestScanDir:
    """
    Tests for scan_dir_files() function.
    """

    def test_scan_same_as_list(self):
        # tests directory
        dir_path = Path(__file__).parent.parent.resolve()
        expected = list_dir_files(str(dir_path))
        actual = []
        for file_name in scan_dir_files(dir_path):
            actual.append(file_name)

        assert actual == expected


def test_replace_macros():
    assert replace_macros("{a}/{b}", {"a": "1", "b": "2"}) == "1/2"
    assert replace_macros("{a},{c}", {"a": "1", "b": "2"}) == "1,{c}"
    assert replace_macros(" } {a} { ", {"a": "1", "b": "2"}) == " } 1 { "
    assert replace_macros("", {"a": "1", "b": "2"}) == ""


def test_compare_s3_endpoints():
    endpoint_1 = "https://cloud-storage-qwerty1234.s3.yandexcloud.net/cloud_storage/qwerty1234/invalid-shard-1/"
    endpoint_2 = "https://s3.yandexcloud.net/cloud-storage-qwerty1234/cloud_storage/qwerty1234/invalid-shard-1/"
    assert is_equal_s3_endpoints(endpoint_1, endpoint_2)

    endpoint_1 = "https://s3.yandexcloud.net/cloud-storage-qwerty1234/cloud_storage/qwerty1234/invalid-shard-1/"
    endpoint_2 = "https://cloud-storage-qwerty1234.s3.yandexcloud.net/cloud_storage/qwerty1234/invalid-shard-1/"
    assert is_equal_s3_endpoints(endpoint_1, endpoint_2)

    endpoint_1 = "https://cloud-storage-qwerty1234.s3.yandexcloud.net/cloud_storage/qwerty1234/invalid-shard-1/"
    endpoint_2 = "https://cloud-storage-qwerty1234.s3.yandexcloud.net/cloud_storage/qwerty1234/invalid-shard-1/"
    assert is_equal_s3_endpoints(endpoint_1, endpoint_2)

    endpoint_1 = "https://s3.yandexcloud.net/cloud-storage-qwerty1234/cloud_storage/qwerty1234/invalid-shard-1/"
    endpoint_2 = "https://s3.yandexcloud.net/cloud-storage-qwerty1234/cloud_storage/qwerty1234/invalid-shard-1/"
    assert is_equal_s3_endpoints(endpoint_1, endpoint_2)

    endpoint_1 = "https://cloud-storage-1234QWERTYU.s3.yandexcloud.net/cloud_storage/qwerty1234/invalid-shard-1/"
    endpoint_2 = "https://s3.yandexcloud.net/cloud-storage-qwerty1234/cloud_storage/qwerty1234/invalid-shard-1/"
    assert not is_equal_s3_endpoints(endpoint_1, endpoint_2)
