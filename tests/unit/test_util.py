"""
Unit test for util module.
"""
import pytest

from ch_backup.exceptions import ClickhouseBackupError
from ch_backup.util import (get_zookeeper_paths, normalize_schema, retry, strip_query)

from . import ExpectedException, UnexpectedException


class TestStripQuery:
    """
    Tests for strip_query() function.
    """
    def test_query_without_newlines(self) -> None:
        input_query = 'SELECT 42 FROM {db_name}.{table_name}'
        expected = 'SELECT 42 FROM {db_name}.{table_name}'
        assert strip_query(input_query) == expected

    def test_query_with_newlines(self) -> None:
        input_query = '''
            SHOW TABLES
            FROM {db_name}
            FORMAT JSON
        '''
        expected = 'SHOW TABLES FROM {db_name} FORMAT JSON'
        assert strip_query(input_query) == expected


@retry(ExpectedException, max_attempts=3, max_interval=0.1)
def unreliable_function(context: dict) -> None:
    context['attempts'] = context.get('attempts', 0) + 1

    if context.get('failure_count', 0) >= context['attempts']:
        raise context.get('exception', ExpectedException)()


class TestRetry:
    """
    Tests for retry() function.
    """
    def test_function_succeeds(self):
        context: dict = {}
        unreliable_function(context)
        assert context['attempts'] == 1

    def test_function_raises_once(self):
        context = {'failure_count': 1}
        unreliable_function(context)
        assert context['attempts'] == 2

    def test_function_always_raises(self):
        context = {'failure_count': 3}
        with pytest.raises(ExpectedException):
            unreliable_function(context)
        assert context['attempts'] == 3

    def test_function_raises_unexpected_exception(self):
        context = {'failure_count': 3, 'exception': UnexpectedException}
        with pytest.raises(UnexpectedException):
            unreliable_function(context)
        assert context['attempts'] == 1


_test_tables = {
    'valid_test': "CREATE TABLE valid_test (id Int32)"
                  "ENGINE ReplicatedMergeTree('/clickhouse/tables/shard1/valid_test', '{replica}') "
                  "PARTITION BY (id) ORDER BY (id)",
    'valid_legacy': "CREATE TABLE legacy_test (id Date) ENGINE"
                    "ReplicatedMergeTree('/clickhouse/tables/shard1/legacy_test', '{replica}', id, id, id, 8192)",
    'valid_summing_test': "CREATE TABLE valid_summing_test (id Int32)"
                          "ENGINE ReplicatedMergeTree('/clickhouse/tables/shard1/valid_summing_test', '{replica}') "
                          "PARTITION BY (id) ORDER BY (id)",
    'valid_summing_legacy': "CREATE TABLE legacy_summing_test (id Date) ENGINE ReplicatedSummingMergeTree("
                            "'/clickhouse/tables/shard1/legacy_summing_test', '{replica}', id, id, id, 8192)",
    'valid_with_spaces': "CREATE TABLE valid_test (id Int32)"
                         "ENGINE ReplicatedMergeTree('/clickhouse/tables/shard1/valid_test ', '{replica}') "
                         "PARTITION BY (id) ORDER BY (id)",
    'invalid_test': "CREATE TABLE invalid_test (id Int32) ENGINE MergeTree() PARTITION BY (id) ORDER BY (id)",
    # valid path for clickhouse
    'invalid_with_quotes_test': "CREATE TABLE valid_test (id Int32)"
                                "ENGINE ReplicatedMergeTree('/clickhouse/tables/sh\'a\'rd1/valid_test', '{replica}') "
                                "PARTITION BY (id) ORDER BY (id)",
}


def get_table(name):
    return _test_tables[name]


class TestGetZooKeeperPath:
    """
    Tests for get_zookeeper_paths() function.
    """
    def test_valid_sql(self):
        actual = get_zookeeper_paths(value for name, value in _test_tables.items() if name.startswith('valid_'))
        assert actual == [
            '/clickhouse/tables/shard1/valid_test',
            '/clickhouse/tables/shard1/legacy_test',
            '/clickhouse/tables/shard1/valid_summing_test',
            '/clickhouse/tables/shard1/legacy_summing_test',
            '/clickhouse/tables/shard1/valid_test ',
        ]

    def test_invalid_sql(self):
        with pytest.raises(ClickhouseBackupError):
            get_zookeeper_paths(['invalid_test'])


class TestNormalizeSchema:
    """
    Test _normailze_schema method
    """

    schemas = [
        [
            "CREATE TABLE database.test (`date` Date, `value` UInt32) "
            "ENGINE = MergeTree() ORDER BY date",
            "CREATE TABLE database.test (`date` Date, `value` UInt32) "
            "ENGINE = MergeTree() ORDER BY date",
            True,
        ],
        [
            "CREATE TABLE database.test (`date` Date, `value` UInt32) "
            "ENGINE = MergeTree() ORDER BY date",
            "CREATE TABLE database.test (`date` Date, `value` UInt64) "
            "ENGINE = MergeTree() ORDER BY date",
            False,
        ],
        [
            "CREATE TABLE database.test (`date` Date, `value` UInt32) "
            "ENGINE = Distributed('foo-foo', 'database_bar', 'table_biz')",
            "CREATE TABLE database.test (`date` Date, `value` UInt32) "
            "ENGINE = Distributed('foo-foo', 'database_bar', 'table_biz')",
            True,
        ],
        [
            "CREATE TABLE database.test (`date` Date, `value` UInt32) "
            "ENGINE = Distributed('foo-foo', 'database_bar', 'table_biz')",
            "CREATE TABLE database.test (`date` Date, `value` UInt32) "
            "ENGINE = Distributed('foo-foo', database_bar, table_biz)",
            True,
        ],
        [
            "CREATE TABLE database.test (`date` Date, `value` UInt32) "
            "ENGINE = Distributed('foo-foo', 'database_bar', 'table_biz')",
            "CREATE TABLE database.test (`date` Date, `value` UInt32) "
            "ENGINE = Distributed('foo-foo', database_biz, table_biz)",
            False,
        ],
        [
            "CREATE TABLE database.test (`date` Date, `value` UInt32) "
            "ENGINE = Distributed('foo-foo', 'database_bar', 'table_biz')",
            "CREATE TABLE database.test (`date` Date, `value` UInt32) "
            "ENGINE = Distributed('bar-bar', database_bar, table_biz)",
            False,
        ],
        [
            "CREATE TABLE database.test (`date` Date, `value` UInt32) "
            "ENGINE = Distributed('foo-foo', 'database_bar', 'table_biz', shard_key, policy_name)",
            "CREATE TABLE database.test (`date` Date, `value` UInt32) "
            "ENGINE = Distributed('foo-foo', database_bar, table_biz, shard_key, policy_name)",
            True,
        ],
        [
            "CREATE TABLE database.test (`date` Date, `value` UInt32) "
            "ENGINE = Distributed('foo-foo', 'database_bar', 'table_biz', shard_key, policy_name) SETTINGS a=b",
            "CREATE TABLE database.test (`date` Date, `value` UInt32) "
            "ENGINE = Distributed('foo-foo', database_bar, table_biz, shard_key, policy_name) SETTINGS a=b",
            True,
        ],
        [
            "CREATE TABLE database.test (`date` Date, `value` UInt32) "
            "ENGINE = Distributed('foo-foo', 'database_bar', 'table_biz', shard_key, policy_name) SETTINGS a=b",
            "CREATE TABLE database.test (`date` Date, `value` UInt32) "
            "ENGINE = Distributed('foo-foo', database_bar, table_biz, shard_key, policy_name) SETTINGS c=b",
            False,
        ],
    ]

    def test_normalize_schema(self):
        for schema in self.schemas:
            assert (normalize_schema(str(schema[0])) == normalize_schema(str(schema[1]))) == schema[2]
