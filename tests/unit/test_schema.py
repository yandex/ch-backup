"""
Unit tests schema module.
"""
from ch_backup.clickhouse.models import Table
from ch_backup.clickhouse.schema import is_merge_tree, rewrite_table_schema
from tests.unit.utils import parametrize


@parametrize(
    {
        'id': 'MergeTree',
        'args': {
            'engine': 'MergeTree',
            'result': True,
        },
    },
    {
        'id': 'SummingMergeTree',
        'args': {
            'engine': 'SummingMergeTree',
            'result': True,
        },
    },
    {
        'id': 'ReplicatedMergeTree',
        'args': {
            'engine': 'ReplicatedMergeTree',
            'result': True,
        },
    },
    {
        'id': 'Log',
        'args': {
            'engine': 'Log',
            'result': False,
        },
    },
)
def test_is_merge_tree(engine, result):
    assert is_merge_tree(engine) == result


@parametrize(
    {
        'id': 'MergeTree',
        'args': {
            'table_schema': 'CREATE TABLE test_db.test_table (partition_id Int32, n Int32)'
                            ' ENGINE = MergeTree PARTITION BY partition_id ORDER BY (partition_id, n)',
            'table_engine': 'MergeTree',
            'force_non_replicated_engine': True,
            'result_table_schema': 'CREATE TABLE test_db.test_table (partition_id Int32, n Int32)'
                                   ' ENGINE = MergeTree PARTITION BY partition_id ORDER BY (partition_id, n)',
            'result_table_engine': 'MergeTree',
        },
    },
    {
        'id': 'ReplicatedMergeTree, force_non_replicated_engine=True',
        'args': {
            'table_schema': "CREATE TABLE test_db.test_table (partition_id Int32, n Int32)"
                            " ENGINE = ReplicatedMergeTree('/tables/test_db/test_table', 'replica1')"
                            " PARTITION BY partition_id ORDER BY (partition_id, n)",
            'table_engine': 'ReplicatedMergeTree',
            'force_non_replicated_engine': True,
            'result_table_schema': 'CREATE TABLE test_db.test_table (partition_id Int32, n Int32)'
                                   ' ENGINE = MergeTree PARTITION BY partition_id ORDER BY (partition_id, n)',
            'result_table_engine': 'MergeTree',
        },
    },
    {
        'id': 'ReplicatedMergeTree, force_non_replicated_engine=False',
        'args': {
            'table_schema': "CREATE TABLE test_db.test_table (partition_id Int32, n Int32)"
                            " ENGINE = ReplicatedMergeTree('/tables/test_db/test_table', 'replica1')"
                            " PARTITION BY partition_id ORDER BY (partition_id, n)",
            'table_engine': 'ReplicatedMergeTree',
            'force_non_replicated_engine': False,
            'result_table_schema': "CREATE TABLE test_db.test_table (partition_id Int32, n Int32)"
                                   " ENGINE = ReplicatedMergeTree('/tables/test_db/test_table', '{replica}')"
                                   " PARTITION BY partition_id ORDER BY (partition_id, n)",
            'result_table_engine': 'ReplicatedMergeTree',
        },
    },
)
def test_rewrite_table_schema(table_schema, table_engine, force_non_replicated_engine, result_table_schema,
                              result_table_engine):
    table = Table(database='test_db',
                  name='test_table',
                  create_statement=table_schema,
                  engine=table_engine,
                  disks=[],
                  data_paths=[],
                  uuid=None)
    rewrite_table_schema(table,
                         force_non_replicated_engine=force_non_replicated_engine,
                         override_replica_name='{replica}')
    assert table.create_statement == result_table_schema
    assert table.engine == result_table_engine
