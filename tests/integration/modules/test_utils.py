"""
Unit tests utils module.
"""
from tests.integration.modules.utils import normalize_create_query
from tests.unit.utils import parametrize


@parametrize(
    {
        "id": "MergeTree table",
        "args": {
            "create_query": "CREATE TABLE test_db.test_table (partition_id Int32, n Int32)"
            " ENGINE = MergeTree PARTITION BY partition_id ORDER BY (partition_id, n)",
            "result": "CREATE TABLE test_db.test_table (partition_id Int32, n Int32)"
            " ENGINE = MergeTree PARTITION BY partition_id ORDER BY (partition_id, n)",
        },
    },
    {
        "id": "ReplicatedMergeTree table",
        "args": {
            "create_query": "CREATE TABLE test_db.test_table (partition_id Int32, n Int32)"
            " ENGINE = ReplicatedMergeTree('/tables/test_db/test_table', 'replica1')"
            " PARTITION BY partition_id ORDER BY (partition_id, n)",
            "result": "CREATE TABLE test_db.test_table (partition_id Int32, n Int32)"
            " ENGINE = ReplicatedMergeTree('/tables/test_db/test_table', '{replica}')"
            " PARTITION BY partition_id ORDER BY (partition_id, n)",
        },
    },
    {
        "id": "Distributed table",
        "args": {
            "create_query": "CREATE TABLE test_db.test_table (EventDate DateTime, CounterID UInt32)"
            " ENGINE = Distributed('cluster', 'db', 'local_table')",
            "result": "CREATE TABLE test_db.test_table (EventDate DateTime, CounterID UInt32)"
            " ENGINE = Distributed('cluster', 'db', 'local_table')",
        },
    },
    {
        "id": "Dictionary",
        "args": {
            "create_query": "CREATE DICTIONARY test_db.test_dictionary (n1 UInt32, n2 UInt32)",
            "result": "CREATE DICTIONARY test_db.test_dictionary (n1 UInt32, n2 UInt32)",
        },
    },
)
def test_normalize_create_query(create_query, result):
    assert result == normalize_create_query(create_query)
