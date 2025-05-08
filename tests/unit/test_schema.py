"""
Unit tests schema module.
"""

from ch_backup.clickhouse.models import Table
from ch_backup.clickhouse.schema import rewrite_table_schema
from tests.unit.utils import parametrize

UUID = "223b4576-76f0-4ed3-976f-46db82af82a9"
INNER_UUID = "fa8ff291-1922-4b7f-afa7-06633d5e16ae"


@parametrize(
    {
        "id": "MergeTree",
        "args": {
            "engine": "MergeTree",
            "result": True,
        },
    },
    {
        "id": "SummingMergeTree",
        "args": {
            "engine": "SummingMergeTree",
            "result": True,
        },
    },
    {
        "id": "ReplicatedMergeTree",
        "args": {
            "engine": "ReplicatedMergeTree",
            "result": True,
        },
    },
    {
        "id": "Log",
        "args": {
            "engine": "Log",
            "result": False,
        },
    },
)
def test_is_merge_tree(engine, result):
    table = Table("test", "test", engine, [], [], "", "", None)
    assert table.is_merge_tree() == result


@parametrize(
    {
        "id": "MergeTree",
        "args": {
            "engine": "MergeTree",
            "result": False,
        },
    },
    {
        "id": "View",
        "args": {
            "engine": "View",
            "result": True,
        },
    },
    {
        "id": "MaterializedView",
        "args": {
            "engine": "MaterializedView",
            "result": True,
        },
    },
    {
        "id": "LiveView",
        "args": {
            "engine": "LiveView",
            "result": True,
        },
    },
)
def test_is_view(engine, result):
    table = Table("test", "test", engine, [], [], "", "", None)
    assert table.is_view() == result


@parametrize(
    {
        "id": "MergeTree table",
        "args": {
            "table_schema": "CREATE TABLE test_db.test_table (partition_id Int32, n Int32)"
            " ENGINE = MergeTree PARTITION BY partition_id ORDER BY (partition_id, n)",
            "table_engine": "MergeTree",
            "force_non_replicated_engine": True,
            "add_uuid": False,
            "result_table_schema": "CREATE TABLE test_db.test_table (partition_id Int32, n Int32)"
            " ENGINE = MergeTree PARTITION BY partition_id ORDER BY (partition_id, n)",
            "result_table_engine": "MergeTree",
        },
    },
    {
        "id": "MergeTree table, add_uuid=True",
        "args": {
            "table_schema": "CREATE TABLE test_db.test_table_engine (s String, n Int32)"
            " ENGINE = MergeTree ORDER BY n",
            "table_engine": "MergeTree",
            "force_non_replicated_engine": True,
            "add_uuid": True,
            "result_table_schema": f"CREATE TABLE test_db.test_table_engine UUID '{UUID}' (s String, n Int32)"
            f" ENGINE = MergeTree ORDER BY n",
            "result_table_engine": "MergeTree",
        },
    },
    {
        "id": "ReplicatedMergeTree table, force_non_replicated_engine=True",
        "args": {
            "table_schema": "CREATE TABLE test_db.test_table (partition_id Int32, n Int32)"
            " ENGINE = ReplicatedMergeTree('/tables/test_db/test_table', 'replica1')"
            " PARTITION BY partition_id ORDER BY (partition_id, n)",
            "table_engine": "ReplicatedMergeTree",
            "force_non_replicated_engine": True,
            "add_uuid": False,
            "result_table_schema": "CREATE TABLE test_db.test_table (partition_id Int32, n Int32)"
            " ENGINE = MergeTree PARTITION BY partition_id ORDER BY (partition_id, n)",
            "result_table_engine": "MergeTree",
        },
    },
    {
        "id": "ReplicatedMergeTree table, force_non_replicated_engine=False",
        "args": {
            "table_schema": "CREATE TABLE test_db.test_table (partition_id Int32, n Int32)"
            " ENGINE = ReplicatedMergeTree('/tables/test_db/test_table', 'replica1')"
            " PARTITION BY partition_id ORDER BY (partition_id, n)",
            "table_engine": "ReplicatedMergeTree",
            "force_non_replicated_engine": False,
            "add_uuid": False,
            "result_table_schema": "CREATE TABLE test_db.test_table (partition_id Int32, n Int32)"
            " ENGINE = ReplicatedMergeTree('/tables/test_db/test_table', '{replica}')"
            " PARTITION BY partition_id ORDER BY (partition_id, n)",
            "result_table_engine": "ReplicatedMergeTree",
        },
    },
    {
        "id": "Distributed table",
        "args": {
            "table_schema": "CREATE TABLE test_db.test_table (EventDate DateTime, CounterID UInt32)"
            " ENGINE = Distributed('cluster', 'db', 'local_table')",
            "table_engine": "Distributed",
            "force_non_replicated_engine": True,
            "add_uuid": False,
            "result_table_schema": "CREATE TABLE test_db.test_table (EventDate DateTime, CounterID UInt32)"
            " ENGINE = Distributed('cluster', 'db', 'local_table')",
            "result_table_engine": "Distributed",
        },
    },
    {
        "id": "Distributed table, add_uuid=True",
        "args": {
            "table_schema": "CREATE TABLE test_db.test_table (EventDate DateTime, CounterID UInt32)"
            " ENGINE = Distributed('cluster', 'db', 'local_table')",
            "table_engine": "Distributed",
            "force_non_replicated_engine": True,
            "add_uuid": True,
            "result_table_schema": f"CREATE TABLE test_db.test_table UUID '{UUID}' (EventDate DateTime, CounterID UInt32)"
            " ENGINE = Distributed('cluster', 'db', 'local_table')",
            "result_table_engine": "Distributed",
        },
    },
    {
        "id": "Distributed table without columns, add_uuid=True",
        "args": {
            "table_schema": "CREATE TABLE test_db.test_table"
            " ENGINE = Distributed('cluster', 'db', 'local_table')",
            "table_engine": "Distributed",
            "force_non_replicated_engine": True,
            "add_uuid": True,
            "result_table_schema": f"CREATE TABLE test_db.test_table UUID '{UUID}'"
            " ENGINE = Distributed('cluster', 'db', 'local_table')",
            "result_table_engine": "Distributed",
        },
    },
    {
        "id": "Dictionary",
        "args": {
            "table_schema": "CREATE DICTIONARY test_db.test_dictionary (n1 UInt32, n2 UInt32)",
            "table_engine": "Dictionary",
            "force_non_replicated_engine": True,
            "add_uuid": False,
            "result_table_schema": "CREATE DICTIONARY test_db.test_dictionary (n1 UInt32, n2 UInt32)",
            "result_table_engine": "Dictionary",
        },
    },
    {
        "id": "Dictionary, add_uuid=True",
        "args": {
            "table_schema": "CREATE DICTIONARY test_db.test_dictionary (n1 UInt32, n2 UInt32)",
            "table_engine": "Dictionary",
            "force_non_replicated_engine": True,
            "add_uuid": True,
            "result_table_schema": f"CREATE DICTIONARY test_db.test_dictionary UUID '{UUID}' (n1 UInt32, n2 UInt32)",
            "result_table_engine": "Dictionary",
        },
    },
    {
        "id": "MergeTree table, add name",
        "args": {
            "table_schema": "CREATE TABLE _ (partition_id Int32, n Int32)"
            " ENGINE = MergeTree PARTITION BY partition_id ORDER BY (partition_id, n)",
            "table_engine": "MergeTree",
            "force_non_replicated_engine": True,
            "add_uuid": False,
            "result_table_schema": "CREATE TABLE `test_db`.`test_table` (partition_id Int32, n Int32)"
            " ENGINE = MergeTree PARTITION BY partition_id ORDER BY (partition_id, n)",
            "result_table_engine": "MergeTree",
        },
    },
    {
        "id": "MergeTree table, add database",
        "args": {
            "table_schema": "CREATE TABLE test_table (partition_id Int32, n Int32)"
            " ENGINE = MergeTree PARTITION BY partition_id ORDER BY (partition_id, n)",
            "table_engine": "MergeTree",
            "force_non_replicated_engine": True,
            "add_uuid": False,
            "result_table_schema": "CREATE TABLE `test_db`.`test_table` (partition_id Int32, n Int32)"
            " ENGINE = MergeTree PARTITION BY partition_id ORDER BY (partition_id, n)",
            "result_table_engine": "MergeTree",
        },
    },
    {
        "id": "view, add name",
        "args": {
            "table_schema": "CREATE VIEW _ AS SELECT n FROM test_db.table_01",
            "table_engine": "MergeTree",
            "force_non_replicated_engine": True,
            "add_uuid": False,
            "result_table_schema": "CREATE VIEW `test_db`.`test_table` AS SELECT n FROM test_db.table_01",
            "result_table_engine": "MergeTree",
        },
    },
    {
        "id": "LIVE view, add name",
        "args": {
            "table_schema": "CREATE LIVE VIEW _ AS SELECT n FROM test_db.table_01",
            "table_engine": "MergeTree",
            "force_non_replicated_engine": True,
            "add_uuid": False,
            "result_table_schema": "CREATE LIVE VIEW `test_db`.`test_table` AS SELECT n FROM test_db.table_01",
            "result_table_engine": "MergeTree",
        },
    },
    {
        "id": "add UUID",
        "args": {
            "table_schema": "CREATE TABLE `test_db`.`test_table` (partition_id Int32, n Int32)"
            " ENGINE = MergeTree PARTITION BY partition_id ORDER BY (partition_id, n)",
            "table_engine": "MergeTree",
            "force_non_replicated_engine": True,
            "add_uuid": True,
            "result_table_schema": f"CREATE TABLE `test_db`.`test_table` UUID '{UUID}' (partition_id Int32, n Int32)"
            " ENGINE = MergeTree PARTITION BY partition_id ORDER BY (partition_id, n)",
            "result_table_engine": "MergeTree",
        },
    },
    {
        "id": "add_uuid=True do not add UUID if schema already contains it",
        "args": {
            "table_schema": "CREATE TABLE _ (partition_id Int32, n Int32)"
            f" UUID '{INNER_UUID}'"
            " ENGINE = MergeTree PARTITION BY partition_id ORDER BY (partition_id, n)",
            "table_engine": "MergeTree",
            "force_non_replicated_engine": True,
            "add_uuid": True,
            "result_table_schema": "CREATE TABLE `test_db`.`test_table` (partition_id Int32, n Int32)"
            f" UUID '{INNER_UUID}'"
            " ENGINE = MergeTree PARTITION BY partition_id ORDER BY (partition_id, n)",
            "result_table_engine": "MergeTree",
        },
    },
    {
        "id": "attach statement add UUID",
        "args": {
            "table_schema": "ATTACH TABLE `test_db`.`test_table` (partition_id Int32, n Int32)"
            " ENGINE = MergeTree PARTITION BY partition_id ORDER BY (partition_id, n)",
            "table_engine": "MergeTree",
            "force_non_replicated_engine": True,
            "add_uuid": True,
            "result_table_schema": f"ATTACH TABLE `test_db`.`test_table` UUID '{UUID}' (partition_id Int32, n Int32)"
            " ENGINE = MergeTree PARTITION BY partition_id ORDER BY (partition_id, n)",
            "result_table_engine": "MergeTree",
        },
    },
    {
        "id": "attach table add_uuid=True do not add UUID if schema already contains it",
        "args": {
            "table_schema": "ATTACH TABLE _ (partition_id Int32, n Int32)"
            f" UUID '{INNER_UUID}'"
            " ENGINE = MergeTree PARTITION BY partition_id ORDER BY (partition_id, n)",
            "table_engine": "MergeTree",
            "force_non_replicated_engine": True,
            "add_uuid": True,
            "result_table_schema": "ATTACH TABLE `test_db`.`test_table` (partition_id Int32, n Int32)"
            f" UUID '{INNER_UUID}'"
            " ENGINE = MergeTree PARTITION BY partition_id ORDER BY (partition_id, n)",
            "result_table_engine": "MergeTree",
        },
    },
)
# pylint: disable=too-many-positional-arguments
def test_rewrite_table_schema(
    table_schema,
    table_engine,
    force_non_replicated_engine,
    add_uuid,
    result_table_schema,
    result_table_engine,
):
    table = Table(
        database="test_db",
        name="test_table",
        create_statement=table_schema,
        engine=table_engine,
        disks=[],
        data_paths=[],
        metadata_path="",
        uuid=UUID,
    )
    rewrite_table_schema(
        table,
        force_non_replicated_engine=force_non_replicated_engine,
        override_replica_name="{replica}",
        add_uuid=add_uuid,
        inner_uuid=INNER_UUID,
    )
    assert table.create_statement == result_table_schema
    assert table.engine == result_table_engine
