"""
Unit tests ClickhouseBackup.
"""

from ch_backup.ch_backup import _rewrite_with_explicit_uuid
from ch_backup.clickhouse.control import Table
from tests.unit.utils import parametrize


@parametrize(
    {
        'id': 'Distributed table with columns',
        'args': {
            'create':
                """
                CREATE TABLE db.dist_table (EventDate DateTime, CounterID UInt32)
                ENGINE = Distributed('cluster', 'db', 'local_table')
            """,
            'uuid':
                "123",
            'result':
                """
                CREATE TABLE db.dist_table UUID '123' (EventDate DateTime, CounterID UInt32)
                ENGINE = Distributed('cluster', 'db', 'local_table')
            """,
        },
    }, {
        'id': 'Distributed table without columns',
        'args': {
            'create': "CREATE TABLE db.dist_table ENGINE = Distributed('cluster', 'db', 'local_table')",
            'uuid': "123",
            'result': "CREATE TABLE db.dist_table UUID '123' ENGINE = Distributed('cluster', 'db', 'local_table')",
        },
    }, {
        'id': 'Dictionary creation without engine',
        'args': {
            'create': "CREATE DICTIONARY test_db.test_dictionary (n1 UInt32, n2 UInt32)",
            'uuid': "123",
            'result': "CREATE DICTIONARY test_db.test_dictionary UUID '123' (n1 UInt32, n2 UInt32)",
        },
    })
def test_rewrite_with_explicit_uuid(create, uuid, result):
    table: Table = Table(name="", database="", engine="", disks=[], data_paths=[], create_statement=create, uuid=uuid)
    assert _rewrite_with_explicit_uuid(table) == result
