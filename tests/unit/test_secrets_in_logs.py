"""
Unit tests for masking secrets before they reach the log.
"""

from ch_backup import cli
from ch_backup.util import mask_sql_literals
from tests.unit.utils import parametrize


@parametrize(
    {
        "id": "named collection with s3 credentials",
        "args": {
            "sql": (
                "CREATE NAMED COLLECTION IF NOT EXISTS s3_args AS"
                " access_key_id = 'AKIAIOSFODNN7EXAMPLE',"
                " format = 'CSVWithNames',"
                " secret_access_key = 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',"
                " url = 'https://s3.example.com/test-bucket/data/*.xz'"
            ),
            "expected": (
                "CREATE NAMED COLLECTION IF NOT EXISTS s3_args AS"
                " access_key_id = '[HIDDEN]',"
                " format = '[HIDDEN]',"
                " secret_access_key = '[HIDDEN]',"
                " url = '[HIDDEN]'"
            ),
        },
    },
    {
        "id": "named collection with a plain text password",
        "args": {
            "sql": (
                "CREATE NAMED COLLECTION db_creds AS"
                " addresses_expr = 'db-host.example.com:3306',"
                " password = 'hunter2-plain-text-password',"
                " user = 'remote_user'"
            ),
            "expected": (
                "CREATE NAMED COLLECTION db_creds AS"
                " addresses_expr = '[HIDDEN]',"
                " password = '[HIDDEN]',"
                " user = '[HIDDEN]'"
            ),
        },
    },
    {
        "id": "escaped quote does not end the literal early",
        "args": {
            "sql": (
                "structure = 'kind Enum8(\\'AAA\\' = 1, \\'BBB\\' = 2)',"
                " password = 'hunter2'"
            ),
            "expected": "structure = '[HIDDEN]', password = '[HIDDEN]'",
        },
    },
    {
        "id": "table engine arguments",
        "args": {
            "sql": (
                "CREATE TABLE test_db.test_table (n Int32)"
                " ENGINE = MySQL('db-host.example.com:3306', 'remote_db',"
                " 'remote_table', 'remote_user', 'hunter2')"
            ),
            "expected": (
                "CREATE TABLE test_db.test_table (n Int32)"
                " ENGINE = MySQL('[HIDDEN]', '[HIDDEN]',"
                " '[HIDDEN]', '[HIDDEN]', '[HIDDEN]')"
            ),
        },
    },
    {
        "id": "dictionary source",
        "args": {
            "sql": (
                "CREATE DICTIONARY test_db.d (n UInt32)"
                " SOURCE(MYSQL(host 'db-host.example.com' password 'hunter2'))"
            ),
            "expected": (
                "CREATE DICTIONARY test_db.d (n UInt32)"
                " SOURCE(MYSQL(host '[HIDDEN]' password '[HIDDEN]'))"
            ),
        },
    },
    {
        "id": "merge tree with an inline disk definition",
        "args": {
            "sql": (
                "CREATE TABLE test_db.test_table (n Int32) ENGINE = MergeTree"
                " ORDER BY n SETTINGS disk = disk(type = s3,"
                " secret_access_key = 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY')"
            ),
            "expected": (
                "CREATE TABLE test_db.test_table (n Int32) ENGINE = MergeTree"
                " ORDER BY n SETTINGS disk = disk(type = s3,"
                " secret_access_key = '[HIDDEN]')"
            ),
        },
    },
    {
        "id": "backslash before the closing quote",
        "args": {
            "sql": "path = 'C:\\\\', password = 'hunter2'",
            "expected": "path = '[HIDDEN]', password = '[HIDDEN]'",
        },
    },
    {
        "id": "empty literal",
        "args": {
            "sql": "comment = ''",
            "expected": "comment = '[HIDDEN]'",
        },
    },
    {
        "id": "statement without literals is left alone",
        "args": {
            "sql": "CREATE TABLE test_db.test_table (n Int32) ENGINE = MergeTree ORDER BY n",
            "expected": "CREATE TABLE test_db.test_table (n Int32) ENGINE = MergeTree ORDER BY n",
        },
    },
    {
        "id": "backticked identifiers are not literals",
        "args": {
            "sql": "DROP NAMED COLLECTION `db_creds`",
            "expected": "DROP NAMED COLLECTION `db_creds`",
        },
    },
    {
        "id": "unbalanced quote leaves the tail unmasked",
        "args": {
            "sql": "password = 'hunter2",
            "expected": "password = 'hunter2",
        },
    },
    {
        "id": "empty statement",
        "args": {
            "sql": "",
            "expected": "",
        },
    },
)
def test_mask_sql_literals(sql: str, expected: str) -> None:
    assert mask_sql_literals(sql) == expected


def test_mask_secret_params():
    # pylint: disable=protected-access
    assert cli._mask_secret_params(
        {
            "config_parameters": [("clickhouse.clickhouse_password", "hunter2")],
            "host": "db-host.example.com",
        }
    ) == {
        "config_parameters": [("clickhouse.clickhouse_password", "[HIDDEN]")],
        "host": "db-host.example.com",
    }
