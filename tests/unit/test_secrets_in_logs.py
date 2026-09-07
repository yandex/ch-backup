"""
Unit tests for masking secrets before they reach the log.
"""

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
