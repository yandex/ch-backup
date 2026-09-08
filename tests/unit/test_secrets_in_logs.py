"""
Unit tests for masking secrets before they reach the log.
"""

from unittest.mock import MagicMock, patch

import pytest
import requests

from ch_backup.clickhouse.client import ClickhouseClient, ClickhouseErrorNotRetriable
from ch_backup.clickhouse.masking import mask_sql_literals
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
        "id": "numeric value is masked",
        "args": {
            "sql": "CREATE NAMED COLLECTION numeric_creds AS password = 123456",
            "expected": "CREATE NAMED COLLECTION numeric_creds AS password = '[HIDDEN]'",
        },
    },
    {
        "id": "values of every literal type are masked",
        "args": {
            "sql": (
                "CREATE NAMED COLLECTION mixed_creds AS"
                " key1 = 1, key2 = 'value2', key3 = true, key4 = NULL, key5 = -1.5"
            ),
            "expected": (
                "CREATE NAMED COLLECTION mixed_creds AS"
                " key1 = '[HIDDEN]', key2 = '[HIDDEN]', key3 = '[HIDDEN]',"
                " key4 = '[HIDDEN]', key5 = '[HIDDEN]'"
            ),
        },
    },
    {
        "id": "apostrophe in a backquoted collection name",
        "args": {
            "sql": "CREATE NAMED COLLECTION `team's` AS password = 'hunter2'",
            "expected": "CREATE NAMED COLLECTION `team's` AS password = '[HIDDEN]'",
        },
    },
    {
        "id": "commas inside a map value do not split the pair",
        "args": {
            "sql": (
                "CREATE NAMED COLLECTION headers_creds AS"
                " headers = {'Authorization':'Bearer token', 'X-Trace':'on'},"
                " password = 123"
            ),
            "expected": (
                "CREATE NAMED COLLECTION headers_creds AS"
                " headers = '[HIDDEN]', password = '[HIDDEN]'"
            ),
        },
    },
    {
        "id": "commas inside an inline disk definition do not split the pair",
        "args": {
            "sql": (
                "CREATE NAMED COLLECTION disk_creds AS"
                " disk = disk(type = 's3', secret_access_key = 'secret'),"
                " user = 'remote_user'"
            ),
            "expected": (
                "CREATE NAMED COLLECTION disk_creds AS"
                " disk = '[HIDDEN]', user = '[HIDDEN]'"
            ),
        },
    },
    {
        "id": "overridable modifier is kept",
        "args": {
            "sql": (
                "CREATE NAMED COLLECTION overridable_creds AS"
                " password = 'hunter2' NOT OVERRIDABLE, user = 'remote_user' OVERRIDABLE"
            ),
            "expected": (
                "CREATE NAMED COLLECTION overridable_creds AS"
                " password = '[HIDDEN]' NOT OVERRIDABLE,"
                " user = '[HIDDEN]' OVERRIDABLE"
            ),
        },
    },
    {
        "id": "alter sets are masked",
        "args": {
            "sql": (
                "ALTER NAMED COLLECTION db_creds SET"
                " password = 'hunter2', port = 3306"
            ),
            "expected": (
                "ALTER NAMED COLLECTION db_creds SET"
                " password = '[HIDDEN]', port = '[HIDDEN]'"
            ),
        },
    },
    {
        "id": "alter keeps the list of deleted keys",
        "args": {
            "sql": (
                "ALTER NAMED COLLECTION db_creds SET password = 'hunter2'"
                " DELETE old_password, legacy_user"
            ),
            "expected": (
                "ALTER NAMED COLLECTION db_creds SET password = '[HIDDEN]'"
                " DELETE old_password, legacy_user"
            ),
        },
    },
    {
        "id": "alter keeps the overridable modifier before deleted keys",
        "args": {
            "sql": "ALTER NAMED COLLECTION db_creds SET user = 'u' OVERRIDABLE DELETE password",
            "expected": (
                "ALTER NAMED COLLECTION db_creds SET user = '[HIDDEN]' OVERRIDABLE"
                " DELETE password"
            ),
        },
    },
    {
        "id": "alter with a key named delete masks every value",
        "args": {
            "sql": "ALTER NAMED COLLECTION db_creds SET delete = 1, password = 'hunter2'",
            "expected": (
                "ALTER NAMED COLLECTION db_creds SET"
                " delete = '[HIDDEN]', password = '[HIDDEN]'"
            ),
        },
    },
    {
        "id": "literals after delete are masked too",
        "args": {
            "sql": "ALTER NAMED COLLECTION db_creds SET port = 3306 DELETE user, password = 'hunter2'",
            "expected": (
                "ALTER NAMED COLLECTION db_creds SET port = '[HIDDEN]'"
                " DELETE user, password = '[HIDDEN]'"
            ),
        },
    },
    {
        "id": "alter without a set clause is left alone",
        "args": {
            "sql": "ALTER NAMED COLLECTION db_creds DELETE password, user",
            "expected": "ALTER NAMED COLLECTION db_creds DELETE password, user",
        },
    },
    {
        "id": "alter on cluster is masked",
        "args": {
            "sql": (
                "ALTER NAMED COLLECTION IF EXISTS db_creds ON CLUSTER `ch_cluster`"
                " SET password = 'hunter2'"
            ),
            "expected": (
                "ALTER NAMED COLLECTION IF EXISTS db_creds ON CLUSTER `ch_cluster`"
                " SET password = '[HIDDEN]'"
            ),
        },
    },
    {
        "id": "unterminated literal hides all values of an altered collection",
        "args": {
            "sql": "ALTER NAMED COLLECTION db_creds SET password = 'hunter2",
            "expected": "ALTER NAMED COLLECTION db_creds SET '[HIDDEN]'",
        },
    },
    {
        "id": "unterminated literal hides all values of a named collection",
        "args": {
            "sql": "CREATE NAMED COLLECTION db_creds AS user = 'u', password = 'hunter2",
            "expected": "CREATE NAMED COLLECTION db_creds AS '[HIDDEN]'",
        },
    },
    {
        "id": "unclosed parenthesis hides all values of a named collection",
        "args": {
            "sql": (
                "CREATE NAMED COLLECTION disk_creds AS"
                " disk = disk(type = 's3', secret_access_key = 'secret'"
            ),
            "expected": "CREATE NAMED COLLECTION disk_creds AS '[HIDDEN]'",
        },
    },
    {
        "id": "unexpected closing parenthesis hides all values of a named collection",
        "args": {
            "sql": "CREATE NAMED COLLECTION db_creds AS port = 3306), password = 'hunter2'",
            "expected": "CREATE NAMED COLLECTION db_creds AS '[HIDDEN]'",
        },
    },
    {
        "id": "value without a key hides all values of a named collection",
        "args": {
            "sql": "CREATE NAMED COLLECTION db_creds AS missing_value, password = 'hunter2'",
            "expected": "CREATE NAMED COLLECTION db_creds AS '[HIDDEN]'",
        },
    },
    {
        "id": "named collection statement is recognized only at the start",
        "args": {
            "sql": (
                "Code: 62. DB::Exception: Syntax error in"
                " CREATE NAMED COLLECTION db_creds AS port = 3306"
            ),
            "expected": (
                "Code: 62. DB::Exception: Syntax error in"
                " CREATE NAMED COLLECTION db_creds AS port = 3306"
            ),
        },
    },
    {
        "id": "keyword is matched only on word boundaries",
        "args": {
            "sql": "CREATE NAMED COLLECTION fast_creds",
            "expected": "CREATE NAMED COLLECTION fast_creds",
        },
    },
    {
        "id": "literals are masked when the values keyword is missing",
        "args": {
            "sql": "CREATE NAMED COLLECTION db_creds ON CLUSTER 'ch_cluster'",
            "expected": "CREATE NAMED COLLECTION db_creds ON CLUSTER '[HIDDEN]'",
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
        "id": "doubled quote escapes the quote inside a literal",
        "args": {
            "sql": "SELECT 'it''s a secret' FROM test_table",
            "expected": "SELECT '[HIDDEN]' FROM test_table",
        },
    },
    {
        "id": "doubled quote hides a comma inside a named collection value",
        "args": {
            "sql": (
                "CREATE NAMED COLLECTION db_creds AS"
                " password = 'it''s, a secret', user = 'remote_user'"
            ),
            "expected": (
                "CREATE NAMED COLLECTION db_creds AS"
                " password = '[HIDDEN]', user = '[HIDDEN]'"
            ),
        },
    },
    {
        "id": "doubled backquote escapes the backquote inside an identifier",
        "args": {
            "sql": "DROP NAMED COLLECTION `db``creds`",
            "expected": "DROP NAMED COLLECTION `db``creds`",
        },
    },
    {
        "id": "unterminated identifier is masked to the end",
        "args": {
            "sql": "SELECT `col, password = 'hunter2'",
            "expected": "SELECT '[HIDDEN]'",
        },
    },
    {
        "id": "apostrophe in a backquoted identifier does not shift masking",
        "args": {
            "sql": "SELECT decrypt('aes-128-ctr', `col's`, 'key_hex') FROM test_table",
            "expected": "SELECT decrypt('[HIDDEN]', `col's`, '[HIDDEN]') FROM test_table",
        },
    },
    {
        "id": "unterminated literal is masked to the end",
        "args": {
            "sql": "password = 'hunter2",
            "expected": "password = '[HIDDEN]'",
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


CLICKHOUSE_ERROR_TEXT = (
    "Code: 62. DB::Exception: Syntax error: failed at position 47 ('missing_value'):"
    " missing_value, password = 'hunter2'. Expected one of: token, Comma."
    " (SYNTAX_ERROR) (version 24.8.1.1)"
)


def _query_error_text(error_text: str, sensitive: bool) -> str:
    response = MagicMock()
    response.text = error_text
    response.raise_for_status.side_effect = requests.exceptions.HTTPError(
        response=response
    )
    session = MagicMock()
    session.post.return_value = response

    with patch.object(ClickhouseClient, "_create_session", return_value=session):
        client = ClickhouseClient(
            {
                "host": "clickhouse.test",
                "protocol": "http",
                "port": 8123,
                "timeout": 1,
                "connect_timeout": 1,
            }
        )
        with pytest.raises(ClickhouseErrorNotRetriable) as exc_info:
            client.query(
                "CREATE NAMED COLLECTION db_creds AS missing_value, password = 'hunter2'",
                should_retry=False,
                sensitive=sensitive,
            )

    return str(exc_info.value)


def test_sensitive_query_masks_clickhouse_error_text() -> None:
    assert _query_error_text(CLICKHOUSE_ERROR_TEXT, sensitive=True) == (
        "Code: 62. DB::Exception: Syntax error: failed at position 47 ('[HIDDEN]'):"
        " missing_value, password = '[HIDDEN]'. Expected one of: token, Comma."
        " (SYNTAX_ERROR) (version 24.8.1.1)"
    )


def test_regular_query_keeps_clickhouse_error_text() -> None:
    assert _query_error_text(CLICKHOUSE_ERROR_TEXT, sensitive=False) == (
        CLICKHOUSE_ERROR_TEXT
    )
