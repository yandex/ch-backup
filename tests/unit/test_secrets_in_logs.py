"""
Unit tests for masking secrets before they reach the log.
"""

from unittest.mock import MagicMock, patch

import pytest
import requests

from ch_backup.clickhouse.client import ClickhouseClient, ClickhouseErrorNotRetriable
from ch_backup.clickhouse.masking import mask_named_collection, mask_sql_literals
from tests.unit.utils import parametrize

CLIENT_CONFIG = {
    "host": "clickhouse.test",
    "protocol": "http",
    "port": 8123,
    "timeout": 1,
    "connect_timeout": 1,
}

DECRYPT_QUERY = (
    "SELECT decrypt('aes-128-ctr', unhex('0a0b0c'), unhex('0d0e0f'), unhex('010203'))"
    " AS data FORMAT JSON"
)


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
            "expected": "CREATE NAMED COLLECTION IF NOT EXISTS s3_args AS '[HIDDEN]'",
        },
    },
    {
        "id": "cluster name is kept in the header",
        "args": {
            "sql": (
                "CREATE NAMED COLLECTION db_creds ON CLUSTER 'ch_cluster' AS"
                " password = 'hunter2'"
            ),
            "expected": (
                "CREATE NAMED COLLECTION db_creds ON CLUSTER 'ch_cluster' AS '[HIDDEN]'"
            ),
        },
    },
    {
        "id": "collection name containing a keyword is kept in the header",
        "args": {
            "sql": "CREATE NAMED COLLECTION as_creds AS password = 123456",
            "expected": "CREATE NAMED COLLECTION as_creds AS '[HIDDEN]'",
        },
    },
    {
        "id": "value containing the values keyword does not expose the pairs",
        "args": {
            "sql": (
                "CREATE NAMED COLLECTION db_creds AS"
                " query = 'SELECT 1 AS x', password = 123456"
            ),
            "expected": "CREATE NAMED COLLECTION db_creds AS '[HIDDEN]'",
        },
    },
    {
        "id": "keywords are case insensitive",
        "args": {
            "sql": "create named collection db_creds as password = 'hunter2'",
            "expected": "create named collection db_creds as '[HIDDEN]'",
        },
    },
    {
        "id": "leading whitespace and line breaks are tolerated",
        "args": {
            "sql": "\nCREATE NAMED COLLECTION db_creds\nAS\npassword = 'hunter2'",
            "expected": "\nCREATE NAMED COLLECTION db_creds\nAS '[HIDDEN]'",
        },
    },
    {
        "id": "statement is recognized only at the start",
        "args": {
            "sql": (
                "Code: 62. DB::Exception: Syntax error in"
                " CREATE NAMED COLLECTION db_creds AS port = 3306"
            ),
            "expected": None,
        },
    },
    {
        "id": "statement declaring nothing is not recognized",
        "args": {
            "sql": "CREATE NAMED COLLECTION db_creds",
            "expected": None,
        },
    },
    {
        "id": "another create statement is not recognized",
        "args": {
            "sql": "CREATE TABLE test_db.test_table AS SELECT 1",
            "expected": None,
        },
    },
)
def test_mask_named_collection(sql: str, expected: str | None) -> None:
    assert mask_named_collection(sql) == expected


@parametrize(
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
        "id": "doubled quote escapes the quote inside a literal",
        "args": {
            "sql": "SELECT 'it''s a secret' FROM test_table",
            "expected": "SELECT '[HIDDEN]' FROM test_table",
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
        "id": "apostrophe in a backquoted identifier does not shift masking",
        "args": {
            "sql": "SELECT decrypt('aes-128-ctr', `col's`, 'key_hex') FROM test_table",
            "expected": "SELECT decrypt('[HIDDEN]', `col's`, '[HIDDEN]') FROM test_table",
        },
    },
    {
        "id": "unterminated identifier is masked to the end",
        "args": {
            "sql": "SELECT `col, password = 'hunter2'",
            "expected": "SELECT '[HIDDEN]'",
        },
    },
)
def test_mask_sql_literals(sql: str, expected: str) -> None:
    assert mask_sql_literals(sql) == expected


def _client(response: MagicMock) -> ClickhouseClient:
    session = MagicMock()
    session.post.return_value = response
    with patch.object(ClickhouseClient, "_create_session", return_value=session):
        return ClickhouseClient(CLIENT_CONFIG)


def _logged_query(query: str, sensitive: bool) -> str:
    response = MagicMock()
    response.raise_for_status.return_value = None
    response.json.return_value = {}

    client = _client(response)
    with patch("ch_backup.clickhouse.client.logging") as logging_mock:
        client.query(query, sensitive=sensitive)

    logging_mock.debug.assert_called_once()
    message, *args = logging_mock.debug.call_args.args
    return message.format(*args)


def test_sensitive_named_collection_query_is_logged_without_values() -> None:
    assert (
        _logged_query(
            "CREATE NAMED COLLECTION db_creds AS password = 'hunter2'", sensitive=True
        )
        == "Executing sensitive query: CREATE NAMED COLLECTION db_creds AS '[HIDDEN]'"
    )


def test_sensitive_query_without_a_safe_header_is_not_logged() -> None:
    assert _logged_query(DECRYPT_QUERY, sensitive=True) == (
        f"Executing sensitive query: <{len(DECRYPT_QUERY.encode())} bytes>"
    )


def test_regular_query_is_logged_unmasked() -> None:
    query = "SELECT name FROM system.databases WHERE engine = 'Replicated'"
    assert query in _logged_query(query, sensitive=False)


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

    client = _client(response)
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
