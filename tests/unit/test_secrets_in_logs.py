"""
Unit tests for masking secrets before they reach the log.
"""

# pylint: disable=protected-access

import traceback
from collections.abc import Iterator
from unittest.mock import MagicMock, patch

import pytest
import requests
from click import Command
from click.testing import CliRunner
from loguru import logger

from ch_backup import logging
from ch_backup.backup.layout import BackupLayout
from ch_backup.cli import _mask_command_params, cli, copy_command
from ch_backup.clickhouse.client import (
    ClickhouseClient,
    ClickhouseError,
    ClickhouseErrorNotRetriable,
)
from ch_backup.clickhouse.control import ClickhouseCTL
from ch_backup.clickhouse.disks import ClickHouseBackupDisks
from ch_backup.clickhouse.masking import mask_named_collection, mask_sql_literals
from ch_backup.clickhouse.metadata_cleaner import (
    MetadataCleaner,
    _is_not_table_path_error,
    _is_table_was_not_dropped_error,
)
from ch_backup.clickhouse.models import Database, Table
from ch_backup.clickhouse.schema import rewrite_table_schema
from ch_backup.clickhouse.storage_schema import (
    StoragePolicy,
    StorageSchemaError,
    remove_unavailable_move_ttls,
)
from ch_backup.exceptions import ClickhouseBackupError
from ch_backup.logic.database_sync import (
    DatabaseZookeeperData,
    _check_ddl_queue_for_errors,
)
from ch_backup.logic.named_collections import NamedCollectionsBackup
from ch_backup.logic.storage_schema_restore import StorageSchemaRestore
from ch_backup.logic.table import TableBackup
from ch_backup.logic.workload_entities import WorkloadEntity
from ch_backup.util import get_table_zookeeper_paths
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


def _client(response: MagicMock | requests.Response) -> ClickhouseClient:
    """Use a controlled HTTP response with the real query implementation."""
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
    """Only the byte count of a sensitive query is printable."""
    query = "CREATE NAMED COLLECTION db_creds AS password = 'hunter2'"
    assert (
        _logged_query(query, sensitive=True)
        == f"Executing sensitive query: <{len(query.encode())} bytes>"
    )


@pytest.mark.parametrize(
    "query",
    [DECRYPT_QUERY, "CREATE HANDLER test URL '/test' AS SELECT 'private-token'"],
)
def test_sensitive_query_without_a_safe_header_is_not_logged(query: str) -> None:
    assert _logged_query(query, sensitive=True) == (
        f"Executing sensitive query: <{len(query.encode())} bytes>"
    )


def test_regular_query_is_logged_unmasked() -> None:
    query = "SELECT name FROM system.databases WHERE engine = 'Replicated'"
    assert query in _logged_query(query, sensitive=False)


CLICKHOUSE_ERROR_TEXT = (
    "Code: 62. DB::Exception: Syntax error: failed at position 47 ('missing_value'):"
    " missing_value, password = 'hunter2'. Expected one of: token, Comma."
    " (SYNTAX_ERROR) (version 24.8.1.1)"
)


def _query_error(
    error_text: str, sensitive: bool = True
) -> ClickhouseErrorNotRetriable:
    """Convert a real HTTP error response through the client."""
    response = requests.Response()
    response.status_code = 500
    response._content = error_text.encode()

    client = _client(response)
    with pytest.raises(ClickhouseErrorNotRetriable) as exc_info:
        client.query(
            "SYSTEM RESTORE REPLICA db.t",
            should_retry=False,
            sensitive=sensitive,
        )

    return exc_info.value


def test_sensitive_query_masks_clickhouse_error_text() -> None:
    """A sensitive query exposes only the numeric server error code."""
    assert str(_query_error(CLICKHOUSE_ERROR_TEXT, sensitive=True)) == (
        "Code: 62. ClickHouse query failed; server response hidden"
    )


def test_regular_query_hides_clickhouse_error_text() -> None:
    """Even a query without literals can return secrets from server metadata."""
    assert str(_query_error(CLICKHOUSE_ERROR_TEXT, sensitive=False)) == (
        "Code: 62. ClickHouse query failed; server response hidden"
    )


@pytest.fixture(name="log_messages")
def log_messages_fixture() -> Iterator[list[str]]:
    """Capture rendered logs and tracebacks without local variable diagnostics."""
    messages: list[str] = []
    sink = logger.add(
        messages.append, format="{message}", diagnose=False, backtrace=False
    )
    try:
        yield messages
    finally:
        logger.remove(sink)


@pytest.mark.parametrize("field", ["clickhouse_password", "clickhouse_user"])
@pytest.mark.parametrize("new_session", [False, True])
@pytest.mark.parametrize(
    "value",
    [
        "hunter2\n",
        "hunter2\r",
        "hunter2\r\nInjected: value",
        " hunter2",
        "hunter2\u2603",
        b"hunter2\n",
        {"password": "hunter2"},
    ],
)
def test_invalid_auth_header_never_exposes_credentials(
    field: str, new_session: bool, value: object, log_messages: list[str]
) -> None:
    """Real header validation fails safely before sending or retrying a request."""
    config = {**CLIENT_CONFIG, "host": "127.0.0.1", "port": 1}
    with pytest.raises(
        ClickhouseErrorNotRetriable, match="Invalid ClickHouse HTTP header"
    ) as error:
        try:
            if new_session:
                client = ClickhouseClient(config)
                config[field] = value
            else:
                config[field] = value
                client = ClickhouseClient(config)
            client.query("SELECT 1", sensitive=True, new_session=new_session)
        except Exception:
            logging.exception("Command failed")
            raise
    assert "hunter2" not in str(error.value)
    assert "hunter2" not in repr(error.value)
    assert "hunter2" not in "".join(traceback.format_exception(error.value))
    logged = "".join(log_messages)
    assert "hunter2" not in logged
    assert "Retrying" not in logged


@pytest.mark.parametrize("value", ["hunter2", "hunter2 ", "hunter2\u00e9", b"hunter2"])
def test_valid_auth_headers_are_preserved(value: str | bytes) -> None:
    """Valid credentials reach the prepared request without trimming or rewriting."""
    client = ClickhouseClient(
        {**CLIENT_CONFIG, "clickhouse_user": value, "clickhouse_password": value}
    )
    prepared = client._session.prepare_request(
        requests.Request("POST", "http://clickhouse.test/", data=b"SELECT 1")
    )
    assert prepared.headers["X-ClickHouse-User"] == value
    assert prepared.headers["X-ClickHouse-Key"] == value


@pytest.mark.parametrize("sensitive", [False, True])
@pytest.mark.parametrize("should_retry", [False, True])
@pytest.mark.parametrize(
    "error_text",
    [
        CLICKHOUSE_ERROR_TEXT,
        "Code: 62. DB::Exception: failed at position 47: hunter2', setting=123",
        "Code: 1000. DB::Exception: Authorization failed with password=hunter2",
        "upstream returned hunter2",
    ],
)
def test_query_errors_hide_secrets_in_retries_and_tracebacks(
    sensitive: bool, should_retry: bool, error_text: str, log_messages: list[str]
) -> None:
    """HTTP responses stay available for handling but never reach printable errors."""
    response = requests.Response()
    response.status_code = 500
    response.reason = "hunter2"
    response._content = error_text.encode()
    session = MagicMock()
    session.post.return_value = response
    with (
        patch.object(ClickhouseClient, "_create_session", return_value=session),
        patch.object(ClickhouseClient.query.retry, "sleep"),
    ):
        client = ClickhouseClient(CLIENT_CONFIG)
        with pytest.raises(ClickhouseError) as exc_info:
            try:
                client.query(
                    "SYSTEM RESTORE REPLICA db.t",
                    sensitive=sensitive,
                    should_retry=should_retry,
                )
            except ClickhouseError:
                logging.exception("Command 'restore' failed")
                raise

    error = exc_info.value
    assert session.post.call_count == (5 if should_retry else 1)
    assert "server response hidden" in str(error)
    assert "hunter2" not in repr(error)
    assert "hunter2" not in str(error.args)
    assert "hunter2" not in "".join(traceback.format_exception(error))
    assert "hunter2" not in "".join(log_messages)
    assert ("Retrying" in "".join(log_messages)) is should_retry
    assert error.server_message == error_text


@pytest.mark.parametrize(
    "method,statement",
    [
        (
            "restore_database",
            "CREATE DATABASE db ENGINE = MySQL('host', 'db', 'user', 'hunter2')",
        ),
        ("restore_udf", "CREATE FUNCTION f AS x -> concat(x, 'hunter2')"),
        (
            "restore_named_collection",
            "CREATE NAMED COLLECTION /* hunter2 */ nc AS password=123456",
        ),
        ("restore_http_handler", "CREATE HANDLER h URL '/test' AS SELECT 'hunter2'"),
        ("restore_access_entity", "CREATE USER u IDENTIFIED BY 'hunter2'"),
        (
            "restore_workload_entity",
            "CREATE WORKLOAD w SETTINGS arbitrary_secret='hunter2'",
        ),
        ("format_query", "CREATE WORKLOAD w SETTINGS arbitrary_secret='hunter2'"),
        (
            "create_table",
            "CREATE TABLE t (x Int64) ENGINE = MySQL('host', 'db', 't', 'user', 'hunter2')",
        ),
        (
            "create_table",
            "CREATE TABLE t (x Int64) ENGINE = MergeTree ORDER BY x SETTINGS disk=disk(type='s3', secret_access_key='hunter2')",
        ),
        ("create_table", "CREATE VIEW t AS SELECT * FROM s3('url', 'user', 'hunter2')"),
        (
            "create_table",
            "CREATE DICTIONARY t (x Int64) SOURCE(MYSQL(PASSWORD 'hunter2')) LAYOUT(FLAT())",
        ),
    ],
)
def test_restore_sends_original_sql_without_logging_it(
    method: str, statement: str, log_messages: list[str]
) -> None:
    """Exercise every free-form SQL entry point through the real client and logger."""
    response = MagicMock()
    response.json.return_value = {"data": [{"query": statement}]}
    session = MagicMock()
    session.post.return_value = response
    with patch.object(ClickhouseClient, "_create_session", return_value=session):
        client = ClickhouseClient(CLIENT_CONFIG)
    ctl = ClickhouseCTL.__new__(ClickhouseCTL)
    ctl._ch_client = client
    if method == "create_table":
        table = Table("db", "t", "MergeTree", [], [], "", statement, None)
        rewrite_table_schema(table)
        ctl.create_table(table)
        expected = table.create_statement
        assert "hunter2" in expected
    else:
        result = getattr(ctl, method)(statement)
        expected = statement
        if method == "format_query":
            assert result == statement
            expected = f"SELECT formatQuerySingleLine(unhex('{statement.encode().hex()}')) AS query FORMAT JSON"
    assert session.post.call_args.kwargs["data"] == expected.encode()
    logged = "".join(log_messages)
    assert "Executing sensitive query" in logged
    assert "hunter2" not in logged
    assert statement.encode().hex() not in logged
    assert expected not in logged


def test_unrecognized_database_schema_is_not_logged(log_messages: list[str]) -> None:
    """A parser warning cannot disclose the DDL it failed to recognize."""
    db = Database("db", None, None, None, None)
    db.set_engine_from_sql(
        "CREATE DATABASE db ENGINE MySQL('host', 'db', 'user', 'hunter2')"
    )
    assert db.engine is None
    assert "Failed to parse engine" in "".join(log_messages)
    assert "hunter2" not in "".join(log_messages)


@pytest.mark.parametrize("option", ["config_parameters", "target_config_parameters"])
def test_all_config_override_values_are_hidden(option: str) -> None:
    """Unknown keys and nested objects are hidden without changing CLI input."""
    values = [("new.option", {"nested": ["hunter2", 123456]})]
    params = {option: values, "keep_going": True}
    assert _mask_command_params(params) == {
        option: [("new.option", "[HIDDEN]")],
        "keep_going": True,
    }
    assert params[option] == values
    assert values[0][1]["nested"] == ["hunter2", 123456]


@pytest.mark.parametrize("target", [False, True])
def test_invalid_json_override_hides_value(target: bool) -> None:
    """Both actual CLI option definitions reject malformed JSON without echoing it."""
    source = copy_command if target else cli
    name = "target_config_parameters" if target else "config_parameters"
    option = next(param for param in source.params if param.name == name)
    command = Command("check", params=[option])
    flag = "--target-config-parameter" if target else "--config-parameter"
    result = CliRunner().invoke(
        command, [flag, "storage.credentials", '{"secret_access_key": "hunter2"']
    )
    assert result.exit_code == 2
    assert "Not a valid json value" in result.output
    assert "hunter2" not in result.output


@pytest.mark.parametrize(
    "message,table_exists,invalid_path",
    [
        ("TABLE_WAS_NOT_DROPPED", True, False),
        ("There is a local table", True, False),
        ("does not look like a table path", False, True),
        ("unexpected server failure", False, False),
    ],
)
def test_error_classification_uses_original_response(
    message: str, table_exists: bool, invalid_path: bool
) -> None:
    """Masking printable errors preserves the existing cleanup predicates."""
    error = _query_error(message)
    assert _is_table_was_not_dropped_error(error) is table_exists
    assert _is_not_table_path_error(error) is invalid_path


@pytest.mark.parametrize("same_name", [False, True])
def test_schema_conflicts_hide_both_schemas(
    same_name: bool, log_messages: list[str]
) -> None:
    """Both conflict branches replace the right table without logging either DDL."""
    table_uuid = "223b4576-76f0-4ed3-976f-46db82af82a9"
    statement = (
        "CREATE TABLE t (x Int64) ENGINE=MySQL('host', 'db', 't', 'user', 'hunter2')"
    )
    table = Table("db", "t", "MySQL", [], [], "", statement, table_uuid)
    old_name = "t" if same_name else "old"
    old = Table(
        "db",
        old_name,
        "MySQL",
        [],
        [],
        "",
        statement.replace("hunter2", "old-secret"),
        table_uuid,
    )
    stored = {("db", old_name): old}
    context = MagicMock()
    context.config = {"force_non_replicated": False, "override_replica_name": None}
    context.config_root = {"restore": {"max_table_name": 255}}
    context.ch_ctl.get_tables.return_value = list(stored.values())
    context.ch_ctl.get_detached_tables.return_value = []
    context.ch_ctl.get_replicas.return_value = []
    context.ch_ctl.drop_table_if_exists.side_effect = lambda item: stored.pop(
        (item.database, item.name)
    )
    context.ch_ctl.create_table.side_effect = lambda item: stored.__setitem__(
        (item.database, item.name), item
    )
    db = Database("db", "Atomic", None, None, None)

    result = TableBackup()._preprocess_tables_to_restore(
        context, {"db": db}, [table], False, False, None
    )
    assert result == ([table], 0, set())
    assert not stored
    TableBackup._restore_table_object(context, db, table)
    assert stored == {("db", "t"): table}
    assert "hunter2" in stored["db", "t"].create_statement
    logged = "".join(log_messages)
    assert "schema mismatches" in logged
    assert "hunter2" not in logged and "old-secret" not in logged


@pytest.mark.parametrize("recoverable", [False, True])
def test_detached_table_error_retains_recovery_decision(
    recoverable: bool, log_messages: list[str]
) -> None:
    """Only the original missing-DDL error enables detached table repair."""
    error_text = "CANNOT_GET_CREATE_TABLE_QUERY" if recoverable else "UNKNOWN_TABLE"
    error = _query_error(error_text + " hunter2")
    table = Table(
        "db",
        "t",
        "MergeTree",
        [],
        [],
        "",
        "CREATE TABLE t (x Int64) ENGINE=MergeTree ORDER BY x",
        None,
    )
    context = MagicMock()
    context.config = {"force_non_replicated": False, "override_replica_name": None}
    context.ch_ctl.get_tables.return_value = []
    context.ch_ctl.get_detached_tables.return_value = [table]
    context.ch_ctl.get_replicas.return_value = []
    context.ch_ctl.attach_table.side_effect = [error, None]
    db = Database("db", "Atomic", None, None, None)
    result = TableBackup()._preprocess_tables_to_restore(
        context, {"db": db}, [table], True, False, None
    )
    assert result == (([table], 0, set()) if recoverable else ([], 1, {("db", "t")}))
    assert context.ch_ctl.drop_table_if_exists.call_count == int(recoverable)
    assert context.ch_ctl.create_metadata_for_missed_detached_table.call_count == int(
        recoverable
    )
    assert "hunter2" not in "".join(log_messages)


def test_local_schema_errors_hide_sql() -> None:
    """Malformed backup SQL is absent from local parser errors as well."""
    sql = "broken SQL containing hunter2"
    with pytest.raises(
        RuntimeError, match="Cannot parse workload entity"
    ) as workload_error:
        WorkloadEntity.from_create_statement(sql)
    with pytest.raises(
        RuntimeError, match="Expected CREATE NAMED COLLECTION"
    ) as collection_error:
        NamedCollectionsBackup._add_if_not_exists(sql)
    table = Table("db", "t", "ReplicatedMergeTree", [], [], "", sql, None)
    with pytest.raises(
        ClickhouseBackupError, match="Couldn't parse create statement"
    ) as table_error:
        get_table_zookeeper_paths([table])
    db = Database("db", "Replicated", None, None, sql)
    with pytest.raises(RuntimeError, match="cannot parse ZK path") as database_error:
        DatabaseZookeeperData.parse_replicated_db_zk_info(db, {})
    policy = StoragePolicy("default", ("default",), ("default",), ("default",))
    with pytest.raises(
        StorageSchemaError, match="Cannot parse TTL MOVE rule"
    ) as ttl_error:
        remove_unavailable_move_ttls("x TO DISK concat('hunter2')", policy)
    for error in (
        workload_error,
        collection_error,
        table_error,
        database_error,
        ttl_error,
    ):
        assert "hunter2" not in str(error.value)


@pytest.mark.parametrize("error_code", [0, 62])
def test_ddl_queue_error_hides_response(
    error_code: int, log_messages: list[str]
) -> None:
    """A DDL queue error is still detected without printing its server text."""
    context = MagicMock()
    context.ch_ctl.get_ddl_queue_unfinished_status.return_value = [
        {
            "entry": "query-123",
            "exception_code": error_code,
            "exception_text": "Code: 62. hunter2",
        }
    ]
    assert _check_ddl_queue_for_errors(context, "db") is bool(error_code)
    logged = "".join(log_messages)
    assert "hunter2" not in logged
    if error_code:
        assert "query-123" in logged and "exception_code=62" in logged


@pytest.mark.parametrize(
    "message,removes_replica,raises",
    [
        ("TABLE_WAS_NOT_DROPPED", True, False),
        ("There is a local table", True, False),
        ("does not look like a table path", False, False),
        ("unexpected error", False, True),
    ],
)
def test_cleanup_preserves_decisions_and_other_replicas(
    message: str, removes_replica: bool, raises: bool, log_messages: list[str]
) -> None:
    """An HTTP failure deletes only the intended replica on the existing fallback."""
    error = _query_error(message + " hunter2")
    root = "/tables/db/t/replicas/r1"
    other = "/tables/db/t/replicas/r2"
    nodes = {root, root + "/is_active", other, other + "/is_active"}
    controller = MagicMock()
    controller.get_macros.return_value = {}
    controller.system_drop_replica.side_effect = error
    zk_ctl = MagicMock()
    zk_ctl.zk_root_path = ""
    zk_client = zk_ctl.zk_client.__enter__.return_value
    zk_client.exists.side_effect = lambda path: path in nodes

    def delete(path: str, recursive: bool = False) -> None:
        """Apply Keeper deletion to the selected subtree in the fixture."""
        nodes.difference_update(
            {
                node
                for node in nodes
                if node == path or (recursive and node.startswith(path + "/"))
            }
        )

    zk_client.delete.side_effect = delete
    table = Table(
        "db",
        "t",
        "ReplicatedMergeTree",
        [],
        [],
        "",
        "CREATE TABLE db.t (x Int64) ENGINE=ReplicatedMergeTree('/tables/db/t', 'r1') ORDER BY x",
        None,
    )
    cleaner = MetadataCleaner(controller, zk_ctl, "r1", 1)
    try:
        if raises:
            with pytest.raises(
                ClickhouseErrorNotRetriable, match="server response hidden"
            ):
                cleaner.clean_tables_metadata([table])
        else:
            cleaner.clean_tables_metadata([table])
    finally:
        cleaner._exec_pool.shutdown()
    assert (root not in nodes) is removes_replica
    assert other in nodes and other + "/is_active" in nodes
    assert "hunter2" not in "".join(log_messages)


@pytest.mark.parametrize(
    "message,raises",
    [
        ("does not look like a path of Replicated database", False),
        ("node doesn't exist", False),
        ("unexpected error", True),
    ],
)
def test_database_cleanup_keeps_original_error_decision(
    message: str, raises: bool, log_messages: list[str]
) -> None:
    """Only the previously ignored database cleanup errors are still ignored."""
    controller = MagicMock()
    controller.get_macros.return_value = {}
    controller.system_drop_database_replica.side_effect = _query_error(
        message + " hunter2"
    )
    zk_ctl = MagicMock()
    zk_ctl.zk_root_path = ""
    db = Database("db", "Replicated", None, None, "Replicated('/db', 's1', 'r1')")
    cleaner = MetadataCleaner(controller, zk_ctl, "r1", 1)
    try:
        if raises:
            with pytest.raises(
                ClickhouseErrorNotRetriable, match="server response hidden"
            ):
                cleaner.clean_database_metadata([db])
        else:
            cleaner.clean_database_metadata([db])
    finally:
        cleaner._exec_pool.shutdown()
    assert "hunter2" not in "".join(log_messages)


@pytest.mark.parametrize("operation", ["frozen", "metadata", "copy"])
def test_missing_table_path_does_not_print_table_schema(operation: str) -> None:
    """Assertions about missing disk paths cannot expose the table's DDL."""
    table = Table("db", "t", "MergeTree", [], [], "", "hunter2", None)
    with pytest.raises(
        AssertionError, match="db.t doesn't store data on disk"
    ) as error:
        if operation == "frozen":
            BackupLayout.has_frozen_cloud_storage_data(
                MagicMock(), MagicMock(), MagicMock(), table
            )
        elif operation == "metadata":
            BackupLayout.upload_cloud_storage_metadata(
                MagicMock(), MagicMock(), MagicMock(), table
            )
        else:
            ClickHouseBackupDisks.copy_table_data(MagicMock(), "default", table)
    assert "hunter2" not in str(error.value)


def test_removed_ttl_is_not_logged(log_messages: list[str]) -> None:
    """Adapting a MOVE rule preserves other expressions without logging its SQL."""
    context = MagicMock()
    context.ch_ctl.get_storage_policies.return_value = {
        "default": StoragePolicy("default", ("default",), ("default",), ("default",))
    }
    context.ch_config.config = {}
    table = Table(
        "db",
        "t",
        "MergeTree",
        [],
        [],
        "",
        "CREATE TABLE db.t (d DateTime) ENGINE=MergeTree ORDER BY d TTL toDateTime('hunter2') TO VOLUME 'cold', d + INTERVAL 30 DAY DELETE",
        None,
    )
    StorageSchemaRestore(context).adapt(table)
    assert "hunter2" not in table.create_statement
    assert "d + INTERVAL 30 DAY DELETE" in table.create_statement
    logged = "".join(log_messages)
    assert "Removing 1 TTL MOVE rule(s)" in logged
    assert "hunter2" not in logged
