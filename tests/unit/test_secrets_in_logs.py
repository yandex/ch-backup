"""
Unit tests ensuring secrets do not leak into the log.

Reproduces a scenario: restoring named collections wrote raw passwords and
S3 credentials into ch-backup.log at DEBUG level via "Executing query: ...".
All hosts, buckets and object names below are synthetic.
"""

import copy
from contextlib import contextmanager
from typing import Any, Generator, List, Optional, cast
from unittest.mock import MagicMock, patch

import click
import requests
from loguru import logger

from ch_backup import cli
from ch_backup.clickhouse.client import ClickhouseClient
from ch_backup.clickhouse.control import ClickhouseCTL
from ch_backup.clickhouse.models import Database, Table
from ch_backup.clickhouse.schema import rewrite_table_schema
from ch_backup.config import DEFAULT_CONFIG
from ch_backup.logic.named_collections import NamedCollectionsBackup
from ch_backup.util import mask_sql_literals
from tests.unit.utils import parametrize

UUID = "223b4576-76f0-4ed3-976f-46db82af82a9"
CH_VERSION = "24.8.1.1"

S3_ACCESS_KEY_ID = "AKIAIOSFODNN7EXAMPLE"
S3_SECRET_ACCESS_KEY = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
PLAIN_TEXT_PASSWORD = "hunter2-plain-text-password"
ENCRYPTION_KEY_HEX = "0123456789abcdef0123456789abcdef"

NC_S3_NAME = "s3_source_args"
NC_MYSQL_NAME = "external_db_creds"
NC_S3_STATEMENT = (
    f"CREATE NAMED COLLECTION IF NOT EXISTS {NC_S3_NAME} AS"
    f" access_key_id = '{S3_ACCESS_KEY_ID}',"
    " format = 'CSVWithNames',"
    f" secret_access_key = '{S3_SECRET_ACCESS_KEY}',"
    " structure = '\\r\\n    item_id UInt64,\\r\\n"
    "    kind Enum8(\\'AAA\\' = 1, \\'BBB\\' = 2)\\r\\n    ',"
    " url = 'https://s3.example.com/test-bucket/data/*.xz'"
)

NC_MYSQL_STATEMENT = (
    f"CREATE NAMED COLLECTION {NC_MYSQL_NAME} AS"
    " addresses_expr = 'db-host.example.com:3306',"
    " db = 'test_db',"
    f" password = '{PLAIN_TEXT_PASSWORD}',"
    " table = 'test_table',"
    " user = 'test_user'"
)

MYSQL_ENGINE_CLAUSE = (
    " ENGINE = MySQL('db-host.example.com:3306', 'test_db', 'test_table',"
    f" 'test_user', '{PLAIN_TEXT_PASSWORD}')"
)

MYSQL_TABLE_STATEMENT = (
    "CREATE TABLE test_db.test_table (s String, n Int32)" + MYSQL_ENGINE_CLAUSE
)

ALL_SECRETS = (
    S3_ACCESS_KEY_ID,
    S3_SECRET_ACCESS_KEY,
    PLAIN_TEXT_PASSWORD,
    ENCRYPTION_KEY_HEX,
)


class _FakeResponse:
    """
    Minimal stand-in for requests.Response.
    """

    def __init__(self, payload: Optional[dict] = None, text: str = "") -> None:
        self._payload = payload
        self.text = text

    def raise_for_status(self) -> None:
        """
        No-op: the fake response is always successful.
        """

    def json(self) -> dict:
        """
        Return the JSON payload, mimicking requests for non-JSON responses.
        """
        if self._payload is None:
            raise ValueError("response is not JSON")
        return self._payload


def _answer(*_args: Any, **kwargs: Any) -> _FakeResponse:
    """
    Answer a ClickHouse HTTP request without touching the network.

    Only the queries issued while building ClickhouseCTL need real-looking data.
    """
    query = kwargs.get("data", b"")
    if isinstance(query, bytes):
        query = query.decode("utf-8", "replace")

    if "version()" in query:
        return _FakeResponse(text=CH_VERSION)
    if "system.disks" in query:
        return _FakeResponse(payload={"data": []})
    return _FakeResponse(payload={"data": [{"data": ""}]})


@contextmanager
def clickhouse_client() -> Generator[ClickhouseClient, None, None]:
    """
    Build a ClickhouseClient whose HTTP transport is scripted.
    """
    with patch.object(requests.Session, "post", side_effect=_answer):
        yield ClickhouseClient(copy.deepcopy(cast(dict, DEFAULT_CONFIG["clickhouse"])))


@contextmanager
def clickhouse_ctl() -> Generator[ClickhouseCTL, None, None]:
    """
    Build a ClickhouseCTL through its real constructor, with a scripted transport.
    """
    with patch.object(requests.Session, "post", side_effect=_answer):
        yield ClickhouseCTL(
            copy.deepcopy(cast(dict, DEFAULT_CONFIG["clickhouse"])), {}, {}
        )


@contextmanager
def capture_logs() -> Generator[List[str], None, None]:
    """
    Collect everything ch-backup writes to loguru during the block.
    """
    messages: List[str] = []
    sink_id = logger.add(
        lambda message: messages.append(str(message)), level="DEBUG", format="{message}"
    )
    try:
        yield messages
    finally:
        logger.remove(sink_id)


def assert_no_secrets(messages: List[str]) -> None:
    """
    Fail if any known secret value appears in the captured log.
    """
    log = "".join(messages)
    for secret in ALL_SECRETS:
        assert secret not in log, f"secret leaked into the log: {log}"


def make_table(engine: str, create_statement: str, name: str = "test_table") -> Table:
    """
    Build a table with the given engine and create statement.
    """
    return Table(
        database="test_db",
        name=name,
        engine=engine,
        disks=[],
        data_paths=[],
        metadata_path="",
        create_statement=create_statement,
        uuid=UUID,
    )


#
# ClickhouseClient: the `sensitive` flag must suppress the query text.
#


def test_sensitive_query_is_not_logged() -> None:
    """
    The exact scenario: restoring a named collection must not print the statement.
    """
    with clickhouse_client() as client, capture_logs() as messages:
        client.query(NC_MYSQL_STATEMENT, sensitive=True)

    assert_no_secrets(messages)


def test_sensitive_query_logs_a_placeholder() -> None:
    """
    The line must stay in the log, so the query is still traceable.
    """
    with clickhouse_client() as client, capture_logs() as messages:
        client.query(NC_MYSQL_STATEMENT, sensitive=True)

    log = "".join(messages)
    assert "Executing query" in log
    assert "password = '[HIDDEN]'" in log


def test_non_sensitive_query_is_still_logged_in_full() -> None:
    """
    Guard against over-masking: ordinary queries must remain fully visible.
    """
    query = "SELECT name FROM system.named_collections FORMAT JSON"

    with clickhouse_client() as client, capture_logs() as messages:
        client.query(query)

    assert query in "".join(messages)


#
# ClickhouseCTL: no method executing backup-derived SQL may log it.
#


def test_restore_named_collection_does_not_log_the_statement() -> None:
    """
    The statement from the scenario log carries S3 credentials.
    """
    with clickhouse_ctl() as ctl, capture_logs() as messages:
        ctl.restore_named_collection(NC_S3_STATEMENT)

    assert_no_secrets(messages)


def test_restore_database_does_not_log_the_statement() -> None:
    """
    Database engines such as MySQL keep the password in the create statement.
    """
    statement = "CREATE DATABASE test_db" + MYSQL_ENGINE_CLAUSE

    with clickhouse_ctl() as ctl, capture_logs() as messages:
        ctl.restore_database(statement)

    assert_no_secrets(messages)


def test_restore_udf_does_not_log_the_statement() -> None:
    """
    UDF statements come from the backup and are not validated by ch-backup.
    """
    statement = f"CREATE FUNCTION f AS x -> '{PLAIN_TEXT_PASSWORD}'"

    with clickhouse_ctl() as ctl, capture_logs() as messages:
        ctl.restore_udf(statement)

    assert_no_secrets(messages)


def test_restore_workload_entity_does_not_log_the_statement() -> None:
    """
    Workload entities come from the backup and are not validated by ch-backup.
    """
    statement = f"CREATE WORKLOAD production SETTINGS comment = '{PLAIN_TEXT_PASSWORD}'"

    with clickhouse_ctl() as ctl, capture_logs() as messages:
        ctl.restore_workload_entity(statement)

    assert_no_secrets(messages)


def test_create_table_does_not_log_the_statement() -> None:
    """
    Table engines such as MySQL keep the password in the create statement.
    """
    table = make_table("MySQL", MYSQL_TABLE_STATEMENT)

    with clickhouse_ctl() as ctl, capture_logs() as messages:
        ctl.create_table(table)

    assert_no_secrets(messages)


def test_decrypt_aes_ctr_does_not_log_the_key() -> None:
    """
    The decrypt query embeds the named collections encryption key.
    """
    with clickhouse_ctl() as ctl, capture_logs() as messages:
        ctl.decrypt_aes_ctr("aabb", ENCRYPTION_KEY_HEX, 128, "ccdd")

    assert_no_secrets(messages)


#
# The scenario end to end.
#


def test_named_collection_restore_sequence_writes_no_secrets() -> None:
    """
    Replay the query sequence from the scenario log and check the log is clean.

    The log contained, in order:
      1. CREATE NAMED COLLECTION <s3 collection>    (S3 credentials)
      2. DROP NAMED COLLECTION <mysql collection>
      3. CREATE NAMED COLLECTION <mysql collection> (plain text password)
    """
    with clickhouse_ctl() as ctl, capture_logs() as messages:
        ctl.restore_named_collection(NC_S3_STATEMENT)
        ctl.drop_named_collection(NC_MYSQL_NAME)
        ctl.restore_named_collection(NC_MYSQL_STATEMENT)

    assert_no_secrets(messages)


def test_drop_statement_stays_visible() -> None:
    """
    Guard against over-masking: DROP carries no secret and must stay readable.
    """
    with clickhouse_ctl() as ctl, capture_logs() as messages:
        ctl.drop_named_collection(NC_MYSQL_NAME)

    log = "".join(messages)
    assert "DROP NAMED COLLECTION" in log
    assert NC_MYSQL_NAME in log


@parametrize(
    {
        "id": "collection missing on the server",
        "args": {
            "nc_on_clickhouse": [],
            "expect_drop": False,
        },
    },
    {
        "id": "collection present but local .sql is gone",
        "args": {
            "nc_on_clickhouse": [NC_MYSQL_NAME],
            "expect_drop": True,
        },
    },
)
def test_named_collections_restore_always_goes_through_ch_ctl(
    nc_on_clickhouse: List[str], expect_drop: bool
) -> None:
    """
    Both branches of the restore loop must reach ClickhouseCTL.

    If a branch ever talks to the client directly, it bypasses the redaction
    and the leak comes back.
    """
    context = MagicMock()
    context.backup_meta.get_named_collections.return_value = [NC_MYSQL_NAME]
    context.ch_ctl.get_named_collections_query.return_value = nc_on_clickhouse
    context.backup_layout.get_named_collection_create_statement.return_value = (
        NC_MYSQL_STATEMENT
    )
    context.backup_layout.get_local_nc_create_statement.return_value = None

    NamedCollectionsBackup().restore(context)

    context.ch_ctl.restore_named_collection.assert_called_once_with(NC_MYSQL_STATEMENT)
    assert context.ch_ctl.drop_named_collection.called is expect_drop


#
# schema.py: string literals are masked for engines that can carry credentials.
#


def test_mask_sql_literals_hides_values_and_keeps_structure() -> None:
    """
    Masking is value-based, so no engine-specific denylist can be missed.
    """
    masked = mask_sql_literals(NC_S3_STATEMENT)

    for secret in (S3_ACCESS_KEY_ID, S3_SECRET_ACCESS_KEY):
        assert secret not in masked
    assert f"CREATE NAMED COLLECTION IF NOT EXISTS {NC_S3_NAME} AS" in masked
    assert "access_key_id = '[HIDDEN]'" in masked


def test_mask_sql_literals_handles_escaped_quotes() -> None:
    """
    A backslash-escaped quote inside a literal must not end the literal early.
    """
    masked = mask_sql_literals(
        "structure = 'Enum8(\\'AAA\\' = 1)', password = 'secret-value'"
    )

    assert "secret-value" not in masked
    assert "AAA" not in masked


def test_rewrite_table_schema_masks_external_engine() -> None:
    """
    A MySQL table must not put its password into the log.
    """
    table = make_table("MySQL", MYSQL_TABLE_STATEMENT)

    with capture_logs() as messages:
        rewrite_table_schema(table)

    assert_no_secrets(messages)


def test_rewrite_table_schema_masks_merge_tree_too() -> None:
    """
    Masking must not depend on the engine.

    A MergeTree table can still carry credentials: since 23.x the storage
    policy may be defined inline, in the SETTINGS clause of the table itself.
    Any engine-based fast path would let this one through.
    """
    statement = (
        "CREATE TABLE test_db.test_table (n Int32) ENGINE = MergeTree ORDER BY n"
        " SETTINGS disk = disk(type = s3, endpoint = 'https://s3.example.com/b/',"
        f" access_key_id = '{S3_ACCESS_KEY_ID}',"
        f" secret_access_key = '{S3_SECRET_ACCESS_KEY}')"
    )
    table = make_table("MergeTree", statement)

    with capture_logs() as messages:
        rewrite_table_schema(table)

    assert_no_secrets(messages)


def test_rewrite_table_schema_keeps_the_structure_readable() -> None:
    """
    Guard against over-masking: everything outside string literals must survive.
    """
    statement = (
        "CREATE TABLE test_db.test_table (partition_id Int32, n Int32)"
        " ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/test', '{replica}')"
        " PARTITION BY partition_id ORDER BY n"
    )
    table = make_table("ReplicatedMergeTree", statement)

    with capture_logs() as messages:
        rewrite_table_schema(table)

    log = "".join(messages)
    assert "CREATE TABLE test_db.test_table (partition_id Int32, n Int32)" in log
    assert "ENGINE = ReplicatedMergeTree" in log
    assert "PARTITION BY partition_id ORDER BY n" in log


def test_rewrite_table_schema_logs_the_rewritten_statement() -> None:
    """
    The second log line must show the result of the rewrite, not the input.

    Otherwise both lines are identical and the log stops explaining what changed.
    """
    statement = (
        "CREATE TABLE test_db.test_table (s String, n Int32)"
        " ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/test', '{replica}')"
        " ORDER BY n"
    )
    table = make_table("ReplicatedMergeTree", statement)

    with capture_logs() as messages:
        rewrite_table_schema(table, force_non_replicated_engine=True)

    before = next(m for m in messages if "Going to rewrite table schema" in m)
    after = next(m for m in messages if "Resulting table schema" in m)

    assert "ReplicatedMergeTree" in before
    assert "ReplicatedMergeTree" not in after


def test_rewrite_table_schema_masks_dictionary() -> None:
    """
    Dictionary credentials live in SOURCE(...), and Dictionary is not external.
    """
    statement = (
        "CREATE DICTIONARY test_db.test_dictionary (n1 UInt32, n2 UInt32)"
        " SOURCE(MYSQL(host 'db-host.example.com' user 'test_user'"
        f" password '{PLAIN_TEXT_PASSWORD}'))"
    )
    table = make_table("Dictionary", statement, name="test_dictionary")

    with capture_logs() as messages:
        rewrite_table_schema(table)

    assert_no_secrets(messages)


def test_no_secret_survives_repr_escaping() -> None:
    """
    schema.py logs with !r, so masking must happen before repr, not after.
    """
    masked = mask_sql_literals(MYSQL_TABLE_STATEMENT)

    assert PLAIN_TEXT_PASSWORD not in repr(masked)
    assert masked.endswith(
        "ENGINE = MySQL('[HIDDEN]', '[HIDDEN]', '[HIDDEN]', '[HIDDEN]', '[HIDDEN]')"
    )


#
# models.py: an unparsable database statement must not be echoed as is.
#


def make_database() -> Database:
    """
    Build a database with nothing parsed out of its create statement yet.
    """
    return Database(
        name="test_db",
        engine=None,
        metadata_path=None,
        uuid=None,
        engine_full=None,
    )


def test_set_engine_from_sql_masks_the_query_it_failed_to_parse() -> None:
    """
    The warning quotes the whole statement, which may carry credentials.
    """
    db_sql = f"CREATE DATABASE test_db COMMENT '{PLAIN_TEXT_PASSWORD}'"

    with capture_logs() as messages:
        make_database().set_engine_from_sql(db_sql)

    assert_no_secrets(messages)


def test_set_engine_from_sql_keeps_the_warning_useful() -> None:
    """
    Guard against over-masking: the warning must still say what failed to parse.
    """
    db_sql = f"CREATE DATABASE test_db COMMENT '{PLAIN_TEXT_PASSWORD}'"

    with capture_logs() as messages:
        make_database().set_engine_from_sql(db_sql)

    log = "".join(messages)
    assert "Failed to parse engine for database" in log
    assert "CREATE DATABASE test_db COMMENT '[HIDDEN]'" in log


def test_set_engine_from_sql_logs_nothing_when_it_parses() -> None:
    """
    A parsable statement is not logged at all, so nothing can leak from it.
    """
    db_sql = "CREATE DATABASE test_db ENGINE = Atomic"
    db = make_database()

    with capture_logs() as messages:
        db.set_engine_from_sql(db_sql)

    assert db.engine == "Atomic"
    assert not messages


#
# cli.py: --config-parameter values may carry credentials.
#


def _secrets_probe_body(_ctx: click.Context, _ch_backup: Any) -> None:
    """
    Do nothing: the probe only exists to drive the wrapper around it.
    """


_secrets_probe: click.Command = cli.command(name="secrets-probe")(_secrets_probe_body)


def run_secrets_probe(parent_params: dict) -> List[str]:
    """
    Invoke the probe command as click would, and return the captured log.
    """
    parent = click.Context(cli.cli)
    parent.params = parent_params

    ctx = click.Context(_secrets_probe, parent=parent, info_name="secrets-probe")
    ctx.obj = {"backup": None}

    with capture_logs() as messages:
        with ctx:
            _secrets_probe.invoke(ctx)

    return messages


def test_config_parameter_values_are_masked() -> None:
    """
    "--config-parameter clickhouse.clickhouse_password <secret>" reached the log.
    """
    messages = run_secrets_probe(
        {
            "config_parameters": (
                ("clickhouse.clickhouse_password", PLAIN_TEXT_PASSWORD),
                ("encryption.key", ENCRYPTION_KEY_HEX),
            )
        }
    )

    assert_no_secrets(messages)


def test_config_parameter_paths_stay_visible() -> None:
    """
    Guard against over-masking: which settings were overridden must stay in the log.
    """
    messages = run_secrets_probe(
        {
            "config_parameters": (
                ("clickhouse.clickhouse_password", PLAIN_TEXT_PASSWORD),
            )
        }
    )

    log = "".join(messages)
    assert "clickhouse.clickhouse_password" in log
    assert "[HIDDEN]" in log


def test_other_params_stay_visible() -> None:
    """
    Guard against over-masking: only config parameters carry secrets.

    The rest of the params is what makes the line worth logging at all.
    """
    messages = run_secrets_probe(
        {
            "config_parameters": (
                ("clickhouse.clickhouse_password", PLAIN_TEXT_PASSWORD),
            ),
            "host": "db-host.example.com",
            "schema_only": True,
        }
    )

    log = "".join(messages)
    assert "db-host.example.com" in log
    assert "'schema_only': True" in log


def test_no_config_parameters_is_not_a_crash() -> None:
    """
    The option is optional, and an empty tuple must not break the wrapper.
    """
    messages = run_secrets_probe(
        {"config_parameters": (), "host": "db-host.example.com"}
    )

    log = "".join(messages)
    assert "Executing command 'secrets-probe'" in log
    assert "db-host.example.com" in log
