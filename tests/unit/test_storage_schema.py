"""Storage rewrites must preserve all semantics except unavailable placement."""

# pylint: disable=protected-access

from contextlib import ExitStack
from unittest.mock import MagicMock, patch

import pytest
from kazoo.exceptions import NoNodeError

from ch_backup.clickhouse.control import ClickhouseCTL
from ch_backup.clickhouse.models import Database, Disk, Table
from ch_backup.clickhouse.schema import rewrite_table_schema
from ch_backup.clickhouse.storage_schema import (
    StoragePolicy,
    StorageSchemaError,
    adapt_storage_schema,
    remove_unavailable_move_ttls,
    replicated_engine_path,
)
from ch_backup.logic.storage_schema_restore import (
    StorageSchemaKeeperError,
    StorageSchemaRestore,
    restore_disk_name,
)
from ch_backup.logic.table import TableBackup

POLICIES = {
    "default": StoragePolicy("default", ("default",), ("default",), ("default",)),
    "local": StoragePolicy(
        "local", ("hot", "warm"), ("default", "hdd"), ("default", "hdd")
    ),
}


@pytest.mark.parametrize(
    "statement, expected",
    [
        (
            "CREATE TABLE _ (d DateTime) ENGINE = MergeTree ORDER BY d SETTINGS storage_policy = 's3', index_granularity = 8192",
            "CREATE TABLE _ (d DateTime) ENGINE = MergeTree ORDER BY d SETTINGS storage_policy = 'default', index_granularity = 8192",
        ),
        (
            "ATTACH TABLE _ (d DateTime) ENGINE = ReplicatedMergeTree('/t', '{replica}') ORDER BY d TTL d TO VOLUME 'cold' SETTINGS storage_policy = 's3'",
            "ATTACH TABLE _ (d DateTime) ENGINE = ReplicatedMergeTree('/t', '{replica}') ORDER BY d SETTINGS storage_policy = 'default'",
        ),
        (
            "CREATE TABLE _ (d DateTime TTL d + INTERVAL 1 DAY) ENGINE = MergeTree ORDER BY d TTL d TO DISK 's3', d + INTERVAL 2 DAY DELETE SETTINGS storage_policy = 's3'",
            "CREATE TABLE _ (d DateTime TTL d + INTERVAL 1 DAY) ENGINE = MergeTree ORDER BY d TTL d + INTERVAL 2 DAY DELETE SETTINGS storage_policy = 'default'",
        ),
        (
            "CREATE MATERIALIZED VIEW db.mv ENGINE = MergeTree ORDER BY d TTL d TO VOLUME 'cold' SETTINGS storage_policy = 's3' AS SELECT d, 'TO VOLUME cold, storage_policy' FROM db.src SETTINGS max_threads=1",
            "CREATE MATERIALIZED VIEW db.mv ENGINE = MergeTree ORDER BY d SETTINGS storage_policy = 'default' AS SELECT d, 'TO VOLUME cold, storage_policy' FROM db.src SETTINGS max_threads=1",
        ),
        (
            "CREATE TABLE _ (d DateTime, s String DEFAULT 'TTL d TO DISK \\'s3\\'') ENGINE = MergeTree ORDER BY d TTL d TO DISK 's3';",
            "CREATE TABLE _ (d DateTime, s String DEFAULT 'TTL d TO DISK \\'s3\\'') ENGINE = MergeTree ORDER BY d ;",
        ),
        (
            "CREATE TABLE _ (d DateTime, s String DEFAULT 'ENGINE = MergeTree SETTINGS storage_policy = \\'s3\\'') ENGINE = MergeTree ORDER BY d TTL if(d IN (1, 2), d, d + 1) TO DISK 's3' SETTINGS storage_policy = 's3' COMMENT 'TTL stays here'",
            "CREATE TABLE _ (d DateTime, s String DEFAULT 'ENGINE = MergeTree SETTINGS storage_policy = \\'s3\\'') ENGINE = MergeTree ORDER BY d SETTINGS storage_policy = 'default' COMMENT 'TTL stays here'",
        ),
        (
            "CREATE TABLE _ (d DateTime) ENGINE = MergeTree ORDER BY d SETTINGS `storage_policy` /* = 'local' */ = /* target */ 's3'",
            "CREATE TABLE _ (d DateTime) ENGINE = MergeTree ORDER BY d SETTINGS `storage_policy` /* = 'local' */ = /* target */ 'default'",
        ),
    ],
)
def test_storage_schema_rewrite(statement, expected):
    change = adapt_storage_schema(statement, POLICIES)
    assert change is not None
    assert change.statement == expected
    repeated = adapt_storage_schema(change.statement, POLICIES)
    assert repeated is not None
    assert repeated.statement == expected
    assert not repeated.changed


@pytest.mark.parametrize(
    "ttl, expected",
    [
        ("d TO DISK 's3'", ""),
        ("d TO VOLUME IF EXISTS 'cold'", ""),
        ("d TO DISK 'default', d TO VOLUME 'cold'", "d TO DISK 'default'"),
        ("d TO DISK 's3', d DELETE WHERE x IN (1, 2)", " d DELETE WHERE x IN (1, 2)"),
        (
            "d RECOMPRESS CODEC(ZSTD(3)), d TO VOLUME 'cold'",
            "d RECOMPRESS CODEC(ZSTD(3))",
        ),
        ("if(x IN (1, 2), d, d + 1) TO DISK 's3', d DELETE", " d DELETE"),
        ("if(x IN [1, 2], d, d + 1) TO DISK 's3', d DELETE", " d DELETE"),
        (
            "d GROUP BY x, y SET a = max(a), b = argMax(b, d), d + 2 TO DISK 's3'",
            "d GROUP BY x, y SET a = max(a), b = argMax(b, d)",
        ),
        (
            "d TO DISK 's3', d GROUP BY x, y SET a = max(a), b = argMax(b, d)",
            " d GROUP BY x, y SET a = max(a), b = argMax(b, d)",
        ),
        (
            "d DELETE WHERE s = 'TO DISK \\'s3\\', TTL', d TO DISK 's3'",
            "d DELETE WHERE s = 'TO DISK \\'s3\\', TTL'",
        ),
        ("d /* comma, quoted ' text */ TO /* x */ DISK 's3', d DELETE", " d DELETE"),
        ("d TO VOLUME 'hot', d TO DISK 'hdd'", "d TO VOLUME 'hot', d TO DISK 'hdd'"),
    ],
)
def test_remove_only_unavailable_move_rules(ttl, expected):
    rewritten, _ = remove_unavailable_move_ttls(ttl, POLICIES["local"])
    assert rewritten == expected


@pytest.mark.parametrize(
    "statement",
    [
        "CREATE TABLE _ (d DateTime) ENGINE = MergeTree ORDER BY d",
        "CREATE TABLE _ (d DateTime) ENGINE = MergeTree ORDER BY d TTL d TO VOLUME 'hot' SETTINGS storage_policy = 'local'",
        "CREATE TABLE _ (d DateTime) ENGINE = MergeTree ORDER BY d TTL d DELETE WHERE x = 'storage_policy = s3'",
        "CREATE TABLE _ (d DateTime) ENGINE = MergeTree ORDER BY d SETTINGS comment = 'storage_policy = \\'s3\\'', storage_policy = 'local'",
    ],
)
def test_noop_storage_schema_is_verbatim(statement):
    change = adapt_storage_schema(statement, POLICIES)
    assert change is not None
    assert change.statement == statement
    assert not change.changed


@pytest.mark.parametrize(
    "statement",
    [
        "CREATE VIEW v AS SELECT 'ENGINE = MergeTree TTL d TO DISK s3'",
        "CREATE MATERIALIZED VIEW v TO t AS SELECT * FROM src",
        "CREATE TABLE _ (s String) ENGINE = Log",
        "CREATE DICTIONARY d (s String) SOURCE(CLICKHOUSE(QUERY 'TTL TO DISK s3'))",
    ],
)
def test_non_mergetree_storage_is_ignored(statement):
    assert adapt_storage_schema(statement, POLICIES) is None


def test_implicit_policy_uses_server_default():
    statement = "CREATE TABLE _ (d DateTime) ENGINE = MergeTree ORDER BY d TTL d TO VOLUME 'warm'"
    existing = adapt_storage_schema(statement, POLICIES, "local")
    assert existing is not None
    assert not existing.changed
    change = adapt_storage_schema(statement, POLICIES, "missing")
    assert change is not None
    assert change.policy.name == "default"
    assert "storage_policy = 'default'" in change.statement
    assert "TTL" not in change.statement


def test_disk_on_server_but_not_in_selected_policy_is_removed():
    change = adapt_storage_schema(
        "CREATE TABLE _ (d DateTime) ENGINE = MergeTree ORDER BY d TTL d TO DISK 'hdd'",
        POLICIES,
    )
    assert change is not None
    assert change.removed_ttls == ("d TO DISK 'hdd'",)


def test_escaped_policy_and_disk_names():
    name = "a'b\\c"
    policy = StoragePolicy(name, ("v",), (name,), (name,))
    statement = r"CREATE TABLE _ (d DateTime) ENGINE = MergeTree ORDER BY d TTL d TO DISK 'a\'b\\c' SETTINGS `storage_policy` = 'a''b\\c'"
    change = adapt_storage_schema(statement, {name: policy})
    assert change is not None
    assert change.statement == statement
    assert not change.changed


@pytest.mark.parametrize(
    "engine, expected",
    [
        ("ReplicatedMergeTree()", None),
        ("ReplicatedMergeTree('/tables/t', '{replica}')", "/tables/t"),
        (r"ReplicatedMergeTree('/tables/it\'s', '{replica}')", "/tables/it's"),
        (
            "ReplicatedMergeTree /* x */ (/* path */ '/tables/t', '{replica}')",
            "/tables/t",
        ),
    ],
)
def test_replica_path_ignores_sql_inside_column_expressions(engine, expected):
    statement = (
        'CREATE TABLE _ (s String DEFAULT \'ReplicatedMergeTree("/wrong", "r")\') '
        f"ENGINE = {engine} ORDER BY s"
    )
    assert replicated_engine_path(statement) == expected


@pytest.mark.parametrize(
    "suffix",
    [
        "TTL d TO DISK unknown",
        "TTL d TO DISK 'unterminated",
        "TTL f(d TO DISK 's3'",
        "SETTINGS storage_policy = getSetting('x')",
    ],
)
def test_ambiguous_storage_schema_fails(suffix):
    with pytest.raises(StorageSchemaError):
        adapt_storage_schema(
            "CREATE TABLE _ (d DateTime) ENGINE = MergeTree ORDER BY d " + suffix,
            POLICIES,
        )


def _table():
    return Table(
        "db",
        "t",
        "ReplicatedMergeTree",
        [],
        [],
        "",
        "CREATE TABLE db.t (d DateTime) ENGINE = ReplicatedMergeTree('/tables/{shard}/t', '{replica}') ORDER BY d TTL d TO VOLUME 'cold', d + 10 DELETE SETTINGS storage_policy = 's3'",
        "uuid",
    )


def _restore(metadata=b"ttl: d TO VOLUME 'cold', d + 10 DELETE\n"):
    # Model a target with local policies and an existing shared Keeper schema.
    # Vary metadata to represent either the first restore or a later replica.
    context = MagicMock()
    context.ch_ctl.get_storage_policies.return_value = POLICIES
    context.ch_ctl.get_macros.return_value = {"shard": "s", "replica": "r"}
    context.ch_config.config = {"zookeeper": {"root": "/root"}}
    context.zk_config = {"hosts": ["zk"]}
    context.zk_ctl.zk_root_path = "/root"
    context.lock_conf = {"lock_timeout": 1}
    client = context.zk_ctl.zk_client.__enter__.return_value
    client.get.return_value = (metadata, MagicMock())
    client.get_children.return_value = []
    restore = StorageSchemaRestore(context)
    table = _table()
    restore.adapt(table)
    return restore, table, client


def test_compatible_keeper_lock_is_held_until_creation():
    restore, table, client = _restore(metadata=b"ttl: d + 10 DELETE\n")
    with ExitStack() as stack:
        restore.lock_tables([table], stack)
        client.Lock.return_value.release.assert_not_called()
    client.Lock.return_value.release.assert_called_once()
    client.delete.assert_not_called()


def test_keeper_absent_needs_no_cleanup():
    restore, table, client = _restore()
    client.get.side_effect = NoNodeError
    with ExitStack() as stack:
        restore.lock_tables([table], stack)
    client.delete.assert_not_called()


def test_incompatible_keeper_fails_with_table_and_path_without_deleting_metadata():
    restore, table, client = _restore()
    with (
        ExitStack() as stack,
        pytest.raises(
            StorageSchemaKeeperError, match="db.t.*?/root/tables/s/t"
        ) as error,
    ):
        restore.lock_tables([table], stack)
    assert "Existing replica metadata was not changed" in str(error.value)
    client.delete.assert_not_called()
    client.Lock.return_value.release.assert_called_once()


@pytest.mark.parametrize("policy", ["default", "s3"])
def test_unchanged_ttl_needs_no_direct_keeper_configuration(policy):
    restore, table, client = _restore()
    restore.context.zk_config = {"hosts": []}
    table.create_statement = (
        "CREATE TABLE db.t (d DateTime) ENGINE = ReplicatedMergeTree('/t', 'r') "
        f"ORDER BY d SETTINGS storage_policy = '{policy}'"
    )
    restore.adapt(table)
    with ExitStack() as stack:
        restore.lock_tables([table], stack)
    client.get.assert_not_called()
    client.Lock.assert_not_called()
    restore.context.ch_ctl.get_macros.assert_not_called()


def test_converted_table_never_connects_to_keeper():
    restore, table, client = _restore()
    table.engine = "MergeTree"
    with ExitStack() as stack:
        restore.lock_tables([table], stack)
    client.get.assert_not_called()
    client.Lock.assert_not_called()


def test_empty_server_settings_sections_use_default_policy():
    restore, table, _ = _restore()
    restore.ch_config = {"merge_tree": None, "replicated_merge_tree": None}
    table.create_statement = (
        "CREATE TABLE db.t (d DateTime) ENGINE = ReplicatedMergeTree() ORDER BY d"
    )
    restore.adapt(table)
    assert restore.changes[("db", "t")].policy.name == "default"


@pytest.mark.parametrize(
    "source, expected", [("hdd", "default"), ("default", "default")]
)
def test_local_disk_remapping(source, expected):
    table = _table()
    assert restore_disk_name(table, source, {("db", "t"): ("default",)}) == expected
    assert restore_disk_name(table, source, None) == source
    with pytest.raises(StorageSchemaError, match="No local disk"):
        restore_disk_name(table, source, {("db", "t"): ()})


def test_storage_policies_snapshot_uses_volume_order():
    ctl = object.__new__(ClickhouseCTL)
    ctl._ch_client = MagicMock()
    ctl._ch_client.query.return_value = {
        "data": [
            {"policy_name": "p", "volume_name": "hot", "disks": ["hdd", "default"]},
            {"policy_name": "p", "volume_name": "cold", "disks": ["s3"]},
        ]
    }
    with patch.object(
        ctl,
        "get_disks",
        return_value={
            "default": Disk("default", "/data/", "local"),
            "hdd": Disk("hdd", "/hdd/", "local"),
            "s3": Disk("s3", "/s3/", "s3"),
        },
    ):
        assert ctl.get_storage_policies()["p"] == StoragePolicy(
            "p", ("hot", "cold"), ("hdd", "default", "s3"), ("hdd", "default")
        )
    assert ctl._ch_client.query.call_args.args[0].endswith("FORMAT JSON")


@pytest.mark.parametrize("replica_filter", ["disabled", "replica-1", None])
@pytest.mark.parametrize("keep_going", [False, True])
def test_keeper_conflict_precedes_local_drops_and_every_cleanup_mode(
    replica_filter, keep_going
):
    restore, _, client = _restore()
    context = restore.context
    cleaner = None if replica_filter == "disabled" else MagicMock()
    if cleaner is not None:
        cleaner._replica_to_drop = replica_filter
    context.backup_meta.get_tables.return_value = [MagicMock(database="db", name="t")]
    manager = TableBackup()
    with (
        patch.object(manager, "_filter_duplicate_uuid", return_value=([], 0)),
        patch.object(manager, "_get_tables_from_meta", return_value=([_table()], 0)),
        patch.object(manager, "_rewrite_table_schema"),
        patch.object(manager, "_preprocess_tables_to_restore") as preprocess,
        patch.object(manager, "_restore_tables") as create_tables,
        pytest.raises(StorageSchemaKeeperError, match="Keeper metadata"),
    ):
        manager.restore(
            context,
            {"db": Database("db", "Atomic", None, None, None)},
            schema_only=True,
            tables=[],
            metadata_cleaner=cleaner,
            cloud_storage_source_bucket=None,
            cloud_storage_source_path=None,
            cloud_storage_source_endpoint=None,
            skip_cloud_storage=True,
            restore_detached_parts=False,
            keep_going=keep_going,
            restore_tables_in_replicated_database=False,
        )
    preprocess.assert_not_called()
    create_tables.assert_not_called()
    client.delete.assert_not_called()
    if cleaner is not None:
        cleaner.clean_tables_metadata.assert_not_called()


def test_default_replicated_engine_converts_before_storage_adaptation():
    table = _table()
    table.create_statement = "CREATE TABLE db.t (d DateTime) ENGINE = ReplicatedMergeTree() ORDER BY d TTL d TO DISK 's3'"
    rewrite_table_schema(table, force_non_replicated_engine=True)
    assert not table.is_replicated()
    assert "ReplicatedMergeTree" not in table.create_statement
    change = adapt_storage_schema(table.create_statement, POLICIES)
    assert change is not None
    assert change.removed_ttls == ("d TO DISK 's3'",)


@pytest.mark.parametrize("failure_stage", ["adapt", "preprocess", "create"])
def test_keep_going_restores_independent_schema_then_reports_error(failure_stage):
    context = MagicMock()
    context.ch_ctl.get_storage_policies.return_value = POLICIES
    context.ch_config.config = {}
    bad, good = _table(), _table()
    for table, name in [(bad, "bad"), (good, "good")]:
        table.engine = "MergeTree"
        table.name = name
        table.create_statement = (
            "CREATE TABLE _ (d DateTime) ENGINE = MergeTree ORDER BY d"
        )
    if failure_stage == "adapt":
        bad.create_statement += " TTL d TO DISK dynamic_disk"
    context.backup_meta.get_tables.return_value = [
        MagicMock(database="db", name="bad"),
        MagicMock(database="db", name="good"),
    ]
    manager = TableBackup()
    # Keep the real restore orchestration, but isolate storage adaptation from
    # unrelated I/O. Restore the good schema before reporting the saved error.
    with (
        patch.object(manager, "_filter_duplicate_uuid", return_value=([], 0)),
        patch.object(manager, "_get_tables_from_meta", return_value=([bad, good], 0)),
        patch.object(manager, "_rewrite_table_schema"),
        patch.object(
            manager,
            "_preprocess_tables_to_restore",
            side_effect=lambda _ctx, _dbs, tables, *_args, **_kwargs: (
                [good] if failure_stage == "preprocess" else tables,
                1 if failure_stage == "preprocess" else 0,
                {("db", "bad")} if failure_stage == "preprocess" else set(),
            ),
        ),
        patch.object(
            manager,
            "_restore_tables",
            return_value=[bad] if failure_stage == "create" else [],
        ) as restore_tables,
        pytest.raises(StorageSchemaError, match="db.bad"),
    ):
        manager.restore(
            context,
            {"db": Database("db", "Atomic", None, None, None)},
            schema_only=True,
            tables=[],
            metadata_cleaner=None,
            cloud_storage_source_bucket=None,
            cloud_storage_source_path=None,
            cloud_storage_source_endpoint=None,
            skip_cloud_storage=True,
            restore_detached_parts=False,
            keep_going=True,
            restore_tables_in_replicated_database=False,
        )
    assert restore_tables.call_args.args[2] == (
        [bad, good] if failure_stage == "create" else [good]
    )


@pytest.mark.parametrize(
    "path", ["/", "/tables/{unknown}/t", "aux:/tables/t", "/ch-backup"]
)
def test_keeper_invalid_path_never_acquires_lock_or_mutates(path):
    restore, table, client = _restore()
    table.create_statement = f"CREATE TABLE db.t (d DateTime) ENGINE = ReplicatedMergeTree('{path}', 'r') ORDER BY d"
    with ExitStack() as stack, pytest.raises(StorageSchemaKeeperError):
        restore.lock_tables([table], stack)
    client.Lock.assert_not_called()
    client.delete.assert_not_called()


@pytest.mark.parametrize(
    "column",
    [
        "ttl",
        "comment",
        "empty",
        "settings",
        "partition",
        "order",
        "primary",
        "sample",
        "unique",
        "as",
    ],
)
@pytest.mark.parametrize("multiline", [False, True])
def test_storage_clause_keywords_are_valid_column_references(column, multiline):
    separator = "\n" if multiline else " "
    prefix = (
        f"CREATE TABLE _ (`{column}` DateTime) ENGINE = MergeTree ORDER BY {column}"
    )
    ttl = f"{column} + toIntervalDay(1) TO VOLUME 'cold', {column} + toIntervalDay(100) DELETE"
    statement = separator.join([prefix, f"TTL {ttl}", "SETTINGS storage_policy = 's3'"])
    change = adapt_storage_schema(statement, POLICIES)
    assert change is not None
    assert f"ORDER BY {column}" in change.statement
    assert f"TTL {column} + toIntervalDay(100) DELETE" in change.statement
    assert "storage_policy = 'default'" in change.statement
    assert "TO VOLUME" not in change.statement
    repeated = adapt_storage_schema(change.statement, POLICIES)
    assert repeated is not None
    assert not repeated.changed
    assert repeated.statement == change.statement


@pytest.mark.parametrize(
    "expression",
    [
        "ttl + 1",
        "tuple(ttl, settings)",
        "comment",
        "empty(s)",
        "x.settings",
        "if(empty(s), ttl, settings)",
    ],
)
def test_expression_operands_do_not_end_storage_clauses(expression):
    statement = (
        "CREATE TABLE _ (ttl DateTime, settings DateTime, comment DateTime, s String, x Tuple(settings UInt64)) "
        f"ENGINE = MergeTree ORDER BY {expression} "
        "TTL ttl TO VOLUME 'cold', settings DELETE WHERE comment < ttl "
        "SETTINGS storage_policy = 's3' COMMENT 'saved comment'"
    )
    change = adapt_storage_schema(statement, POLICIES)
    assert change is not None
    assert f"ORDER BY {expression}" in change.statement
    assert "TTL settings DELETE WHERE comment < ttl" in change.statement
    assert change.statement.endswith("COMMENT 'saved comment'")
    assert "TO VOLUME" not in change.statement
