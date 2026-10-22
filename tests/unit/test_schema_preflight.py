"""Server-side schema checks must finish before destructive restore operations."""

# pylint: disable=protected-access

from contextlib import ExitStack
from unittest.mock import MagicMock, patch

import pytest

from ch_backup.backup.metadata import TableMetadata
from ch_backup.backup.sources import BackupSources
from ch_backup.ch_backup import ClickhouseBackup
from ch_backup.clickhouse.client import ClickhouseError
from ch_backup.clickhouse.models import Database, Disk, Table
from ch_backup.clickhouse.schema_probe import (
    can_create_probe,
    probe_database,
    probe_table,
)
from ch_backup.clickhouse.storage_schema import StoragePolicy
from ch_backup.exceptions import ClickhouseBackupError
from ch_backup.logic.database import DatabaseBackup
from ch_backup.logic.schema_preflight import (
    validate_database_schemas,
    validate_table_schemas,
)
from ch_backup.logic.table import TableBackup


@pytest.mark.parametrize("keep_going", [False, True])
def test_preflight_failure_precedes_database_restore_without_cloud_adaptation(
    keep_going,
):
    backup = object.__new__(ClickhouseBackup)
    backup._context = MagicMock()
    backup._config = MagicMock()
    backup._context.backup_meta.get_database.return_value = Database(
        "db", "Atomic", None, None, None
    )
    backup._table_backup_manager = MagicMock()
    backup._database_backup_manager = MagicMock()
    backup._table_backup_manager.prepare_restore.side_effect = ClickhouseBackupError(
        "Schema preflight failed"
    )
    with (
        patch("ch_backup.ch_backup.wait_sync_replicated_databases", return_value={}),
        pytest.raises(ClickhouseBackupError, match="Schema preflight failed"),
    ):
        backup._restore(
            BackupSources.for_restore(schema=True), ["db"], [], keep_going=keep_going
        )
    backup._database_backup_manager.restore.assert_not_called()
    backup._table_backup_manager.restore.assert_not_called()


def _table(name="t", engine="MergeTree", statement=None, database="db"):
    return Table(
        database,
        name,
        engine,
        [],
        [],
        "",
        statement
        or f"CREATE TABLE {database}.{name} (x UInt64) ENGINE={engine} ORDER BY x",
        None,
    )


def _context():
    context = MagicMock()
    context.ch_config.config = {}
    context.ch_ctl.get_disks.return_value = {
        "default": Disk("default", "/data/", "local"),
        "s3": Disk("s3", "/s3/", "s3"),
    }
    context.ch_ctl.get_storage_policies.return_value = {
        "default": StoragePolicy("default", ("main",), ("default",), ("default",)),
        "s3": StoragePolicy("s3", ("main",), ("s3",), ()),
    }
    context.ch_ctl.explain_schema.return_value = (
        " Storage definition (children 1)\n  Function MergeTree"
    )
    return context


@pytest.mark.parametrize(
    "name,quoted",
    [
        (r"a\b", r"`a\\b`"),
        (r"a\` (x UInt8) ENGINE=Memory -- ", r"`a\\\` (x UInt8) ENGINE=Memory -- `"),
    ],
)
def test_probe_quotes_backslashes_in_metadata_names(name, quoted):
    table = _table(
        name=name, statement="CREATE TABLE db.t (x UInt64) ENGINE=MergeTree ORDER BY x"
    )
    probe = probe_table(table, {"db": "scratch"}, {("db", name)})
    assert (
        probe.create_statement
        == f"CREATE TABLE `scratch`.{quoted} (x UInt64) ENGINE=MergeTree ORDER BY x"
    )


@pytest.mark.parametrize(
    "setting",
    [
        "storage_policy='s3'",
        "disk='s3'",
        "disk=disk(type='s3', endpoint='https://example.invalid/')",
    ],
)
def test_remote_storage_receives_only_syntax_checks(setting):
    context = _context()
    table = _table(
        statement=f"CREATE TABLE db.t (x UInt64) ENGINE=MergeTree ORDER BY x SETTINGS {setting}"
    )
    validate_table_schemas(context, [table])
    context.ch_ctl.explain_schema.assert_called_once_with(table.create_statement)
    context.ch_ctl.create_schema_probe_database.assert_not_called()
    context.ch_ctl.create_schema_probe.assert_not_called()


def test_ordinary_preflight_does_not_rewrite_the_original_restore_objects():
    context = _context()
    context.config = {"force_non_replicated": True, "override_replica_name": None}
    table = _table(
        engine="ReplicatedMergeTree",
        statement="ATTACH TABLE _ (x UInt64) ENGINE = ReplicatedMergeTree('/path', 'r') ORDER BY x",
    )
    metadata = TableMetadata("db", "t", table.engine, None)
    context.backup_meta.get_tables.return_value = [metadata]
    manager = TableBackup()
    with (
        ExitStack() as stack,
        patch.object(manager, "_get_tables_from_meta", return_value=([table], 0)),
        patch("ch_backup.logic.table.validate_table_schemas") as validate,
    ):
        prepared = manager.prepare_restore(
            context,
            {"db": Database("db", "Atomic", None, None, None)},
            [],
            False,
            False,
            False,
            stack,
        )
        checked = validate.call_args.args[1][0]
    assert checked is not table
    assert checked.engine == "MergeTree"
    assert "Replicated" not in checked.create_statement
    assert table.engine == "ReplicatedMergeTree"
    assert table.create_statement.startswith("ATTACH TABLE _")
    assert prepared.tables == [table]


@pytest.mark.parametrize(
    "engine",
    [
        "MergeTree",
        "Memory",
        "Log",
        "TinyLog",
        "StripeLog",
        "Null",
        "Set",
        "Join",
        "View",
        "MaterializedView",
    ],
)
def test_local_engines_are_created(engine):
    assert can_create_probe(
        _table(engine=engine),
        f" Storage definition (children 1)\n  Function {engine}",
        {"db.t"},
    )


@pytest.mark.parametrize("defaults", [None, {}])
def test_empty_replicated_settings_use_merge_tree_defaults(defaults):
    context = _context()
    context.ch_config.config = {"replicated_merge_tree": defaults}
    validate_table_schemas(context, [_table(engine="ReplicatedMergeTree")])
    probe = context.ch_ctl.create_schema_probe.call_args.args[0]
    assert "ENGINE=MergeTree ORDER BY x" in probe.create_statement
    assert "storage_policy" not in probe.create_statement


@pytest.mark.parametrize("engine", ["MergeTree", "MaterializedView"])
def test_actual_external_engine_overrides_metadata(engine):
    assert not can_create_probe(
        _table(engine=engine),
        " Storage definition (children 1)\n  Function Kafka",
        {"db.t"},
    )


@pytest.mark.parametrize(
    "engine",
    [
        "Kafka",
        "RabbitMQ",
        "S3",
        "S3Queue",
        "AzureQueue",
        "Distributed",
        "Dictionary",
        "MySQL",
        "PostgreSQL",
        "Buffer",
        "LiveView",
        "UnknownEngine",
    ],
)
def test_external_or_active_engines_are_only_parsed(engine):
    context = _context()
    table = _table(engine=engine)
    validate_table_schemas(context, [table])
    context.ch_ctl.explain_schema.assert_called_once_with(table.create_statement)
    context.ch_ctl.create_schema_probe.assert_not_called()
    context.ch_ctl.create_schema_probe_database.assert_not_called()


@pytest.mark.parametrize(
    "ast",
    [
        " TableExpression (children 1)\n  Function remote (children 1)",
        " Function dictGet (children 1)",
        " TableIdentifier db.missing",
        " TableIdentifier db.t (alias db)",
    ],
)
def test_views_cannot_read_outside_disposable_schemas(ast):
    assert not can_create_probe(_table(engine="View"), ast, {"db.t"})


def test_materialized_view_target_must_be_isolated():
    table = _table(
        engine="MaterializedView",
        statement="CREATE MATERIALIZED VIEW db.t TO db.destination AS SELECT x FROM db.src",
    )
    assert not can_create_probe(table, " TableIdentifier db.src", {"db.src", "db.t"})
    assert can_create_probe(
        table, " TableIdentifier db.src", {"db.src", "db.t", "db.destination"}
    )


def test_external_dependencies_are_transitive():
    context = _context()
    context.ch_ctl.explain_schema.side_effect = [
        "CreateQuery",
        " TableIdentifier db.external",
        " TableIdentifier db.v",
    ]
    tables = [_table("external", "Kafka"), _table("v", "View"), _table("v2", "View")]
    validate_table_schemas(context, tables)
    assert context.ch_ctl.explain_schema.call_count == 3
    context.ch_ctl.create_schema_probe.assert_not_called()


def test_user_defined_functions_are_not_executed_by_preflight():
    table = _table(
        statement="CREATE TABLE db.t (x UInt64 DEFAULT custom_external()) ENGINE = MergeTree ORDER BY x"
    )
    assert not can_create_probe(
        table,
        " Storage definition (children 1)\n  Function MergeTree\n Function custom_external (children 1)",
        {"db.t"},
        {"custom_external"},
    )


def test_probe_changes_only_identities_and_preserves_literals():
    sql = "CREATE MATERIALIZED VIEW `d``b`.`v` UUID '11111111-1111-1111-1111-111111111111' TO INNER UUID '22222222-2222-2222-2222-222222222222' (x UInt64, s String) ENGINE=MergeTree ORDER BY x AS SELECT x, 'd`b.src' AS s FROM `d``b`.`src` /* d`b.src */"
    table = _table("v", "MaterializedView", sql, "d`b")
    probe = probe_table(table, {"d`b": "scratch"}, {("d`b", "v"), ("d`b", "src")})
    assert probe.create_statement.startswith(
        "CREATE MATERIALIZED VIEW `scratch`.`v` UUID '"
    )
    assert "11111111" not in probe.create_statement
    assert "22222222" not in probe.create_statement
    assert "'d`b.src' AS s FROM `scratch`.`src` /* d`b.src */" in probe.create_statement
    assert table.create_statement == sql
    assert table.database == "d`b"


@pytest.mark.parametrize(
    "arguments,expected",
    [
        ("", "MergeTree ORDER BY"),
        ("()", "MergeTree ORDER BY"),
        ("('/path', '{replica}')", "MergeTree ORDER BY"),
        (r"('/a\'b','r\'r')", "MergeTree ORDER BY"),
    ],
)
def test_replicated_probe_never_joins_keeper(arguments, expected):
    table = _table(
        engine="ReplicatedMergeTree",
        statement=f"ATTACH TABLE db.t (x UInt64) ENGINE=ReplicatedMergeTree{arguments} ORDER BY x",
    )
    probe = probe_table(table, {"db": "scratch"}, {("db", "t")}, "local")
    assert expected in probe.create_statement
    assert "Replicated" not in probe.create_statement
    assert "storage_policy = 'local'" in probe.create_statement
    assert "/path" not in probe.create_statement


def test_replicated_parameters_and_explicit_policy_are_preserved():
    table = _table(
        engine="ReplicatedReplacingMergeTree",
        statement="CREATE TABLE db.t (x UInt64) ENGINE=ReplicatedReplacingMergeTree('/path','r',x) ORDER BY x SETTINGS storage_policy = 'explicit'",
    )
    probe = probe_table(table, {"db": "scratch"}, {("db", "t")}, "local")
    assert "ReplacingMergeTree(x)" in probe.create_statement
    assert "storage_policy = 'explicit'" in probe.create_statement
    assert "'local'" not in probe.create_statement


def test_replicated_default_policy_is_a_string_literal():
    table = _table(
        engine="ReplicatedMergeTree",
        statement="CREATE TABLE db.t (x UInt64) ENGINE=ReplicatedMergeTree ORDER BY x",
    )
    probe = probe_table(table, {"db": "scratch"}, {("db", "t")}, "a\\b'c")
    assert r"storage_policy = 'a\\b\'c'" in probe.create_statement


def test_database_probe_replaces_name_and_uuid():
    sql = "ATTACH DATABASE `d``b` UUID '11111111-1111-1111-1111-111111111111' ENGINE = Atomic COMMENT 'keep'"
    probe = probe_database(sql, "scratch")
    assert probe.startswith("CREATE DATABASE `scratch` UUID '")
    assert "11111111" not in probe
    assert probe.endswith("ENGINE = Atomic COMMENT 'keep'")


def test_invalid_sql_is_fatal_before_scratch_database_creation():
    context = _context()
    context.ch_ctl.explain_schema.side_effect = ClickhouseError("bad SQL 'secret'")
    with pytest.raises(ClickhouseBackupError, match="db.t") as error:
        validate_table_schemas(context, [_table()])
    assert "secret" not in str(error.value)
    assert error.value.__cause__ is None
    context.ch_ctl.create_schema_probe_database.assert_not_called()


def test_cleanup_runs_after_create_error_and_does_not_mask_it():
    context = _context()
    context.ch_ctl.create_schema_probe.side_effect = ClickhouseError("invalid storage")
    context.ch_ctl.drop_schema_probe_database.side_effect = ClickhouseError(
        "cleanup error"
    )
    with pytest.raises(ClickhouseBackupError, match="invalid storage"):
        validate_table_schemas(context, [_table()])
    name = context.ch_ctl.create_schema_probe_database.call_args.args[0]
    context.ch_ctl.drop_schema_probe_database.assert_called_once_with(name)


def test_cleanup_failure_aborts_before_target_restore():
    context = _context()
    context.ch_ctl.drop_schema_probe_database.side_effect = ClickhouseError(
        "cleanup error"
    )
    with pytest.raises(
        ClickhouseBackupError, match="Cannot remove schema preflight databases"
    ):
        validate_table_schemas(context, [_table()])


def test_database_collision_never_drops_an_unowned_database():
    context = _context()
    context.ch_ctl.create_schema_probe_database.side_effect = ClickhouseError(
        "already exists"
    )
    with pytest.raises(ClickhouseError, match="already exists"):
        validate_table_schemas(context, [_table()])
    context.ch_ctl.drop_schema_probe_database.assert_not_called()


def test_dependent_views_are_retried_after_tables():
    context = _context()
    context.ch_ctl.explain_schema.side_effect = [
        " TableIdentifier db.t",
        " Storage definition (children 1)\n  Function MergeTree",
    ]
    context.ch_ctl.create_schema_probe.side_effect = [
        ClickhouseError("missing dependency"),
        None,
        None,
    ]
    validate_table_schemas(context, [_table("v", "View"), _table()])
    assert [
        call.args[0].name for call in context.ch_ctl.create_schema_probe.call_args_list
    ] == ["v", "t", "v"]
    context.ch_ctl.drop_schema_probe_database.assert_called_once()


@pytest.mark.parametrize("keep_going", [False, True])
def test_invalid_database_definition_never_drops_existing_database(keep_going):
    context = _context()
    context.ch_ctl.explain_schema.return_value = (
        " Storage definition (children 1)\n  Function Atomic"
    )
    context.config = {"force_non_replicated": False, "override_replica_name": None}
    context.backup_layout.get_database_create_statements.return_value = [
        ("db", "CREATE DATABASE db ENGINE = Atomic")
    ]
    context.ch_ctl.get_databases.return_value = [
        Database("db", "Ordinary", None, None, None)
    ]
    context.ch_ctl.create_schema_probe_database.side_effect = ClickhouseError(
        "invalid database setting"
    )
    with pytest.raises(ClickhouseBackupError, match="database db"):
        DatabaseBackup.restore(
            context,
            {"db": Database("db", "Atomic", None, None, None)},
            keep_going,
            None,
        )
    context.ch_ctl.drop_database_if_exists.assert_not_called()


@pytest.mark.parametrize("keep_going", [False, True])
def test_incomplete_preflight_never_drops_a_database(keep_going):
    context = _context()
    context.ch_ctl.get_databases.return_value = [
        Database("db", "Ordinary", None, None, None)
    ]

    def restore():
        return DatabaseBackup.restore(
            context,
            {"db": Database("db", "Atomic", None, None, None)},
            keep_going,
            None,
            incomplete_table_databases={"db"},
        )

    if keep_going:
        result = restore()
        assert result.skipped == {"db"}
        assert len(result.restored) == 0
    else:
        with pytest.raises(
            ClickhouseBackupError, match="incomplete table schema preflight"
        ):
            restore()
    context.ch_ctl.drop_database_if_exists.assert_not_called()


@pytest.mark.parametrize("engine", ["MergeTree", "ReplicatedMergeTree"])
@pytest.mark.parametrize("setting", ["disk", "storage_policy"])
def test_remote_engine_defaults_are_syntax_only(engine, setting):
    context = _context()
    context.ch_config.config = {"merge_tree": {setting: "s3"}}
    validate_table_schemas(context, [_table(engine=engine)])
    context.ch_ctl.create_schema_probe.assert_not_called()


def test_remote_replicated_override_and_dependent_views_are_syntax_only():
    context = _context()
    context.ch_config.config = {"replicated_merge_tree": {"storage_policy": "s3"}}
    context.ch_ctl.explain_schema.side_effect = [
        " Storage definition\n  Function ReplicatedMergeTree",
        " SelectWithUnionQuery\n  TableIdentifier db.t",
    ]
    validate_table_schemas(
        context,
        [
            _table(engine="ReplicatedMergeTree"),
            _table("v", "View", "CREATE VIEW db.v AS SELECT * FROM db.t"),
        ],
    )
    context.ch_ctl.create_schema_probe_database.assert_not_called()


@pytest.mark.parametrize("setting", ["disk", "storage_policy"])
def test_unknown_static_storage_is_fatal_before_any_probe(setting):
    context = _context()
    with pytest.raises(ClickhouseBackupError, match="unknown"):
        validate_table_schemas(
            context,
            [
                _table(
                    statement=f"CREATE TABLE db.t (x UInt64) ENGINE=MergeTree ORDER BY x SETTINGS {setting}='absent'"
                )
            ],
        )
    context.ch_ctl.create_schema_probe_database.assert_not_called()


def test_mixed_policy_is_not_local_even_when_its_first_disk_is_local():
    context = _context()
    context.ch_ctl.get_storage_policies.return_value["default"] = StoragePolicy(
        "default", ("main",), ("default", "s3"), ("default",)
    )
    validate_table_schemas(context, [_table()])
    context.ch_ctl.create_schema_probe.assert_not_called()


def test_explicit_local_policy_overrides_remote_default():
    context = _context()
    context.ch_config.config = {"merge_tree": {"storage_policy": "s3"}}
    validate_table_schemas(
        context,
        [
            _table(
                statement="CREATE TABLE db.t (x UInt64) ENGINE=MergeTree ORDER BY x SETTINGS storage_policy='default'"
            )
        ],
    )
    assert len(context.ch_ctl.create_schema_probe.call_args_list) == 1


def test_replicated_local_disk_override_survives_engine_conversion():
    context = _context()
    context.ch_config.config = {
        "merge_tree": {"disk": "s3"},
        "replicated_merge_tree": {"disk": "default"},
    }
    validate_table_schemas(context, [_table(engine="ReplicatedMergeTree")])
    probe = context.ch_ctl.create_schema_probe.call_args.args[0]
    assert (
        "ENGINE=MergeTree ORDER BY x SETTINGS disk = 'default'"
        in probe.create_statement
    )


@pytest.mark.parametrize(
    "statement,local",
    [
        (
            "CREATE TABLE db.t (x UInt64, c String DEFAULT 'disk=evil, storage_policy=s3') ENGINE=MergeTree ORDER BY x",
            True,
        ),
        (
            "CREATE TABLE db.t (x UInt64) ENGINE=MergeTree ORDER BY x SETTINGS `storage_policy` /* policy */ = 's3' COMMENT 'disk=default'",
            False,
        ),
        (
            "CREATE TABLE db.t (x UInt64) ENGINE=MergeTree ORDER BY x SETTINGS disk='default', index_granularity=8192",
            True,
        ),
        (
            "CREATE TABLE db.t (x UInt64) ENGINE=MergeTree ORDER BY x SETTINGS disk=disk(type='cache', disk=disk(type='s3', endpoint='https://example.invalid/'))",
            False,
        ),
        (
            "CREATE MATERIALIZED VIEW db.t ENGINE=MergeTree ORDER BY x SETTINGS storage_policy='s3' AS SELECT 1 AS x",
            False,
        ),
    ],
)
def test_storage_classification_uses_only_the_engine_settings(statement, local):
    context = _context()
    table = _table(
        statement=statement,
        engine="MaterializedView" if "VIEW" in statement else "MergeTree",
    )
    validate_table_schemas(context, [table])
    assert context.ch_ctl.create_schema_probe.call_count == int(local)


def test_incomplete_database_does_not_block_independent_replacement():
    context = _context()
    context.ch_ctl.get_databases.return_value = [
        Database(name, "Ordinary", None, None, None) for name in ("bad", "good")
    ]
    databases = {
        name: Database(name, "Atomic", None, None, None) for name in ("bad", "good")
    }
    with patch(
        "ch_backup.logic.database.validate_database_schemas",
        return_value={"good": "CREATE DATABASE good ENGINE=Atomic"},
    ) as validate:
        result = DatabaseBackup.restore(
            context, databases, True, None, incomplete_table_databases={"bad"}
        )
    assert result.skipped == {"bad"}
    assert result.restored == [databases["good"]]
    assert set(validate.call_args.args[1]) == {"good"}
    context.ch_ctl.drop_database_if_exists.assert_called_once_with("good")
    context.ch_ctl.restore_database.assert_called_once_with(
        "CREATE DATABASE good ENGINE=Atomic"
    )
    assert set(databases) == {"bad", "good"}


@pytest.mark.parametrize(
    "failure", ["missing_schema", "duplicate_uuid", "missing_requested"]
)
def test_preparation_tracks_incompleteness_per_database(failure):
    context = _context()
    context.config = {"force_non_replicated": False, "override_replica_name": None}
    good = _table(database="good")
    good_meta = TableMetadata(
        "good", "t", "MergeTree", "shared" if failure == "duplicate_uuid" else None
    )
    bad_meta = TableMetadata("bad", "t", "MergeTree", good_meta.uuid)
    context.backup_meta.get_tables.side_effect = lambda db: (
        [good_meta]
        if db == "good"
        else ([] if failure == "missing_requested" else [bad_meta])
    )
    databases = {
        name: Database(name, "Atomic", None, None, None) for name in ("good", "bad")
    }
    manager = TableBackup()
    with (
        ExitStack() as stack,
        patch.object(
            manager,
            "_get_tables_from_meta",
            return_value=([good], int(failure == "missing_schema")),
        ),
    ):
        prepared = manager.prepare_restore(
            context,
            databases,
            [good_meta, bad_meta] if failure == "missing_requested" else [],
            True,
            False,
            False,
            stack,
        )
    assert prepared.incomplete_databases == {"bad"}
    assert prepared.tables == [good]


def test_database_preflight_does_not_start_external_engine_from_metadata_mismatch():
    context = _context()
    context.config = {"force_non_replicated": False, "override_replica_name": None}
    context.backup_layout.get_database_create_statements.return_value = [
        (
            "db",
            "CREATE DATABASE db ENGINE = PostgreSQL('host', 'db', 'user', 'password')",
        )
    ]
    context.ch_ctl.explain_schema.return_value = (
        " Storage definition (children 1)\n  Function PostgreSQL"
    )
    validate_database_schemas(
        context, {"db": Database("db", "Atomic", None, None, None)}
    )
    context.ch_ctl.create_schema_probe_database.assert_not_called()
