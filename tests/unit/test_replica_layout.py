"""
Unit tests for taking the schema from a live replica.
"""

import copy
from unittest.mock import MagicMock, patch

import pytest

from ch_backup.backup.replica_layout import ReplicaLayout
from ch_backup.backup.sources import BackupSources
from ch_backup.clickhouse.control import _parse_version
from ch_backup.clickhouse.models import Database, Table
from ch_backup.config import DEFAULT_CONFIG
from ch_backup.exceptions import ClickhouseBackupError

MACROS = {"replica": "host1", "shard": "shard1"}
DATABASES = {
    "db1": "CREATE DATABASE db1 UUID '1' ENGINE = Atomic",
    "mysql_db": "CREATE DATABASE mysql_db ENGINE = MySQL('h:3306', 'db', 'u', 'p')",
    "repl_db": "CREATE DATABASE repl_db UUID '3' ENGINE = Replicated('/db', '{shard}', '{replica}')",
}
TABLE = (
    "CREATE TABLE db1.t1 UUID '2' (n UInt32) "
    "ENGINE = ReplicatedMergeTree('/tables/{shard}/t1', '{replica}') ORDER BY n"
)
UDF = "CREATE FUNCTION f1 AS x -> x + 1"
NAMED_COLLECTION = "CREATE NAMED COLLECTION nc1 AS host = 'h', password = 'p'"
ACCESS = [
    "CREATE ROLE r1",
    "CREATE USER u1 IDENTIFIED WITH sha256_hash BY 'abc' SALT 'def' DEFAULT ROLE r1",
    "GRANT SELECT ON db1.* TO r1",
    "GRANT r1 TO u1",
]
ALL = BackupSources(data=False, workload_entities=False)


def _ctl(version: str = "25.8.1.1", macros: dict | None = None) -> MagicMock:
    """Helper: a ClickHouse control mock of the given version and macros."""
    ctl = MagicMock()
    ctl.get_version.return_value = version
    ctl.ch_version_ge.side_effect = lambda other: _parse_version(
        version
    ) >= _parse_version(other)
    ctl.get_macros.side_effect = lambda: dict(macros or MACROS)
    return ctl


def _local(version: str = "25.8.1.1", **returns: object) -> MagicMock:
    """Helper: this host with a collection from the config and local access storage,
    unless given other results of its control methods."""
    ctl = _ctl(version, macros={"replica": "host2", "shard": "shard1"})
    ctl.get_named_collections_query.return_value = ["nc_config"]
    ctl.get_named_collections_create_queries.return_value = {}
    ctl.get_access_storage_types.return_value = {"users_xml", "local_directory"}
    for method, value in returns.items():
        getattr(ctl, method).return_value = value
    return ctl


def _source(
    databases: dict | None = None, table: str = TABLE, **kwargs: str | dict
) -> MagicMock:
    """Helper: a source replica with an Atomic, a MySQL and a Replicated database."""
    ctl = _ctl(**kwargs)  # type: ignore[arg-type]
    ctl.get_databases.return_value = [
        Database("db1", "Atomic", "/m/db1.sql", "1", None),
        Database("mysql_db", "MySQL", "/m/mysql_db.sql", None, None),
        Database("repl_db", "Replicated", "/m/repl_db.sql", "3", None),
    ]
    ctl.get_database_schema.side_effect = lambda db: (databases or DATABASES)[db]
    ctl.get_tables.return_value = [
        Table("db1", "t1", "ReplicatedMergeTree", [], [], "/m/t1.sql", table, "2"),
        Table("db1", "dict1", "Dictionary", [], [], "", "", None),
    ]
    ctl.get_udf_query.return_value = {"f1": UDF}
    ctl.get_named_collections_query.return_value = ["nc1", "nc_config"]
    ctl.get_named_collections_create_queries.return_value = {"nc1": NAMED_COLLECTION}
    ctl.get_local_access_statements.return_value = list(ACCESS)
    return ctl


def _layout(
    source: MagicMock,
    local: MagicMock | None = None,
    *,
    replica_name: str | None = None,
    zookeeper_hosts: str = "zk:2181",
    sources: BackupSources = ALL,
    override_replica_name: str | None = "{replica}",
    ch_config: dict | None = None,
) -> ReplicaLayout:
    """Helper: a replica layout that fails on any access to the backup storage."""
    config: dict = copy.deepcopy(DEFAULT_CONFIG)
    config["zookeeper"]["hosts"] = zookeeper_hosts
    config["backup"]["override_replica_name"] = override_replica_name
    with patch("ch_backup.backup.layout.StorageLoader"):
        layout = ReplicaLayout(
            config,  # type: ignore[arg-type]
            local or _local(),
            source,
            sources,
            replica_name,
        )
    setattr(layout, "_storage_loader", None)
    setattr(layout, "_ch_config", MagicMock(config=ch_config or {}))
    return layout


def test_schema_is_read_from_the_replica() -> None:
    """
    Tables of external and Replicated databases and tables without metadata are left
    out, and nothing is read from the backup storage.
    """
    source = _source()
    layout = _layout(source)

    backup = layout.get_backup("replica1")

    assert backup.schema_only
    assert list(backup.get_databases()) == ["db1", "mysql_db", "repl_db"]
    assert [(table.name, table.uuid) for table in backup.get_tables()] == [("t1", "2")]
    source.get_tables.assert_called_once_with("db1")
    assert layout.get_database_create_statements(backup, ["db1"]) == [
        ("db1", DATABASES["db1"])
    ]
    assert layout.get_table_create_statements(backup, "db1", ["t1"]) == [("t1", TABLE)]


@pytest.mark.parametrize(
    "local,source,layout,error",
    [
        (_ctl("23.3.1.1"), {}, {}, "23.8 or newer"),
        (None, {"version": "23.3.1.1"}, {}, "23.8 or newer"),
        (None, {"version": "25.3.1.1"}, {}, "differ: the schema"),
        (_ctl(macros=MACROS), {}, {}, "is this host itself"),
        (
            None,
            {"macros": {"replica": "host1", "shard": "shard2"}},
            {},
            "not a replica of this shard",
        ),
        (None, {"macros": {"shard": "shard1"}}, {}, "replica macro must be set"),
        (None, {}, {"zookeeper_hosts": ""}, "ZooKeeper is not configured"),
        (None, {}, {"replica_name": "host1"}, "is the source replica1"),
    ],
    ids=[
        "old local version",
        "old source version",
        "other minor version",
        "same replica",
        "other shard",
        "no replica macro",
        "no zookeeper",
        "replica name of the source",
    ],
)
def test_wrong_source_fails_before_reading_schema(
    local: MagicMock | None, source: dict, layout: dict, error: str
) -> None:
    """
    The schema must come from another replica of the same shard on the same version,
    and the replica to remove from ZooKeeper must not be the source.
    """
    source_ctl = _source(**source)

    with pytest.raises(ClickhouseBackupError, match=error):
        _layout(source_ctl, local, **layout).get_backup("replica1")

    source_ctl.get_databases.assert_not_called()


def test_other_patch_version_is_accepted() -> None:
    """
    Replicas of a shard are updated one by one, so patch versions may differ for a while.
    """
    backup = _layout(_source(version="25.8.2.2")).get_backup("replica1")

    assert list(backup.get_databases()) == ["db1", "mysql_db", "repl_db"]


@pytest.mark.parametrize(
    "databases,table,error",
    [
        (
            {
                **DATABASES,
                "mysql_db": DATABASES["mysql_db"].replace("'p'", "'[HIDDEN]'"),
            },
            TABLE,
            "Secrets of mysql_db on replica1 are hidden",
        ),
        (
            None,
            TABLE.replace("ORDER BY n", "ORDER BY n COMMENT 'mongodb://u:[HIDDEN]@h'"),
            "Secrets of db1.t1 on replica1 are hidden",
        ),
        (
            None,
            TABLE.replace("'{replica}'", "'host1'"),
            "db1.t1 gets replica name 'host1'",
        ),
        (
            {
                **DATABASES,
                "repl_db": DATABASES["repl_db"].replace("'{replica}'", "'host1'"),
            },
            TABLE,
            "repl_db gets replica name 'host1'",
        ),
    ],
    ids=[
        "quoted secret",
        "secret in uri",
        "table replica literal",
        "database replica literal",
    ],
)
def test_unusable_statement_fails_the_restore(
    databases: dict | None, table: str, error: str
) -> None:
    """
    An object created from a masked statement would not work, and a literal replica
    name left by restore would make this host take over the replica of the source.
    """
    with pytest.raises(ClickhouseBackupError, match=error):
        _layout(_source(databases, table), override_replica_name=None).get_backup(
            "replica1"
        )


@pytest.mark.parametrize(
    "databases,table",
    [
        (None, TABLE.replace("'{replica}'", "'host1'")),
        (
            {
                **DATABASES,
                "repl_db": DATABASES["repl_db"].replace("'{replica}'", "'host1'"),
            },
            TABLE,
        ),
    ],
    ids=["table", "database"],
)
def test_replica_literal_is_accepted_if_restore_replaces_it(
    databases: dict | None, table: str
) -> None:
    """
    Restore puts override_replica_name in place of the replica name, so the statement
    is checked as rewritten and kept as is for restore to rewrite.
    """
    layout = _layout(_source(databases, table))

    backup = layout.get_backup("replica1")

    assert layout.get_table_create_statements(backup, "db1", ["t1"]) == [("t1", table)]
    assert layout.get_database_create_statements(backup, ["repl_db"]) == [
        ("repl_db", (databases or DATABASES)["repl_db"])
    ]


def test_literal_override_of_replica_name_fails_the_restore() -> None:
    """
    This host would take over the replica named in override_replica_name.
    """
    with pytest.raises(ClickhouseBackupError, match="'host1'.*override_replica_name"):
        _layout(_source(), override_replica_name="host1").get_backup("replica1")


def test_functions_named_collections_and_access_are_read_from_the_replica() -> None:
    """
    Collections from the server config of the replica are left alone. Access entities of
    this host are replaced with ones of the replica.
    """
    layout = _layout(_source())

    backup = layout.get_backup("replica1")

    assert backup.get_udf() == ["f1"]
    assert layout.get_udf_create_statement(backup, "f1") == UDF
    assert backup.get_named_collections() == ["nc1"]
    assert (
        layout.get_named_collection_create_statement(backup, "nc1") == NAMED_COLLECTION
    )
    assert layout.get_access_statements() == ACCESS


def test_existing_function_and_named_collection_are_read() -> None:
    """
    Restore skips ones this host has the same and replaces others, so a stale one does
    not stay. A collection from the server config of this host is left alone.
    """
    source = _source()
    source.get_named_collections_create_queries.return_value = {
        "nc1": NAMED_COLLECTION,
        "nc_config": "CREATE NAMED COLLECTION nc_config AS host = 'h'",
    }
    local = _local(
        get_udf_query={"f1": "CREATE FUNCTION f1 AS x -> x + 2"},
        get_named_collections_query=["nc1", "nc_config"],
        get_named_collections_create_queries={"nc1": "CREATE NAMED COLLECTION nc1"},
    )

    backup = _layout(source, local).get_backup("replica1")

    assert backup.get_udf() == ["f1"]
    assert backup.get_named_collections() == ["nc1"]


def test_named_collections_in_zookeeper_are_read_only_if_missing() -> None:
    """
    They are shared with the replica, and restore may find them different in format and
    drop them on all replicas.
    """
    local = _local(get_named_collections_query=["nc1", "nc_config"])
    ch_config = {"named_collections_storage": {"type": "zookeeper", "path": "/nc"}}

    backup = _layout(_source(), local, ch_config=ch_config).get_backup("replica1")

    assert not backup.get_named_collections()


def test_functions_in_zookeeper_are_read_only_if_missing() -> None:
    """
    They are shared with the replica, and restore would drop one that differs in format
    on all replicas.
    """
    source = _source()
    source.get_udf_query.return_value = {
        "f1": UDF,
        "f2": "CREATE FUNCTION f2 AS x -> x",
    }
    local = _local(get_udf_query={"f1": "CREATE FUNCTION f1 AS x -> x + 2"})
    ch_config = {"user_defined_zookeeper_path": "/udf"}

    backup = _layout(source, local, ch_config=ch_config).get_backup("replica1")

    assert backup.get_udf() == ["f2"]


def test_only_requested_objects_are_read() -> None:
    """
    A partial restore must not fail on objects it does not restore.
    """
    source = _source()
    sources = BackupSources.for_restore(udf=True)

    backup = _layout(source, sources=sources).get_backup("replica1")

    assert backup.get_udf() == ["f1"]
    assert not backup.get_databases()
    source.get_databases.assert_not_called()
    source.get_named_collections_query.assert_not_called()
    source.get_local_access_statements.assert_not_called()


@pytest.mark.parametrize(
    "statement,error",
    [
        (
            "CREATE USER u2 IDENTIFIED WITH sha256_password",
            "Passwords of users on replica1 are hidden",
        ),
        (
            "CREATE USER u2 IDENTIFIED WITH sha256_hash BY '[HIDDEN]'",
            "Secrets of access entities on replica1 are hidden",
        ),
    ],
    ids=["password type without hash", "masked hash"],
)
def test_user_with_hidden_password_fails_the_restore(
    statement: str, error: str
) -> None:
    """
    Without secrets shown ClickHouse prints only the type of a password, and the user
    would be created without it.
    """
    source = _source()
    source.get_local_access_statements.return_value = ACCESS + [statement]

    with pytest.raises(ClickhouseBackupError, match=error):
        _layout(source).get_backup("replica1")


@pytest.mark.parametrize(
    "storages",
    [
        {"users_xml", "replicated"},
        {"users_xml", "local_directory", "replicated"},
        {"users_xml", "memory"},
        {"users_xml"},
    ],
    ids=["replicated", "local and replicated", "memory", "no local"],
)
def test_access_is_restored_only_into_local_storage(storages: set[str]) -> None:
    """
    Entities created here in replicated storage would be shared with other replicas, and
    ones in memory would be lost on restart.
    """
    local = _local(get_access_storage_types=storages)

    with pytest.raises(ClickhouseBackupError, match="only into local storage"):
        _layout(_source(), local).get_backup("replica1")


def test_named_collections_need_a_version_that_shows_them() -> None:
    """
    Before 25.5 ClickHouse does not show create statements of named collections.
    """
    local = _local("25.3.1.1", get_named_collections_query=[])

    with pytest.raises(ClickhouseBackupError, match="nc1, nc_config are missing"):
        _layout(_source(version="25.3.1.1"), local).get_backup("replica1")


def test_named_collection_from_server_config_is_not_created_by_sql() -> None:
    """
    It would clash with the collection once the server config of this host gets it.
    """
    local = _local(get_named_collections_query=[])

    with pytest.raises(ClickhouseBackupError, match="nc_config comes from the server"):
        _layout(_source(), local).get_backup("replica1")
