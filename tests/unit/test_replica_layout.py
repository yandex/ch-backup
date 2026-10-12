"""
Unit tests for taking the schema from a live replica.
"""

import copy
from unittest.mock import MagicMock, patch

import pytest

from ch_backup.backup.replica_layout import ReplicaLayout
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


def _ctl(version: str = "25.8.1.1", macros: dict | None = None) -> MagicMock:
    """Helper: a ClickHouse control mock of the given version and macros."""
    ctl = MagicMock()
    ctl.get_version.return_value = version
    ctl.ch_version_ge.side_effect = lambda other: _parse_version(
        version
    ) >= _parse_version(other)
    ctl.get_macros.side_effect = lambda: dict(macros or MACROS)
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
    return ctl


def _layout(
    source: MagicMock,
    local: MagicMock | None = None,
    replica_name: str | None = None,
    zookeeper_hosts: str = "zk:2181",
) -> ReplicaLayout:
    """Helper: a replica layout that fails on any access to the backup storage."""
    config: dict = copy.deepcopy(DEFAULT_CONFIG)
    config["zookeeper"]["hosts"] = zookeeper_hosts
    with (
        patch("ch_backup.backup.layout.StorageLoader"),
        patch("ch_backup.backup.layout.get_encryption"),
    ):
        layout = ReplicaLayout(
            config,  # type: ignore[arg-type]
            local or _ctl(macros={"replica": "host2", "shard": "shard1"}),
            source,
            replica_name,
        )
    setattr(layout, "_storage_loader", None)
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
        (None, {"version": "25.3.1.1"}, {}, "differs"),
        (_ctl(macros=MACROS), {}, {}, "this replica itself"),
        (None, {"macros": {"replica": "host1", "shard": "shard2"}}, {}, "same shard"),
        (None, {"macros": {"shard": "shard1"}}, {}, "replica macro is not set"),
        (None, {}, {"zookeeper_hosts": ""}, "needs ZooKeeper"),
        (None, {}, {"replica_name": "host1"}, "is the one of replica1"),
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
            "mysql_db on replica1 has hidden secrets",
        ),
        (
            None,
            TABLE.replace("ORDER BY n", "ORDER BY n COMMENT 'mongodb://u:[HIDDEN]@h'"),
            "db1.t1 on replica1 has hidden secrets",
        ),
        (
            None,
            TABLE.replace("'{replica}'", "'host1'"),
            "db1.t1 on replica1 has replica name 'host1'",
        ),
        (
            {
                **DATABASES,
                "repl_db": DATABASES["repl_db"].replace("'{replica}'", "'host1'"),
            },
            TABLE,
            "repl_db on replica1 has replica name 'host1'",
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
    name would make this host take over the replica of the source in ZooKeeper.
    """
    with pytest.raises(ClickhouseBackupError, match=error):
        _layout(_source(databases, table)).get_backup("replica1")
