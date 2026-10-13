"""
Access entities unit tests.
"""

from unittest import mock

import pytest

from ch_backup.backup.replica_layout import ReplicaLayout
from ch_backup.backup_context import BackupContext
from ch_backup.clickhouse.client import ClickhouseError
from ch_backup.exceptions import ClickhouseBackupError
from ch_backup.logic import access
from ch_backup.zookeeper.zookeeper import ZookeeperCTL
from tests.unit.utils import parametrize


def zookeeper_mock(config: dict | None = None) -> ZookeeperCTL:
    config = config or {}
    default = {"hosts": [], "root_path": "/"}
    with mock.patch("ch_backup.zookeeper.zookeeper.KazooClient"):
        return ZookeeperCTL({**default, **config})


@parametrize(
    {
        "id": "regular config",
        "args": {
            "config": {
                "clickhouse": {
                    "zk_access_control_path": "/clickhouse/access",
                },
                "zookeeper": {
                    "root_path": "/",
                },
            },
            "zk_path": "/uuid/123",
            "expected": "/clickhouse/access/uuid/123",
        },
    },
    {
        "id": "regular config (without slashes)",
        "args": {
            "config": {
                "clickhouse": {
                    "zk_access_control_path": "clickhouse/access",
                },
                "zookeeper": {
                    "root_path": "/",
                },
            },
            "zk_path": "uuid/123",
            "expected": "/clickhouse/access/uuid/123",
        },
    },
    {
        "id": "empty paths",
        "args": {
            "config": {
                "clickhouse": {
                    "zk_access_control_path": "",
                },
                "zookeeper": {
                    "root_path": "",
                },
            },
            "zk_path": "",
            "expected": "/",
        },
    },
    {
        "id": "extra slashes",
        "args": {
            "config": {
                "clickhouse": {
                    "zk_access_control_path": "//clickhouse/access",
                },
                "zookeeper": {
                    "root_path": "//",
                },
            },
            "zk_path": "//uuid/123",
            "expected": "/clickhouse/access/uuid/123",
        },
    },
    {
        "id": "only slashes",
        "args": {
            "config": {
                "clickhouse": {
                    "zk_access_control_path": "///",
                },
                "zookeeper": {
                    "root_path": "///",
                },
            },
            "zk_path": "///",
            "expected": "/",
        },
    },
)
def test_get_access_zk_path(config, zk_path, expected):
    # pylint: disable=protected-access
    context = BackupContext(config)
    context.zk_ctl = zookeeper_mock(config)
    assert access._get_access_zk_path(context, zk_path) == expected


def test_access_from_replica_is_restored_from_statements() -> None:
    """
    A replica gives SQL statements instead of files, so local entities are replaced with
    them and the file-based restore with removal of the access directory does not run.
    """
    context = mock.MagicMock()
    context.backup_layout = mock.MagicMock(spec=ReplicaLayout)
    context.backup_layout.get_access_statements.return_value = ["CREATE ROLE r1"]

    with mock.patch("ch_backup.logic.access.shutil.rmtree") as rmtree:
        access.AccessBackup().restore(context, reload_users=True)

    assert [call[0] for call in context.ch_ctl.mock_calls] == [
        "drop_local_access_entities",
        "restore_access_entity",
    ]
    context.ch_ctl.restore_access_entity.assert_called_once_with("CREATE ROLE r1")
    rmtree.assert_not_called()


def test_access_of_this_host_is_dropped_if_the_replica_has_none() -> None:
    """
    This host must end up with the same local access entities as the replica.
    """
    context = mock.MagicMock()
    context.backup_layout = mock.MagicMock(spec=ReplicaLayout)
    context.backup_layout.get_access_statements.return_value = []

    access.AccessBackup().restore(context, reload_users=True)

    context.ch_ctl.drop_local_access_entities.assert_called_once_with()
    context.ch_ctl.restore_access_entity.assert_not_called()


def test_users_granting_to_each_other_are_restored() -> None:
    """
    ClickHouse creates a user before it resolves GRANTEES and fails on a grantee that does
    not exist yet, leaving the user with GRANTEES ANY until it is altered.
    """
    context = mock.MagicMock()
    grantees: dict[str, str] = {}

    def run(statement: str) -> None:
        """Create or alter a user as ClickHouse does."""
        verb, name, _, grantee = statement.rsplit(" ", 3)
        created = name not in grantees
        grantees.setdefault(name, "ANY")
        if grantee not in grantees:
            raise ClickhouseError(f"There is no user {grantee}")
        if created or verb == "ALTER USER":
            grantees[name] = grantee

    context.ch_ctl.restore_access_entity.side_effect = run
    access.AccessBackup()._restore_statements(  # pylint: disable=protected-access
        context,
        [
            "CREATE USER IF NOT EXISTS a GRANTEES z",
            "CREATE USER IF NOT EXISTS z GRANTEES a",
            "ALTER USER a GRANTEES z",
            "ALTER USER z GRANTEES a",
        ],
    )

    assert grantees == {"a": "z", "z": "a"}


def test_access_restore_fails_without_progress() -> None:
    """
    Statements that keep failing must stop the restore instead of looping.
    """
    context = mock.MagicMock()
    context.ch_ctl.restore_access_entity.side_effect = ClickhouseError("boom")

    with pytest.raises(
        ClickhouseBackupError, match="2 access statements of the replica failed"
    ):
        access.AccessBackup()._restore_statements(  # pylint: disable=protected-access
            context, ["CREATE ROLE r1", "CREATE USER u1"]
        )
