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


def test_access_statements_are_run_again_until_dependencies_exist() -> None:
    """
    A user who may grant to another user fails until that user exists.
    """
    context = mock.MagicMock()
    created: list[str] = []

    def run(statement: str) -> None:
        """Fail u1 until u2 exists."""
        if "GRANTEES u2" in statement and not created:
            raise ClickhouseError("There is no user u2")
        created.append(statement)

    context.ch_ctl.restore_access_entity.side_effect = run
    access.AccessBackup()._restore_statements(  # pylint: disable=protected-access
        context, ["CREATE USER u1 GRANTEES u2", "CREATE USER u2"]
    )

    assert created == ["CREATE USER u2", "CREATE USER u1 GRANTEES u2"]


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
