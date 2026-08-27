"""
Access entities unit tests.
"""

from typing import Optional
from unittest import mock

from ch_backup.backup_context import BackupContext
from ch_backup.logic import access
from ch_backup.zookeeper.zookeeper import ZookeeperCTL
from tests.unit.utils import parametrize


def zookeeper_mock(config: Optional[dict] = None) -> ZookeeperCTL:
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


@parametrize(
    {
        "id": "reload local access",
        "args": {
            "replicated": False,
            "reload_users": True,
            "expected_events": ["restore_local", "reload_users"],
        },
    },
    {
        "id": "do not reload local access by default",
        "args": {
            "replicated": False,
            "reload_users": False,
            "expected_events": ["restore_local"],
        },
    },
    {
        "id": "do not reload replicated access",
        "args": {
            "replicated": True,
            "reload_users": True,
            "expected_events": ["restore_replicated"],
        },
    },
)
def test_restore_reload_users(replicated, reload_users, expected_events):
    context = mock.Mock(spec=BackupContext)
    context.backup_meta.access_control.acl_ids = ["access-id"]
    context.backup_meta.access_control.acl_meta = {}
    context.backup_meta.name = "backup"
    context.ch_ctl_conf = {
        "access_control_path": "/access",
        "tmp_path": "/tmp",
        "user": "clickhouse",
        "group": "clickhouse",
    }
    context.ch_config.config = (
        {"user_directories": {"replicated": {}}} if replicated else {}
    )

    events = []
    context.ch_ctl.reload_users.side_effect = lambda: events.append("reload_users")
    manager = access.AccessBackup()

    with (
        mock.patch("ch_backup.logic.access.os.path.exists", return_value=False),
        mock.patch("ch_backup.logic.access.ensure_owned_directory"),
        mock.patch("ch_backup.logic.access.temp_directory") as temp_directory_mock,
        mock.patch.object(manager, "_download_access_control_list"),
        mock.patch.object(manager, "_restore_local") as restore_local_mock,
        mock.patch.object(manager, "_restore_replicated") as restore_replicated_mock,
    ):
        temp_directory_mock.return_value.__enter__.return_value = "/restore"
        restore_local_mock.side_effect = lambda *args: events.append("restore_local")
        restore_replicated_mock.side_effect = lambda *args: events.append(
            "restore_replicated"
        )

        manager.restore(context, reload_users=reload_users)

    assert events == expected_events


def test_restore_does_not_reload_users_for_empty_access_control():
    context = mock.Mock(spec=BackupContext)
    context.backup_meta.access_control.acl_ids = []
    context.backup_meta.access_control.acl_meta = {}
    manager = access.AccessBackup()

    manager.restore(context, reload_users=True)

    context.ch_ctl.reload_users.assert_not_called()
