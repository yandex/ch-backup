"""
Access entities unit tests.
"""

from unittest import mock

from ch_backup.backup.replica_layout import ReplicaLayout
from ch_backup.backup_context import BackupContext
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


def test_access_from_replica_is_restored_by_its_layout() -> None:
    """
    A replica gives SQL statements instead of files, so its layout restores them and
    the file-based restore with removal of the access directory does not run.
    """
    context = mock.MagicMock()
    context.backup_layout = mock.MagicMock(spec=ReplicaLayout)
    context.ch_config.config = {"user_directories": {"replicated": {}}}

    with mock.patch("ch_backup.logic.access.shutil.rmtree") as rmtree:
        access.AccessBackup().restore(context, reload_users=True)

    context.backup_layout.restore_access.assert_called_once_with(context.ch_ctl, True)
    rmtree.assert_not_called()
    context.ch_ctl.reload_users.assert_not_called()
