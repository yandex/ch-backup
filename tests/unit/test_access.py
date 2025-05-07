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
