"""
ZooKeeper control unit tests.
"""

from unittest import mock

from kazoo.exceptions import NoNodeError

from ch_backup.zookeeper.zookeeper import ZookeeperCTL


def _zk_ctl(children: dict[str, list[str]]) -> ZookeeperCTL:
    """Helper: a control object with the root /ch over the given child nodes."""

    def get_children(path: str) -> list[str]:
        """Answer the request from the given data like ZooKeeper does."""
        if path not in children:
            raise NoNodeError
        return children[path]

    with mock.patch("ch_backup.zookeeper.zookeeper.KazooClient") as client_cls:
        client_cls.return_value.get_children.side_effect = get_children
        return ZookeeperCTL({"hosts": ["zk"], "root_path": "/ch"})


def test_get_children_prepends_root_path() -> None:
    """
    A path is relative to the root, as the ZooKeeper paths of ClickHouse are.
    """
    zk_ctl = _zk_ctl({"/ch/zk/t1/replicas": ["r1", "r2"]})

    assert zk_ctl.get_children("/zk/t1/replicas") == ["r1", "r2"]


def test_get_children_of_missing_path_is_empty() -> None:
    """
    A table without replicas in ZooKeeper has no replicas path.
    """
    zk_ctl = _zk_ctl({})

    assert not zk_ctl.get_children("/zk/t1/replicas")
