"""
ZooKeeper client calls
"""

import logging
from typing import List, Optional

from kazoo.client import KazooClient
from kazoo.exceptions import NodeExistsError, NoNodeError

from tests.integration.modules.docker import get_container, get_exposed_port

from .typing import ContextT


def initialize_zookeeper_roots(context: ContextT, node: str = "zookeeper01") -> None:
    """
    create nodes for clickhouse roots
    """
    zk = _get_zookeeper_client(context, node)
    zk.start()
    instance_count = (
        context.conf.get("services", {})
        .get("clickhouse", {})
        .get("docker_instances", 1)
    )
    for i in range(1, instance_count + 1):
        if not zk.exists(f"/clickhouse{i:02d}"):
            try:
                zk.create(f"/clickhouse{i:02d}")
            except NodeExistsError:
                pass
    zk.stop()


def write_znode(context: ContextT, node: str, znode: str, data: bytes) -> None:
    """
    create specified zookeeper node
    """
    zk = _get_zookeeper_client(context, node)
    zk.start()

    zk.create(znode, data, makepath=True)
    zk.stop()


def delete_znode(context: ContextT, node: str, znode: str) -> None:
    """
    delete specified zookeeper node
    """
    zk = _get_zookeeper_client(context, node)
    zk.start()

    zk.delete(znode)
    zk.stop()


def znode_exists(context: ContextT, node: str, zk_path: str) -> bool:
    """
    Check if the znode is exists.
    """
    zk = _get_zookeeper_client(context, node)
    zk.start()

    result = zk.exists(path=zk_path)
    zk.stop()
    return result


def get_children_list(context: ContextT, node: str, zk_path: str) -> Optional[List]:
    zk = _get_zookeeper_client(context, node)
    zk_config = context.conf.get("zk", {})
    try:
        zk.start()
        if zk_config.get("user") and zk_config.get("password"):
            zk.add_auth(
                "digest", f'{zk_config.get("user")}:{zk_config.get("password")}'
            )
        return zk.get_children(zk_path)
    except NoNodeError:
        return None
    finally:
        zk.stop()


def _get_zookeeper_client(context: ContextT, node: str) -> KazooClient:
    # disable logging
    logging.getLogger("kazoo").setLevel(logging.CRITICAL)

    zk_container = get_container(context, node)
    host, port = get_exposed_port(zk_container, 2181)
    return KazooClient(f"{host}:{port}")
