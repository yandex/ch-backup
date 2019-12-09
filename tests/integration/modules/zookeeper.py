"""
ZooKeeper client calls
"""

from kazoo.client import KazooClient

from tests.integration.modules.docker import get_container, get_exposed_port

from .typing import ContextT


def initialize_zookeeper_roots(context: ContextT, node: str = 'zookeeper01') -> None:
    """
    create nodes for clickhouse roots
    """
    zk = _get_zookeeper_client(context, node)
    zk.start()
    instance_count = context.conf.get('projects', {}).get('clickhouse', {}).get('docker_instances', 1)
    for i in range(1, instance_count + 1):
        zk.create(f'/clickhouse{i:02d}')
    zk.stop()


def write_znode(context: ContextT, node: str, znode: str, data: str) -> None:
    """
    create specified zookeeper node
    """
    zk = _get_zookeeper_client(context, node)
    zk.start()
    zk.create(znode, data)
    zk.stop()


def _get_zookeeper_client(context: ContextT, node: str) -> KazooClient:
    zk_container = get_container(context, node)
    host, port = get_exposed_port(zk_container, 2181)
    return KazooClient(f'{host}:{port}')
