"""
ZooKeeper-control classes module
"""

import os
from typing import Dict, Iterable, Tuple

from kazoo.client import KazooClient
from kazoo.exceptions import KazooException, NoNodeError
from kazoo.handlers.threading import KazooTimeoutError

from ch_backup.logging import debug

from ..clickhouse.models import Table
from ..util import retry

KAZOO_RETRIES = retry((KazooException, KazooTimeoutError), max_attempts=5, max_interval=60)


class ZookeeperCTL:
    """
    ZooKeeper control tool.
    """
    def __init__(self, config: dict) -> None:
        if not config:
            raise RuntimeError('No zookeeper config provided, ZookeeperCTL unavailable')
        self._zk_secure = config['secure'] if 'secure' in config else False
        self._zk_cert = config['cert'] if 'cert' in config else None
        self._zk_key = config['key'] if 'key' in config else None
        self._zk_ca = config['ca'] if 'ca' in config else None
        self._zk_client = KazooClient(config['hosts'],
                                      use_ssl=self._zk_secure,
                                      certfile=self._zk_cert,
                                      keyfile=self._zk_key,
                                      ca=self._zk_ca)
        self._zk_root_path = config['root_path']
        self._zk_user = config['user'] if 'user' in config else None
        self._zk_password = config['password'] if 'password' in config else None

    @property
    def zk_client(self) -> KazooClient:
        """
        Getter zk_client
        """
        return self._zk_client

    @property
    def zk_root_path(self) -> str:
        """
        Getter zk_root_path
        """
        return self._zk_root_path

    def zk_add_auth(self) -> None:
        """
        Send credentials to server if they were configured.
        """
        if self._zk_user and self._zk_password:
            self._zk_client.add_auth('digest', f'{self._zk_user}:{self._zk_password}')

    @KAZOO_RETRIES
    def delete_replica_metadata(self, tables: Iterable[Tuple[Table, str]], replica: str, macros: Dict = None) -> None:
        """
        Remove replica metadata from zookeeper for all tables from args.
        """
        if macros is None:
            macros = {}

        self._zk_client.start()
        self.zk_add_auth()
        for table, table_path in tables:
            table_macros = dict(database=table.database, table=table.name, uuid=table.uuid)
            table_macros.update(macros)
            path = os.path.join(self._zk_root_path, table_path[1:].format(**table_macros), 'replicas',
                                replica)  # remove leading '/'
            debug(f'Deleting zk node: "{path}"')
            try:
                self._zk_client.delete(path, recursive=True)
            except NoNodeError:
                pass
        self._zk_client.stop()

    @KAZOO_RETRIES
    def delete_replicated_database_metadata(self, databases: Iterable[str], replica: str, macros: Dict = None) -> None:
        """
        Remove replica metadata from zookeeper for all replicated databases from args.
        """
        if macros is None:
            macros = {}
        macros['replica'] = replica

        self._zk_client.start()
        self.zk_add_auth()
        for zk_path in databases:
            path = os.path.join(self._zk_root_path, zk_path[1:].format(**macros))  # remove leading '/'
            debug(f'Deleting zk node: "{path}"')
            try:
                self._zk_client.delete(path, recursive=True)
            except NoNodeError:
                pass
        self._zk_client.stop()
