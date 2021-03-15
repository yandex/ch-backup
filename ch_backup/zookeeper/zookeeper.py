"""
ZooKeeper-control classes module
"""

import os
import socket
from typing import Dict, Iterable, Optional

from kazoo.client import KazooClient
from kazoo.exceptions import KazooException, NoNodeError

from ch_backup.logging import debug

from ..util import retry

KAZOO_RETRIES = retry(KazooException, max_attempts=5, max_interval=60)


class ZookeeperCTL:
    """
    ZooKeeper control tool.
    """
    def __init__(self, config: dict) -> None:
        if not config:
            raise RuntimeError('No zookeeper config provided, ZookeeperCTL unavailable')
        self._zk_secure = config['secure'] if 'secure' in config else False
        self._zk_cert = config['cert'] if 'cert' in config else None
        self._zk_client = KazooClient(config['hosts'], use_ssl=self._zk_secure, certfile=self._zk_cert)
        self._zk_root_path = config['root_path']
        self._zk_user = config['user'] if 'user' in config else None
        self._zk_password = config['password'] if 'password' in config else None

    @KAZOO_RETRIES
    def delete_replica_metadata(self,
                                table_paths: Iterable[str],
                                replica: Optional[str] = None,
                                macros: Dict = None) -> None:
        """
        Remove replica metadata from zookeeper for all tables from args.
        """
        if macros is None:
            macros = {}
        if not replica:
            replica = socket.getfqdn()

        self._zk_client.start()
        if self._zk_user and self._zk_password:
            self._zk_client.add_auth('digest', f'{self._zk_user}:{self._zk_password}')
        for table in table_paths:
            path = os.path.join(self._zk_root_path, table[1:].format(**macros), 'replicas',
                                replica)  # remove leading '/'
            debug(f'Deleting zk node: "{path}"')
            try:
                self._zk_client.delete(path, recursive=True)
            except NoNodeError:
                pass
        self._zk_client.stop()
