"""
ZooKeeper-control classes module
"""

import logging as py_logging

from kazoo.client import KazooClient
from kazoo.exceptions import KazooException
from kazoo.handlers.threading import KazooTimeoutError

from ..util import retry

KAZOO_RETRIES = retry(
    (KazooException, KazooTimeoutError), max_attempts=5, max_interval=60
)


class ZookeeperClient:
    """
    ZooKeeper client, adds context management to KazooClient
    """

    def __init__(self, config: dict):
        self._client = KazooClient(
            config["hosts"],
            use_ssl=config.get("secure"),
            certfile=config.get("cert"),
            keyfile=config.get("key"),
            ca=config.get("ca"),
            logger=py_logging.getLogger("zookeeper"),
            randomize_hosts=config.get("randomize_hosts", True),
        )
        self._zk_user = config.get("user")
        self._zk_password = config.get("password")
        self._connect_timeout = config.get("connect_timeout")
        self._entered = 0

    @KAZOO_RETRIES
    def __enter__(self) -> KazooClient:
        if self._entered == 0:
            self._client.start(self._connect_timeout)
            if self._zk_user and self._zk_password:
                self._client.add_auth("digest", f"{self._zk_user}:{self._zk_password}")
        self._entered += 1
        return self._client

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._entered == 0:
            self._client.stop()
        self._entered -= 1


class ZookeeperCTL:
    """
    ZooKeeper control tool.
    """

    def __init__(self, config: dict) -> None:
        if not config:
            raise RuntimeError("No zookeeper config provided, ZookeeperCTL unavailable")
        self._zk_client = ZookeeperClient(config)
        self._zk_root_path = config.get("root_path", "")

    @property
    def zk_client(self) -> ZookeeperClient:
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
