"""
ZooKeeper-control classes module
"""

import logging as py_logging
from contextlib import suppress

from kazoo.client import KazooClient
from kazoo.exceptions import KazooException, NoNodeError
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
            timeout=config.get("timeout"),
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
        self._cleanup_failed = False

    @KAZOO_RETRIES
    def __enter__(self) -> KazooClient:
        if self._cleanup_failed:
            raise RuntimeError("ZooKeeper client cannot be reused after failed cleanup")
        if self._entered == 0:
            try:
                self._client.start(self._connect_timeout)
                if self._zk_user and self._zk_password:
                    self._client.add_auth(
                        "digest", f"{self._zk_user}:{self._zk_password}"
                    )
            except BaseException as error:
                # A failed __enter__ has no matching __exit__. Stop Kazoo's
                # reconnect loop before retrying start(), which otherwise joins
                # that still-running loop without asking it to stop.
                self._close(error)
                raise
        self._entered += 1
        return self._client

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._entered -= 1
        if self._entered == 0:
            self._close(exc_val)

    def _close(self, original_error: BaseException | None = None) -> None:
        # Keep nested contexts (including Keeper locks) in the same session.
        # If cleanup fails, neither Tenacity nor a later caller may restart it.
        self._cleanup_failed = True
        try:
            try:
                self._client.stop()
            except BaseException:
                # stop() may raise after stopping the thread. Release remaining
                # resources if possible, without masking the original failure.
                with suppress(Exception):
                    self._client.close()
                raise
            self._client.close()
        except Exception as error:
            if original_error is not None and not isinstance(original_error, Exception):
                # Termination must not become an ordinary, catchable failure.
                raise original_error from error
            raise RuntimeError(
                f"Failed to close ZooKeeper client ({type(error).__name__})"
            ) from (original_error if original_error is not None else error)
        self._cleanup_failed = False


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

    def get_children(self, path: str) -> list[str]:
        """
        Get names of the child nodes of a path under the root, empty if the path is missing.
        """
        with self._zk_client as client:
            try:
                return client.get_children(self._zk_root_path + path)
            except NoNodeError:
                return []
