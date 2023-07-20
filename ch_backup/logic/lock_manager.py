"""
Lock manager logic
"""
import os
import sys
from fcntl import LOCK_SH, flock
from types import TracebackType
from typing import IO, Optional, Type

from ch_backup.zookeeper.zookeeper import ZookeeperClient, ZookeeperCTL
from kazoo.recipe.lock import Lock


class LockManager:
    """
    Lock manager class
    """

    # pylint: disable=consider-using-with

    _lock_conf: dict
    _process_lockfile_path: str
    _process_zk_lockfile_path: str
    _fd: int
    _zk_lock: Lock
    _file: IO

    def __init__(self, lock_conf: dict, zk_ctl: Optional[ZookeeperCTL]) -> None:
        """
        Init-method for lock manager
        """
        self._lock_conf = lock_conf
        self._process_lockfile_path = str(lock_conf.get("flock_path"))
        self._process_zk_lockfile_path = str(lock_conf.get("zk_flock_path"))
        self._zk_client: Optional[ZookeeperClient] = (
            zk_ctl.zk_client if zk_ctl else None
        )
        self._zk_lock: Optional[Lock] = None
        self._exitcode = lock_conf.get("exitcode")
        self._distributed = zk_ctl is None

    def __call__(self, distributed: bool = True):  # type: ignore
        """
        Call-method for context manager
        """
        self._distributed = distributed
        return self

    def __enter__(self):  # type: ignore
        """
        Enter-method for context manager
        """
        if self._lock_conf.get("flock"):
            self._flock()
        if self._distributed and self._lock_conf.get("zk_flock"):
            self._zk_flock()
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_inst: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        """
        Exit-method for context manager
        """
        if self._lock_conf.get("flock"):
            os.close(self._fd)
        if self._zk_client and self._zk_lock is not None:
            self._zk_lock.release()
            self._zk_client.__exit__(None, None, None)

    def _flock(self) -> None:
        """
        Sets an advisory lock on the process_lockfile_path path
        """
        if not os.path.exists(self._process_lockfile_path):
            with open(self._process_lockfile_path, "w+", encoding="utf-8"):
                pass
        self._file = open(self._process_lockfile_path, "r", encoding="utf-8")
        self._fd = self._file.fileno()
        flock(self._fd, LOCK_SH)

    def _zk_flock(self) -> None:
        """
        Sets zookeeper lock on the process_zk_lockfile_path path
        """
        if self._zk_client:
            client = self._zk_client.__enter__()
            if not client.exists(self._process_zk_lockfile_path):
                client.create(self._process_zk_lockfile_path, makepath=True)
            self._zk_lock = client.Lock(self._process_zk_lockfile_path)
            if not self._zk_lock.acquire(blocking=False):
                sys.exit(self._exitcode)
        else:
            raise RuntimeError("ZK flock enabled, but zookeeper is not configured")
