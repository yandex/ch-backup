"""
Lock manager logic
"""
import os
from fcntl import LOCK_SH, flock
from types import TracebackType
from typing import IO, Optional, Type
from kazoo.recipe.lock import Lock

from ch_backup.zookeeper.zookeeper import ZookeeperCTL


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
        self._process_lockfile_path = str(lock_conf.get('flock_path'))
        self._process_zk_lockfile_path = str(lock_conf.get('zk_flock_path'))
        self._zk_client = zk_ctl.zk_client if zk_ctl else None

    def __call__(self):  # type: ignore
        """
        Call-method for context manager
        """
        return self

    def __enter__(self):  # type: ignore
        """
        Enter-method for context manager
        """
        if self._lock_conf.get('flock'):
            self._flock()
        if self._zk_client and self._lock_conf.get('zk_flock'):
            self._zk_flock()
        return self

    def __exit__(self, exc_type: Optional[Type[BaseException]], exc_inst: Optional[BaseException],
                 exc_tb: Optional[TracebackType]) -> None:
        """
        Exit-method for context manager
        """
        if self._lock_conf.get('flock'):
            os.close(self._fd)
        if self._zk_client and self._lock_conf.get('zk_flock'):
            self._zk_lock.release()

    def _flock(self) -> None:
        """
        Sets an advisory lock on the process_lockfile_path path
        """
        if not os.path.exists(self._process_lockfile_path):
            with open(self._process_lockfile_path, 'w+', encoding='utf-8'):
                pass
        self._file = open(self._process_lockfile_path, 'r', encoding='utf-8')
        self._fd = self._file.fileno()
        flock(self._fd, LOCK_SH)

    def _zk_flock(self) -> None:
        """
        Sets zookeeper lock on the process_zk_lockfile_path path
        """
        self._zk_lock = self._zk_client.Lock(self._process_zk_lockfile_path)
