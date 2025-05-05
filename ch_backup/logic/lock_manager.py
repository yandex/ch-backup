"""
Lock manager logic
"""

import os
import socket
import sys
from fcntl import LOCK_SH, flock
from types import TracebackType
from typing import IO, Any, Optional, Type

from kazoo.exceptions import LockTimeout
from kazoo.recipe.lock import Lock

from ch_backup import logging
from ch_backup.zookeeper.zookeeper import ZookeeperClient, ZookeeperCTL


class LockManager:
    """
    Lock manager class
    """

    # pylint: disable=consider-using-with,too-many-instance-attributes

    _lock_conf: dict
    _process_lockfile_path: str
    _process_zk_lockfile_path: str
    _zk_lock: Lock
    _file: IO
    _lock_id: str
    _logger: Any

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
        self._disabled = False
        self._lock_timeout = lock_conf.get("lock_timeout")
        self._logger = logging.getLogger("ch-backup").patch(self._log_message)

    def _log_message(self, record: dict) -> None:
        """
        Patch the log message in order to include the lock id
        """
        record["message"] = f"<LockManager@{self._lock_id}> {record['message']}"

    def __call__(self, distributed: bool = True, disabled: bool = False, operation: str = "UNKNOWN"):  # type: ignore
        """
        Call-method for context manager
        """
        self._distributed = distributed
        self._disabled = disabled
        self._lock_id = f"{operation}/{socket.getfqdn()}"
        return self

    def __enter__(self):  # type: ignore
        """
        Enter-method for context manager
        """
        self._logger.debug("Entering lock.")

        if self._disabled:
            self._logger.debug("Lock is disabled on enter.")
            return self

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
        if self._disabled:
            self._logger.debug("Lock is disabled on exit.")
            return

        if self._lock_conf.get("flock"):
            self._file.close()
        if self._zk_client and self._zk_lock is not None:
            self._zk_lock.release()
            self._zk_client.__exit__(None, None, None)

        self._logger.debug("Exiting lock.")

    def _flock(self) -> None:
        """
        Sets an advisory lock on the process_lockfile_path path
        """
        if not os.path.exists(self._process_lockfile_path):
            with open(self._process_lockfile_path, "w+", encoding="utf-8"):
                pass
        self._file = open(self._process_lockfile_path, "r+", encoding="utf-8")
        flock(self._file, LOCK_SH)

    def _zk_flock(self) -> None:
        """
        Sets zookeeper lock on the process_zk_lockfile_path path
        """
        # pylint: disable=unnecessary-dunder-call

        if self._zk_client:
            client = self._zk_client.__enter__()
            if not client.exists(self._process_zk_lockfile_path):
                client.create(self._process_zk_lockfile_path, makepath=True)
            self._zk_lock = client.Lock(
                self._process_zk_lockfile_path, identifier=self._lock_id
            )
            try:
                _ = self._zk_lock.acquire(blocking=True, timeout=self._lock_timeout)
            except LockTimeout:
                msg = "ZK lock was not acquired due to timeout error."
                contenders = self._zk_lock.contenders()
                if contenders:
                    msg = f"{msg} Contenders are {', '.join(contenders)}."
                self._logger.opt(exception=True).error(msg)
                sys.exit(self._exitcode)
        else:
            raise RuntimeError("ZK flock enabled, but zookeeper is not configured")
