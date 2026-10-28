"""
Lock manager logic
"""

import os
import socket
import sys
from contextlib import ExitStack, contextmanager
from fcntl import LOCK_SH, flock
from functools import partial
from types import TracebackType
from typing import Any, Callable, Iterator, Sequence

from kazoo.exceptions import LockTimeout
from kazoo.hosts import collect_hosts
from kazoo.protocol.paths import normpath

from ch_backup import logging
from ch_backup.config import Config
from ch_backup.zookeeper.zookeeper import ZookeeperClient, ZookeeperCTL


@contextmanager
def storage_locks(configs: Sequence[Config], operation: str) -> Iterator[None]:
    """Acquire each configured lock once, in a stable order across storage pairs.

    These are the same advisory locks used by delete/purge. ZooKeeper lock identity
    depends on the ensemble and path, not the storage credentials or caller identity.
    File locks retain their existing shared semantics.
    """
    resources: dict[tuple, tuple[dict, dict | None]] = {}
    for config in configs:
        lock_conf, zk_conf = config["lock"], config["zookeeper"]
        if lock_conf.get("flock"):
            file_key = ("file", os.path.realpath(lock_conf["flock_path"]))
            resources.setdefault(file_key, ({**lock_conf, "zk_flock": False}, None))
        if lock_conf.get("zk_flock"):
            if not zk_conf["hosts"]:
                raise RuntimeError("ZK flock enabled, but zookeeper is not configured")
            hosts, chroot = collect_hosts(zk_conf["hosts"])
            path = normpath(
                (chroot or "") + "/" + lock_conf["zk_flock_path"].lstrip("/")
            )
            zk_key = ("zk", tuple(sorted(hosts)), path)
            resources.setdefault(zk_key, ({**lock_conf, "flock": False}, zk_conf))

    with ExitStack() as stack:
        for key in sorted(resources):
            lock_conf, zk_conf = resources[key]
            zk_ctl = ZookeeperCTL(zk_conf) if zk_conf else None
            stack.enter_context(LockManager(lock_conf, zk_ctl)(operation=operation))
        yield


class LockManager:
    """
    Lock manager class
    """

    # pylint: disable=consider-using-with,too-many-instance-attributes

    _lock_conf: dict
    _process_lockfile_path: str
    _process_zk_lockfile_path: str
    _exit_stack: ExitStack
    _lock_id: str
    _logger: Any

    def __init__(self, lock_conf: dict, zk_ctl: ZookeeperCTL | None) -> None:
        """
        Init-method for lock manager
        """
        self._lock_conf = lock_conf
        self._process_lockfile_path = str(lock_conf.get("flock_path"))
        self._process_zk_lockfile_path = str(lock_conf.get("zk_flock_path"))
        self._zk_client: ZookeeperClient | None = zk_ctl.zk_client if zk_ctl else None
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

        # Register cleanup as resources are acquired, including partial entry.
        with ExitStack() as stack:
            if self._lock_conf.get("flock"):
                self._flock(stack)
            if self._distributed and self._lock_conf.get("zk_flock"):
                self._zk_flock(stack)
            self._exit_stack = stack.pop_all()

        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_inst: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """
        Exit-method for context manager
        """
        if self._disabled:
            self._logger.debug("Lock is disabled on exit.")
            return

        self._exit_stack.__exit__(exc_type, exc_inst, exc_tb)

        self._logger.debug("Exiting lock.")

    @staticmethod
    def _release_resource(
        release: Callable[[], Any],
        _exc_type: type[BaseException] | None,
        exc_inst: BaseException | None,
        _exc_tb: TracebackType | None,
    ) -> None:
        """Release a resource without turning termination into an ordinary error."""
        try:
            release()
        except Exception as error:
            if exc_inst is not None and not isinstance(exc_inst, Exception):
                raise exc_inst from error
            raise

    def _flock(self, stack: ExitStack) -> None:
        """
        Sets an advisory lock on the process_lockfile_path path
        """
        if not os.path.exists(self._process_lockfile_path):
            with open(self._process_lockfile_path, "w+", encoding="utf-8"):
                pass
        lock_file = open(self._process_lockfile_path, "r", encoding="utf-8")
        stack.push(partial(self._release_resource, lock_file.close))
        flock(lock_file, LOCK_SH)

    def _zk_flock(self, stack: ExitStack) -> None:
        """
        Sets zookeeper lock on the process_zk_lockfile_path path
        """
        if self._zk_client:
            client = stack.enter_context(self._zk_client)
            if not client.exists(self._process_zk_lockfile_path):
                client.create(self._process_zk_lockfile_path, makepath=True)
            lock = client.Lock(self._process_zk_lockfile_path, identifier=self._lock_id)
            try:
                _ = lock.acquire(blocking=True, timeout=self._lock_timeout)
            except LockTimeout:
                msg = "ZK lock was not acquired due to timeout error."
                contenders = lock.contenders()
                if contenders:
                    msg = f"{msg} Contenders are {', '.join(contenders)}."
                self._logger.opt(exception=True).error(msg)
                sys.exit(self._exitcode)
            # Release the lock before closing its session, even on cleanup errors.
            stack.push(partial(self._release_resource, lock.release))
        else:
            raise RuntimeError("ZK flock enabled, but zookeeper is not configured")
