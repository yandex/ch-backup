"""
Module responsible for waiting on replicated database synchronization.
"""

from __future__ import annotations

import posixpath
import re
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, Iterable

from ch_backup import logging
from ch_backup.backup_context import BackupContext
from ch_backup.clickhouse.models import Database
from ch_backup.util import replace_macros
from ch_backup.zookeeper.zookeeper import ZookeeperCTL

REPLICATED_DB_ENGINE_RE = re.compile(
    r"""Replicated\('(?P<zk_path>[^']+)',\s*'(?P<shard>[^']+)',\s*'(?P<replica>[^']+)'\)"""
)


class SyncStatus(Enum):
    """Status of replicated database sync operation."""

    DONE = "done"
    FAILED = "failed"


@dataclass
class DatabaseZookeeperData:
    """
    Resolved ZooKeeper information for a replicated database and helpers
    to read its sync state.
    """

    zk_ctl: ZookeeperCTL = field(repr=False)
    zk_path: str = ""
    shard: str = ""
    replica: str = ""
    max_log_ptr: int = 0

    @property
    def _zk_client(self):
        return self.zk_ctl.zk_client

    @property
    def _zk_root_path(self) -> str:
        return self.zk_ctl.zk_root_path

    def _read_int_node(self, path: str) -> int:
        """Read an integer value from ZooKeeper."""
        with self._zk_client as zk_client:
            data, _ = zk_client.get(path)
            return int(data.decode("utf-8").strip())

    def _full_path(self, suffix: str) -> str:
        # Use posixpath.join to correctly handle zk_path with or without leading '/'.
        # Simple string concatenation like f"{root}{path}" fails when path has no
        # leading '/', producing invalid paths like '/clickhouse02some/path/db'.
        full = posixpath.join(self._zk_root_path, self.zk_path.lstrip("/")) + suffix
        return full

    def get_log_ptr(self) -> int:
        """Read log_ptr for this database replica."""
        replica_key = f"{self.shard}|{self.replica}"
        return self._read_int_node(self._full_path(f"/replicas/{replica_key}/log_ptr"))

    def get_max_log_ptr(self) -> int:
        """Read global max_log_ptr for this replicated database."""
        return self._read_int_node(self._full_path("/max_log_ptr"))

    @staticmethod
    def parse_replicated_db_zk_info(db: Database, macros: Dict[str, str]) -> tuple:
        """
        Parse ZooKeeper path, shard, and replica from Replicated database engine_full.
        """
        if not db.engine_full:
            raise RuntimeError(
                f"Database {db.name}: engine_full is empty, cannot parse ZK path."
            )

        match = REPLICATED_DB_ENGINE_RE.search(db.engine_full)
        if not match:
            raise RuntimeError(
                f"Database {db.name}: cannot parse ZK path from engine_full={db.engine_full}."
            )

        return (
            replace_macros(match.group("zk_path"), macros),
            replace_macros(match.group("shard"), macros),
            replace_macros(match.group("replica"), macros),
        )

    @classmethod
    def resolve(cls, context: BackupContext, db: Database) -> "DatabaseZookeeperData":
        """
        Resolve ZooKeeper parameters for a replicated database and load max_log_ptr.
        Raises RuntimeError if ZK parameters cannot be resolved.
        """
        macros = context.ch_ctl.get_macros()

        live_databases = {d.name: d for d in context.ch_ctl.get_databases()}
        live_db = live_databases.get(db.name)
        if live_db is None:
            raise RuntimeError(
                f"Database {db.name}: not found in ClickHouse, cannot resolve ZK path."
            )

        zk_path, shard, replica = cls.parse_replicated_db_zk_info(live_db, macros)

        logging.debug(
            f"Database {db.name}: ZK path={zk_path!r}, shard={shard!r}, replica={replica!r}."
        )

        zk_data = cls(
            zk_ctl=context.zk_ctl, zk_path=zk_path, shard=shard, replica=replica
        )
        zk_data.max_log_ptr = zk_data.get_max_log_ptr()

        logging.debug(f"Database {db.name}: max_log_ptr={zk_data.max_log_ptr}")
        return zk_data


def _sync_database(context: BackupContext, db: Database, deadline: float) -> None:
    """
    Synchronize a single replicated database by polling ZooKeeper log pointers.
    Raises RuntimeError if ZK parameters cannot be resolved, sync is stuck, or timed out.
    """
    cfg = context.ch_ctl_conf
    poll_interval = cfg["sync_database_replica_poll_interval"]
    stall_threshold = cfg["sync_database_replica_stall_threshold"]

    zk_data = DatabaseZookeeperData.resolve(context, db)

    prev_log_ptr = log_ptr = zk_data.get_log_ptr()
    stall_count = 0

    while max(int(deadline - time.time()), 0) > 0:
        if log_ptr >= zk_data.max_log_ptr:
            logging.info(
                f"Database {db.name}: replica log_ptr={log_ptr} reached "
                f"max_log_ptr={zk_data.max_log_ptr}, sync complete"
            )
            return

        if log_ptr == prev_log_ptr:
            stall_count += 1
            if _check_ddl_queue_for_errors(context, db.name):
                raise RuntimeError(
                    f"Database {db.name}: log_ptr={log_ptr} not advancing "
                    f"and DDL queue has errors"
                )
            if stall_count >= stall_threshold:
                raise RuntimeError(
                    f"Database {db.name}: log_ptr={log_ptr} not advancing for "
                    f"{stall_count} consecutive polls (stall_threshold={stall_threshold}), "
                    f"max_log_ptr={zk_data.max_log_ptr}"
                )
        else:
            stall_count = 0

        logging.debug(
            f"Database {db.name}: log_ptr={log_ptr}, max_log_ptr={zk_data.max_log_ptr}, "
            f"stall_count={stall_count}"
        )

        time.sleep(poll_interval)

        prev_log_ptr = log_ptr
        log_ptr = zk_data.get_log_ptr()

    raise RuntimeError(
        f"Database {db.name}: sync timed out "
        f"(log_ptr={log_ptr}, max_log_ptr={zk_data.max_log_ptr})"
    )


def _check_ddl_queue_for_errors(context: BackupContext, db_name: str) -> bool:
    """
    Query system.distributed_ddl_queue for entries with errors for the given database.
    """
    try:
        entries = context.ch_ctl.get_ddl_queue_unfinished_status(db_name)
    except Exception as exc:  # pylint: disable=broad-except
        logging.warning(
            f"Database {db_name}: failed to query distributed_ddl_queue: {exc}"
        )
        return False

    error_entries = [
        entry for entry in entries if int(entry.get("exception_code", 0)) != 0
    ]

    for entry in error_entries:
        logging.warning(
            f"Stuck DDL entry in database {db_name}: "
            f"entry={entry.get('entry')}, "
            f"exception_code={entry.get('exception_code')}, "
            f"exception_text={str(entry.get('exception_text', ''))[:300]}"
        )

    return bool(error_entries)


def wait_sync_replicated_databases(
    context: BackupContext,
    databases: Iterable[Database],
    keep_going: bool,
) -> Dict[str, SyncStatus]:
    """
    Wait for replicated databases to sync by polling ZooKeeper log pointers.

    For each replicated database in the iterable:
    - Resolves ZooKeeper path, shard and replica from the database engine definition.
    - Polls replica log_ptr until it reaches max_log_ptr (sync complete).
    - If log_ptr stalls and DDL queue has errors, raises RuntimeError (caught → FAILED).
    - If ZK parameters cannot be resolved, raises RuntimeError (caught → FAILED).

    Args:
        context:    Backup context providing access to ClickHouse and ZooKeeper clients.
        databases:  Iterable of Database objects to synchronize.
        keep_going: If True, log warnings on failure and continue; otherwise re-raise on first error.

    Returns:
        Dict mapping database name to SyncStatus (DONE or FAILED).
        Non-replicated databases are not included in the result.
    """
    if context.config["force_non_replicated"]:
        logging.info("Skipping synchronizing replicated database replicas.")
        return {}

    logging.info("Synchronizing replicated database replicas")

    cfg = context.ch_ctl_conf
    deadline = time.time() + cfg["sync_database_replica_timeout"]
    results: Dict[str, SyncStatus] = {}

    for db in databases:
        if not db.is_replicated_db_engine():
            continue

        logging.info(f"Synchronizing replicated database: {db.name}")
        try:
            _sync_database(context, db, deadline)
            results[db.name] = SyncStatus.DONE
            logging.info(f"Database {db.name} sync completed successfully")
        except Exception as exc:  # pylint: disable=broad-except
            results[db.name] = SyncStatus.FAILED
            logging.warning(f"Database {db.name}: sync failed: {exc}")
            if not keep_going:
                raise

    return results
