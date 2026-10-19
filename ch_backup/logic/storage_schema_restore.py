"""Storage schema adaptation and coordinated Keeper reset during restore."""

import posixpath
import re
from contextlib import ExitStack
from hashlib import sha256

from kazoo.exceptions import NoNodeError

from ch_backup import logging
from ch_backup.backup_context import BackupContext
from ch_backup.clickhouse.metadata_cleaner import MetadataCleaner
from ch_backup.clickhouse.models import Table
from ch_backup.clickhouse.storage_schema import (
    StorageSchemaChange,
    StorageSchemaError,
    adapt_storage_schema,
    remove_unavailable_move_ttls,
    replicated_engine_path,
)
from ch_backup.exceptions import ClickhouseBackupError
from ch_backup.util import escape, replace_macros


class StorageSchemaKeeperError(ClickhouseBackupError):
    """A shared schema cannot be inspected or reset safely."""


class StorageSchemaRestore:
    """State belonging to one restore, never persisted in backup metadata."""

    def __init__(self, context: BackupContext, cleanup_enabled: bool) -> None:
        self.context = context
        self.cleanup_enabled = cleanup_enabled
        self.policies = context.ch_ctl.get_storage_policies()
        self.ch_config = context.ch_config.config
        # Reuse the same policy decisions for table creation and part placement.
        # 'resets' is planned under Keeper locks, then applied after local drops.
        self.changes: dict[tuple[str, str], StorageSchemaChange] = {}
        self.errors: dict[tuple[str, str], str] = {}
        self.paths: dict[tuple[str, str], str] = {}
        self.resets: set[tuple[str, str]] = set()

    def adapt(self, table: Table) -> None:
        """Adapt a rewritten table, recording placement and diagnostic details."""
        # ReplicatedMergeTree can override the common MergeTree storage default.
        # This matters when the saved CREATE has no explicit storage_policy.
        default = (self.ch_config.get("merge_tree") or {}).get(
            "storage_policy", "default"
        )
        if table.is_replicated():
            default = (self.ch_config.get("replicated_merge_tree") or {}).get(
                "storage_policy", default
            )
        change = adapt_storage_schema(table.create_statement, self.policies, default)
        if change is None:
            return
        # Modify the table prepared for restore, keeping the selected policy even
        # for unchanged DDL: the target policy may use different physical disks.
        key = (table.database, table.name)
        self.changes[key] = change
        table.create_statement = change.statement
        if change.previous_policy != change.policy.name:
            logging.warning(
                "Adapting {}.{} storage policy: {} -> {}",
                *key,
                change.previous_policy,
                change.policy.name,
            )
        for rule in change.removed_ttls:
            logging.warning(
                "Removing TTL MOVE from {}.{}: {}; destination is absent from policy {}",
                *key,
                rule,
                change.policy.name,
            )

    def changed(self, table: Table) -> bool:
        """Whether the local table must not be repaired using its old schema."""
        change = self.changes.get((table.database, table.name))
        return bool(change and change.changed)

    @property
    def storage_disks(self) -> dict[tuple[str, str], tuple[str, ...]]:
        """Local target disks; a retained policy can have different disks."""
        return {key: change.policy.local_disks for key, change in self.changes.items()}

    def lock_tables(self, tables: list[Table], stack: ExitStack) -> None:
        """Hold per-path locks through preprocessing and creation of all tables."""
        replicated = [
            table
            for table in tables
            if table.is_replicated() and (table.database, table.name) in self.changes
        ]
        if not replicated:
            return
        # Keeper reads and cleanup must address the same namespace as ClickHouse.
        # Fail before acquiring locks or changing any table if this is not known.
        if not self.context.zk_config.get("hosts"):
            raise StorageSchemaKeeperError(
                "--skip-cloud-storage with replicated tables requires ch-backup ZooKeeper configuration"
            )
        root = self.context.zk_ctl.zk_root_path.rstrip("/")
        server_root = (
            (self.ch_config.get("zookeeper") or {}).get("root") or ""
        ).rstrip("/")
        if root != server_root:
            raise StorageSchemaKeeperError(
                "ClickHouse and ch-backup ZooKeeper roots differ; cannot adapt shared schema"
            )
        macros = self.context.ch_ctl.get_macros()
        for table in replicated:
            self.paths[(table.database, table.name)] = self._table_path(table, macros)
        client = stack.enter_context(self.context.zk_ctl.zk_client)
        # A fixed order prevents concurrent restores from taking locks in opposite
        # orders. Locks live outside table roots, which cleanup may delete entirely.
        for path in sorted(set(self.paths.values())):
            full_path = root + path
            lock_path = (
                root + "/ch-backup/storage-schema/" + sha256(path.encode()).hexdigest()
            )
            lock = client.Lock(lock_path, identifier="restore --skip-cloud-storage")
            acquired = lock.acquire(timeout=self.context.lock_conf["lock_timeout"])
            if not acquired:
                raise StorageSchemaKeeperError(
                    f"Cannot acquire storage schema lock for {full_path}"
                )
            # The caller's ExitStack holds this lock until the new CREATE finishes,
            # so the next replica inspects the newly published shared schema.
            stack.callback(lock.release)
        # Inspect current shared TTL, not only the backup's TTL. The first replica
        # may already have replaced it, in which case another reset is unnecessary.
        for table in replicated:
            key = (table.database, table.name)
            path = root + self.paths[key]
            try:
                metadata, _ = client.get(path + "/metadata")
            except NoNodeError:
                continue
            ttl = next(
                (
                    line[5:]
                    for line in metadata.decode().splitlines()
                    if line.startswith("ttl: ")
                ),
                "",
            )
            _, removed = remove_unavailable_move_ttls(ttl, self.changes[key].policy)
            if removed:
                # A shared TTL mismatch requires recreating the whole table root;
                # deleting just this replica would leave the incompatible schema.
                if not self.cleanup_enabled:
                    raise StorageSchemaKeeperError(
                        f"TTL at {path} requires resetting all replicas; ZooKeeper cleanup is disabled"
                    )
                self.resets.add(key)

    def reset_keeper(self, tables: list[Table]) -> None:
        """Reset only incompatible table roots while their creation lock is held."""
        for table in tables:
            key = (table.database, table.name)
            if key not in self.resets:
                continue
            path = self.paths[key]
            logging.warning(
                "Resetting all replica metadata for {}.{} at {} to restore without unavailable TTL destinations",
                *key,
                path,
            )
            # MetadataCleaner reads paths from engine arguments. Supply the exact
            # resolved path, including for tables that used default engine args.
            cleanup_table = Table.make_dummy(table.database, table.name, table.uuid)
            cleanup_table.create_statement = (
                f"ENGINE = ReplicatedMergeTree('{escape(path)}', 'restore')"
            )
            # No replica filter: every replica belongs to the incompatible schema.
            cleaner = MetadataCleaner(
                self.context.ch_ctl,
                self.context.zk_ctl,
                None,
                self.context.config_root["multiprocessing"]["drop_replica_threads"],
            )
            try:
                cleaner.clean_tables_metadata([cleanup_table])
                with self.context.zk_ctl.zk_client as client:
                    full_path = self.context.zk_ctl.zk_root_path.rstrip("/") + path
                    # The cleaner may leave shared nodes behind. Remove the root
                    # only after confirming that all replica entries were cleaned.
                    try:
                        replicas = client.get_children(full_path + "/replicas")
                    except NoNodeError:
                        replicas = []
                    if replicas:
                        raise StorageSchemaKeeperError(
                            f"Replicas remain after cleanup of {full_path}: {replicas}"
                        )
                    try:
                        client.delete(full_path, recursive=True)
                    except NoNodeError:
                        pass
            except Exception as error:
                # Shared state is uncertain after a cleanup failure; unlike a
                # per-table DDL error, it must abort restore even with keep-going.
                raise StorageSchemaKeeperError(
                    f"Failed to reset shared metadata for {table.database}.{table.name}"
                ) from error

    def raise_errors(self) -> None:
        """Report per-table adaptation failures after independent work completes."""
        if self.errors:
            details = "; ".join(
                f"{db}.{table}: {error}" for (db, table), error in self.errors.items()
            )
            raise StorageSchemaError(f"Storage schema adaptation failed: {details}")

    def _table_path(self, table: Table, macros: dict) -> str:
        try:
            path = replicated_engine_path(table.create_statement)
        except StorageSchemaError as error:
            raise StorageSchemaKeeperError(
                f"Cannot resolve replicated engine of {table.database}.{table.name}"
            ) from error
        if path is None:
            # Engines without explicit arguments use ClickHouse's configured path.
            path = self.ch_config.get(
                "default_replica_path", "/clickhouse/tables/{uuid}/{shard}"
            )
        if "{uuid}" in path and not table.uuid:
            raise StorageSchemaKeeperError(
                "Cannot resolve a table path requiring a missing UUID"
            )
        # Use the final table name/UUID from schema rewriting, plus server macros,
        # so locks and cleanup address the path that CREATE will actually use.
        resolved = replace_macros(
            path,
            {
                **macros,
                "database": table.database,
                "table": table.name,
                "uuid": table.uuid or "",
            },
        )
        if not resolved.startswith("/") or re.search(r"[{}]", resolved):
            raise StorageSchemaKeeperError(
                f"Cannot resolve ZooKeeper path {path!r} for {table.database}.{table.name}"
            )
        resolved = posixpath.normpath(resolved)
        # Cleanup deletes recursively. Reject paths that could include the Keeper
        # root or the locks protecting this restore, including their ancestors.
        if (
            resolved == "/"
            or "/ch-backup/storage-schema".startswith(resolved + "/")
            or resolved.startswith("/ch-backup/storage-schema")
        ):
            raise StorageSchemaKeeperError(
                "Refusing to use the ZooKeeper root or schema lock namespace as a table path"
            )
        return resolved


def restore_disk_name(
    table: Table,
    source_disk: str,
    storage_disks: dict[tuple[str, str], tuple[str, ...]] | None,
) -> str:
    """Map local parts only; callers must filter object-storage parts first."""
    key = (table.database, table.name)
    if storage_disks is None or key not in storage_disks:
        return source_disk
    disks = storage_disks[key]
    if source_disk in disks:
        return source_disk
    # The snapshot keeps volume priority and disk order. If the original local
    # disk is absent, place the part on the first local disk of the target policy.
    if not disks:
        raise StorageSchemaError(
            f"No local disk in target policy of {table.database}.{table.name}"
        )
    logging.info(
        "Restoring local parts of {}.{} from disk {} to {}", *key, source_disk, disks[0]
    )
    return disks[0]
