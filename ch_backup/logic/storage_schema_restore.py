"""Storage schema adaptation and Keeper compatibility checks during restore."""

import posixpath
import re
from contextlib import ExitStack
from hashlib import sha256

from kazoo.exceptions import LockTimeout, NoNodeError

from ch_backup import logging
from ch_backup.backup_context import BackupContext
from ch_backup.clickhouse.models import Table
from ch_backup.clickhouse.storage_schema import (
    StorageSchemaChange,
    StorageSchemaError,
    adapt_storage_schema,
    remove_unavailable_move_ttls,
    replicated_engine_path,
)
from ch_backup.exceptions import ClickhouseBackupError
from ch_backup.util import compare_schema, replace_macros


class StorageSchemaKeeperError(ClickhouseBackupError):
    """A shared schema cannot be inspected or is incompatible with the restore."""


class StorageSchemaRestore:
    """State belonging to one restore, never persisted in backup metadata."""

    def __init__(self, context: BackupContext) -> None:
        self.context = context
        self.policies = context.ch_ctl.get_storage_policies()
        self.ch_config = context.ch_config.config
        # Reuse the same policy decisions for table creation and part placement.
        self.changes: dict[tuple[str, str], StorageSchemaChange] = {}
        self.errors: dict[tuple[str, str], str] = {}

    def adapt(self, table: Table) -> None:
        """Adapt a rewritten table, recording placement and diagnostic details."""
        # ReplicatedMergeTree can override the common MergeTree storage default.
        # This matters when the saved CREATE has no explicit storage_policy.
        defaults = dict(self.ch_config.get("merge_tree") or {})
        if table.is_replicated():
            defaults.update(self.ch_config.get("replicated_merge_tree") or {})
        default = defaults.get("storage_policy", "default")
        change = adapt_storage_schema(table.create_statement, self.policies, default)
        if change is None:
            return
        if change.removed_disk is not None:
            # Removing the table setting must actually select a policy. An
            # inherited disk would bypass it and invalidate our part placement.
            if "disk" in defaults:
                raise StorageSchemaError(
                    "Cannot remove table disk: target MergeTree defaults specify disk instead of a storage policy"
                )
            if not change.policy.local_disks:
                raise StorageSchemaError(
                    f"No local disk in target policy {change.policy.name!r} after removing table disk"
                )
        # Modify the table prepared for restore, keeping the selected policy even
        # for unchanged DDL: the target policy may use different physical disks.
        key = (table.database, table.name)
        self.changes[key] = change
        table.create_statement = change.statement
        if change.removed_disk is not None:
            logging.warning(
                "Removing {}.{} disk setting: {}; using target storage policy {}",
                *key,
                change.removed_disk,
                change.policy.name,
            )
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
        return change is not None and change.changed

    @property
    def storage_disks(self) -> dict[tuple[str, str], tuple[str, ...]]:
        """Local target disks; a retained policy can have different disks."""
        return {key: change.policy.local_disks for key, change in self.changes.items()}

    def lock_tables(self, tables: list[Table], stack: ExitStack) -> None:
        """Hold per-path locks through preprocessing and creation of all tables."""
        replicated = [
            table
            for table in tables
            if table.is_replicated()
            and (change := self.changes.get((table.database, table.name))) is not None
            and len(change.removed_ttls) > 0
        ]
        if len(replicated) == 0:
            return
        # Only a rewritten table TTL changes shared Keeper metadata. Policy-only
        # and unchanged schemas retain the ordinary restore requirements.
        # Keeper reads must address the same namespace as ClickHouse.
        # Fail before acquiring locks or changing any table if this is not known.
        if (
            self.context.zk_config.get("hosts") is None
            or len(self.context.zk_config["hosts"]) == 0
        ):
            raise StorageSchemaKeeperError(
                "Adapting a replicated table TTL requires ch-backup ZooKeeper configuration to check existing metadata"
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
        paths = {
            (table.database, table.name): self._table_path(table, macros)
            for table in replicated
        }
        client = stack.enter_context(self.context.zk_ctl.zk_client)
        # A fixed order prevents concurrent restores from taking locks in opposite
        # orders. Locks live outside table metadata and are held through creation.
        for path in sorted(set(paths.values())):
            lock = client.Lock(
                root + "/ch-backup/storage-schema/" + sha256(path.encode()).hexdigest(),
                identifier="restore --skip-cloud-storage",
            )
            try:
                acquired = lock.acquire(timeout=self.context.lock_conf["lock_timeout"])
            except LockTimeout as error:
                raise StorageSchemaKeeperError(
                    f"Cannot acquire storage schema lock for {root + path}"
                ) from error
            if not acquired:
                raise StorageSchemaKeeperError(
                    f"Cannot acquire storage schema lock for {root + path}"
                )
            # The caller's ExitStack holds this lock until the new CREATE finishes,
            # so the next replica inspects the newly published shared schema.
            stack.callback(lock.release)
        # Inspect the current shared TTL before any local drop or normal replica
        # cleanup. Compatible metadata can belong to an already restored replica.
        for table in replicated:
            path = root + paths[(table.database, table.name)]
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
            _, removed = remove_unavailable_move_ttls(
                ttl, self.changes[(table.database, table.name)].policy
            )
            if len(removed) > 0:
                raise StorageSchemaKeeperError(
                    f"Cannot adapt storage schema of {table.database}.{table.name}: "
                    f"Keeper metadata at {path} contains TTL MOVE destinations "
                    f"unavailable in target policy {self.changes[(table.database, table.name)].policy.name!r}. "
                    "Existing replica metadata was not changed. Restore to a separate "
                    "Keeper table path or resolve the metadata conflict before retrying."
                )

    def check_existing_tables(self, tables: list[Table]) -> None:
        """Do not turn disk removal into destructive replacement of a live table."""
        rewritten = [
            table
            for table in tables
            if (change := self.changes.get((table.database, table.name))) is not None
            and change.removed_disk is not None
        ]
        if len(rewritten) == 0:
            return
        existing = [
            *self.context.ch_ctl.get_tables(short_query=True),
            *self.context.ch_ctl.get_detached_tables(),
        ]
        by_name = {(table.database, table.name): table for table in rewritten}
        by_uuid = {
            table.uuid: table
            for table in rewritten
            if table.uuid is not None and table.uuid != ""
        }
        for current in existing:
            table = by_name.get((current.database, current.name))
            if table is None and current.uuid is not None and current.uuid != "":
                table = by_uuid.get(current.uuid)
            if table is not None and not compare_schema(
                current.create_statement, table.create_statement
            ):
                # Cloud parts are intentionally skipped, so DROP could discard
                # data that this restore will never replace. Abort the whole
                # preflight, also with keep-going, before database-level drops.
                raise StorageSchemaError(
                    f"Cannot remove disk setting for existing table {current.database}.{current.name}: "
                    "storage adaptation would require dropping the table while cloud parts are skipped. "
                    "Restore to an empty destination or remove the existing table explicitly before retrying."
                )

    def raise_errors(self) -> None:
        """Report per-table adaptation failures after independent work completes."""
        if len(self.errors) > 0:
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
        if "{uuid}" in path and (table.uuid is None or table.uuid == ""):
            raise StorageSchemaKeeperError(
                "Cannot resolve a table path requiring a missing UUID"
            )
        # Use the final table name/UUID from schema rewriting, plus server macros,
        # so locks and checks address the path that CREATE will actually use.
        resolved = replace_macros(
            path,
            {
                **macros,
                "database": table.database,
                "table": table.name,
                "uuid": table.uuid or "",
            },
        )
        if not resolved.startswith("/") or re.search(r"[{}]", resolved) is not None:
            raise StorageSchemaKeeperError(
                f"Cannot resolve ZooKeeper path {path!r} for {table.database}.{table.name}"
            )
        resolved = posixpath.normpath(resolved)
        # Table metadata must not overlap the root or the lock namespace.
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
    if len(disks) == 0:
        raise StorageSchemaError(
            f"No local disk in target policy of {table.database}.{table.name}"
        )
    logging.info(
        "Restoring local parts of {}.{} from disk {} to {}", *key, source_disk, disks[0]
    )
    return disks[0]
