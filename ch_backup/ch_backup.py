"""
Clickhouse backup logic
"""

# pylint: disable=too-many-lines

import builtins
import os
from collections import defaultdict
from contextlib import ExitStack, suppress
from copy import copy, deepcopy
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from functools import partial
from typing import Any, Callable, Mapping, Sequence

import requests

from ch_backup import logging
from ch_backup.backup.checker import BackupChecker, BackupCheckResult
from ch_backup.backup.cloud_cleanup import CloudObjectDeletionIndex
from ch_backup.backup.copier import BackupCopier
from ch_backup.backup.copy_deduplication import copy_deduplication_cutoff
from ch_backup.backup.copy_report import CopyReport
from ch_backup.backup.deduplication import (
    DedupReferences,
    collect_dedup_info,
    collect_dedup_references_for_batch_backup_deletion,
)
from ch_backup.backup.layout import BackupLayout
from ch_backup.backup.metadata import (
    BackupMetadata,
    BackupState,
    CopyScope,
    CopyScopeStatus,
    TableMetadata,
    sanitize_backup_name,
)
from ch_backup.backup.replica_layout import SOURCE_SETTINGS, ReplicaLayout
from ch_backup.backup.sources import BackupSources
from ch_backup.backup_context import BackupContext
from ch_backup.clickhouse.client import ClickhouseError
from ch_backup.clickhouse.control import ClickhouseCTL
from ch_backup.clickhouse.disks import ClickHouseTemporaryDisks
from ch_backup.clickhouse.metadata_cleaner import MetadataCleaner, select_replica_drop
from ch_backup.clickhouse.models import Database, Table
from ch_backup.clickhouse.part_checksums import CLICKHOUSE_BINARY
from ch_backup.config import Config, as_datetime
from ch_backup.encryption import validate_cloud_storage_data_encryption_config
from ch_backup.exceptions import (
    BackupNotFound,
    ClickhouseBackupError,
    ConfigurationError,
    InvalidBackupStruct,
    StorageError,
    TerminatingSignal,
)
from ch_backup.logic.access import AccessBackup
from ch_backup.logic.database import DatabaseBackup
from ch_backup.logic.database_sync import SyncStatus, wait_sync_replicated_databases
from ch_backup.logic.http_handlers import HTTPHandlersBackup
from ch_backup.logic.lock_manager import storage_locks
from ch_backup.logic.named_collections import NamedCollectionsBackup
from ch_backup.logic.partial_restore import PartialRestoreFilter
from ch_backup.logic.table import TableBackup
from ch_backup.logic.udf import UDFBackup
from ch_backup.logic.workload_entities import WorkloadEntitiesBackup
from ch_backup.storage.async_pipeline.stages import EncryptStage
from ch_backup.storage.engine import delete_key_batches, get_storage_engine
from ch_backup.storage.engine.s3 import validate_s3_retry_config
from ch_backup.storage.engine.s3.s3_config import validate_s3_module_config
from ch_backup.storage.object_copy import (
    normalize_s3_endpoint,
    parse_server_side_copy_mode,
    prefixes_overlap,
    same_storage,
)
from ch_backup.util import cached_property, now, utcnow
from ch_backup.version import get_version


class CleanZooKeeperMode(str, Enum):
    """
    What to clean in ZooKeeper
    """

    DISABLED = "disabled"
    REPLICA_ONLY = "replica-only"
    ALL_REPLICAS = "all-replicas"


@dataclass(frozen=True)
class _CloudCleanupPlan:
    """Prepared cloud operations for one backup's deletion.

    A retained cloud part keeps the owner's archives and objects together.
    The executor records completed external deletion before removing archives.
    Cloud files in backup storage are removed separately because local parts
    can keep the rest of the backup alive.
    """

    empty_link_disks: frozenset[str] = frozenset()
    keep_frozen_data: bool = False
    delete_external_payload: Callable[[], None] | None = None
    delete_backup_cloud_files: Callable[[], None] | None = None


# pylint: disable=too-many-instance-attributes
class ClickhouseBackup:
    """
    Clickhouse backup logic
    """

    # pylint: disable=too-many-instance-attributes

    def __init__(self, config: Config) -> None:
        self._validate_config(config)
        self._config = config
        self._access_backup_manager = AccessBackup()
        self._database_backup_manager = DatabaseBackup()
        self._table_backup_manager = TableBackup()
        self._http_handlers_backup_manager = HTTPHandlersBackup()
        self._udf_backup_manager = UDFBackup()
        self._nc_backup_manager = NamedCollectionsBackup()
        self._we_backup_manager = WorkloadEntitiesBackup()

    @staticmethod
    def _validate_config(config: Config) -> None:
        """Validate operation settings and explicit destructive policies."""
        if not isinstance(config["backup"].get("delete_unreadable", False), bool):
            raise ValueError("backup.delete_unreadable must be true or false")
        parse_server_side_copy_mode(config["copy"]["server_side_copy_mode"])
        copy_deduplication_cutoff(config["copy"], utcnow())
        start_time = as_datetime(config["backup"].get("deduplication_start_time"))
        if start_time is not None and start_time > utcnow():
            raise ValueError(
                "backup.deduplication_start_time must not be in the future"
            )
        if config["storage"]["type"] == "s3":
            validate_s3_retry_config(config["storage"])
            validate_s3_module_config(config["storage"])

    @property
    def config(self) -> Config:
        """
        Returns current config.
        """
        return self._config

    @cached_property
    def _context(self) -> BackupContext:
        """
        Create and configure BackupContext
        """
        ctx = BackupContext(self._config)
        return ctx

    def reload_config(self, config: Config) -> None:
        """
        Completely reloads the config.
        """
        self._validate_config(config)
        logging.info("Reloading config.")
        del self._context
        self._config = config
        logging.info("Config reloaded.")

    def get(self, backup_name: str) -> BackupMetadata:
        """
        Get backup information.
        """
        return self._get_backup(backup_name)

    def pending_deletions(self) -> dict[str, dict]:
        """Return durable deletion intents for diagnostics and explicit retry."""
        return self._context.backup_layout.get_pending_deletions()

    def list(
        self,
        state: BackupState = None,
        *,
        unreadable: dict[str, str] | None = None,
        pending: dict[str, dict] | None = None,
    ) -> Sequence[BackupMetadata]:
        """
        Get list of existing backups sorted by start timestamp.
        If unreadable is provided, collect per-backup read errors there.
        """
        backups = []
        for backup in self._context.backup_layout.get_backups(
            use_light_meta=True, unreadable=unreadable, pending=pending
        ):
            if state and backup.state != state:
                continue
            backups.append(backup)

        return backups

    # pylint: disable=too-many-positional-arguments
    def backup(
        self,
        sources: BackupSources,
        name: str,
        db_names: Sequence[str] = None,
        tables: Sequence[str] = None,
        force: bool = False,
        labels: dict = None,
        include_detached_parts: bool = False,
        detached_parts_disks: Sequence[str] = None,
        detached_parts_reasons: Sequence[str] = None,
    ) -> tuple[str, str | None]:
        """
        Perform backup.

        If force is True, backup.min_interval config option is ignored.
        """
        # pylint: disable=too-many-branches
        # pylint: disable=too-many-locals
        # pylint: disable=too-many-statements
        backups_with_light_meta = (
            self._context.backup_layout.get_backups(use_light_meta=True, unreadable={})
            if not force or self._context.config.get("deduplicate_parts")
            else []
        )

        sanitized_name = sanitize_backup_name(name)
        for backup_name in self._context.backup_layout.get_backup_names():
            if name == backup_name:
                raise ClickhouseBackupError(f"Backup with name {name} already exists")
            if sanitized_name == sanitize_backup_name(backup_name):
                raise ClickhouseBackupError(
                    f"Backup with name {name} conflicts with existing backup "
                    f"{backup_name}: names that differ only in '-' and '_' share "
                    "the same cloud storage path"
                )

        logging.info(f"Backup sources: {sources}")
        assert not (db_names and tables)

        if detached_parts_disks is None:
            detached_parts_disks = ["object_storage"]

        if isinstance(detached_parts_disks, str):
            detached_parts_disks = [detached_parts_disks]
        if isinstance(detached_parts_reasons, str):
            detached_parts_reasons = [detached_parts_reasons]

        backup_labels: dict = copy(self._context.config.get("labels"))  # type: ignore
        if labels:
            backup_labels.update(labels)

        db_tables: dict[str, list] = defaultdict(list)
        if tables:
            for table in tables or []:
                db_name, table_name = table.split(".", 1)
                db_tables[db_name].append(table_name)

            db_names = list(db_tables.keys())

        databases = self._context.ch_ctl.get_databases(
            self._context.config["exclude_dbs"]
        )
        if db_names is not None:
            databases = [db for db in databases if db.name in db_names]

        last_backup = next(iter(backups_with_light_meta), None)
        if last_backup and not self._check_min_interval(last_backup, force):
            msg = "Backup is skipped per backup.min_interval config option."
            logging.info(msg)
            return last_backup.name, msg

        self._context.backup_meta = BackupMetadata(
            name=name,
            # DEPRECATED: ``path`` is populated for backward compatibility
            # with older ch-backup versions and is not used by new code.
            path=self._context.backup_layout.get_backup_path(name),
            labels=backup_labels,
            version=get_version(),
            ch_version=self._context.ch_ctl.get_version(),
            time_format=self._context.config["time_format"],
            schema_only=sources.schema_only,
            encrypted=self._config.get(EncryptStage.stype, {}).get("enabled", True),
        )

        skip_lock = self._check_schema_only_backup_skip_lock(sources)

        with self._context.locker(disabled=skip_lock, operation="BACKUP"):
            if not force or self._context.config.get("deduplicate_parts"):
                backups_with_light_meta = self._context.backup_layout.get_backups(
                    use_light_meta=True, unreadable={}
                )
            for existing in self._context.backup_layout.get_backup_names():
                if sanitize_backup_name(existing) == sanitized_name:
                    raise ClickhouseBackupError(
                        f"Backup {name} conflicts with existing backup {existing}"
                    )
            self._context.backup_layout.upload_backup_metadata(
                self._context.backup_meta
            )
            logging.debug(
                'Starting backup "%s" for databases: %s',
                self._context.backup_meta.name,
                ", ".join(map(lambda db: db.name, databases)),
            )
            try:
                if sources.access:
                    self._access_backup_manager.backup(self._context)
                if sources.udf:
                    self._udf_backup_manager.backup(self._context)
                if sources.named_collections:
                    self._nc_backup_manager.backup(self._context)
                if sources.workload_entities:
                    self._we_backup_manager.backup(self._context)
                if sources.http_handlers:
                    self._http_handlers_backup_manager.backup(self._context)
                if sources.schemas_included():
                    databases = self._database_backup_manager.backup(
                        self._context, databases
                    )
                    collect_dedup_info(
                        context=self._context,
                        backups_with_light_meta=backups_with_light_meta,
                        databases=databases,
                    )
                    self._table_backup_manager.backup(
                        self._context,
                        databases,
                        db_tables,
                        schema_only=sources.schema_only,
                        multiprocessing_config=self._config["multiprocessing"],
                        include_detached_parts=include_detached_parts,
                        detached_parts_disks=detached_parts_disks,
                        detached_parts_reasons=detached_parts_reasons,
                    )

                # Upload operations are async. Should wait until they are all finished.
                self._context.backup_layout.wait()
                self._context.backup_meta.state = BackupState.CREATED
            except (Exception, TerminatingSignal) as e:
                logging.critical("Backup failed", exc_info=True)
                self._context.backup_meta.state = BackupState.FAILED
                self._context.backup_meta.exception = f"{type(e).__name__}: {e}"
                raise
            finally:
                self._context.backup_meta.update_end_time()
                self._context.backup_layout.upload_backup_metadata(
                    self._context.backup_meta
                )

                if not self._context.config.get("keep_freezed_data_on_failure"):
                    self._context.ch_ctl.remove_freezed_data()

        return self._context.backup_meta.name, self._skipped_data_message()

    def _skipped_data_message(self) -> str | None:
        """
        Report tables that were backed up without some or all of their data.
        """
        data_skipped_tables, incomplete_tables = (
            self._context.backup_meta.count_skipped_data_tables()
        )
        if not (data_skipped_tables or incomplete_tables):
            return None

        return (
            f"Backup is incomplete: {data_skipped_tables} tables without data,"
            f" {incomplete_tables} tables without some data parts. Run show to see"
            " them by data_skipped_reason and skipped_parts."
        )

    # pylint: disable=too-many-arguments,duplicate-code,too-many-positional-arguments,too-many-branches,too-many-locals,too-many-statements
    def restore(
        self,
        sources: BackupSources,
        backup_name: str,
        databases: Sequence[str],
        exclude_databases: Sequence[str],
        override_replica_name: str = None,
        force_non_replicated: bool = False,
        replica_name: str | None = None,
        cloud_storage_source_bucket: str = None,
        cloud_storage_source_path: str = None,
        cloud_storage_source_endpoint: str = None,
        skip_cloud_storage: bool = False,
        restore_detached_parts: bool = False,
        clean_zookeeper_mode: CleanZooKeeperMode = CleanZooKeeperMode.DISABLED,
        keep_going: bool = False,
        restore_tables_in_replicated_database: bool = False,
        partial_restore_filter: PartialRestoreFilter | None = None,
        parts_backup_name: str | None = None,
    ) -> None:
        """
        Restore specified backup
        """
        logging.info(f"Restore sources: {sources}")
        self._context.backup_meta = self._get_backup(backup_name)
        if (
            sources.data
            and not skip_cloud_storage
            and self._context.backup_meta.cloud_storage.data_encryption is not None
        ):
            validate_cloud_storage_data_encryption_config(
                self._config,
                self._context.backup_meta.cloud_storage.data_encryption,
            )
        data_skipped_tables, incomplete_tables = (
            self._context.backup_meta.get_skipped_data_tables()
        )
        if data_skipped_tables or incomplete_tables:
            logging.warning(
                "Backup holds no data of tables: {}; and only some parts of tables: {}",
                ", ".join(data_skipped_tables) or "none",
                ", ".join(incomplete_tables) or "none",
            )
        self._context.parts_backup_meta = self._get_parts_backup(
            sources, parts_backup_name
        )

        cloud_storage_source = self._context.backup_meta.cloud_storage.source
        if (
            cloud_storage_source_bucket is None
            and sources.data
            and not skip_cloud_storage
        ):
            if (
                self._context.backup_meta.cloud_storage.requires_source_bucket
                and self._context.backup_meta.select_disks_with_stored_cloud_parts()
                and not cloud_storage_source
            ):
                raise ClickhouseBackupError(
                    "Cloud storage source bucket must be set if backup has data on S3 disks"
                )

        self._context.config["override_replica_name"] = (
            override_replica_name or self._context.config.get("override_replica_name")
        )
        self._context.config["force_non_replicated"] = (
            force_non_replicated or self._context.config["force_non_replicated"]
        )

        if not databases:
            logging.debug("Picking all databases from backup.")
            databases = self._context.backup_meta.get_databases()
        else:
            logging.info("Checking for presence of required databases.")
            missed_databases = [
                db_name
                for db_name in databases
                if db_name not in self._context.backup_meta.get_databases()
            ]
            if missed_databases:
                logging.critical(
                    "Required databases %s were not found in backup metadata: %s",
                    ", ".join(missed_databases),
                    self._context.backup_meta.name,
                )
                raise ClickhouseBackupError(
                    "Required databases were not found in backup metadata"
                )

        if exclude_databases:
            logging.debug(
                f'Excluding specified databases from restoring: {", ".join(exclude_databases)}.'
            )
            databases = [db for db in databases if db not in exclude_databases]

        logging.debug(f"Picking databases to restore: {databases}.")

        tables = []
        if partial_restore_filter:
            excluded_tables = []
            all_tables: list[TableMetadata] = []
            for db_name in databases:
                all_tables.extend(self._context.backup_meta.get_tables(db_name))
            for table in all_tables:
                is_restoring = partial_restore_filter.accept_table(
                    table.database, table.name
                )
                logging.debug(
                    f"Filtering table to restore: {table.database}.{table.name} result: {is_restoring}."
                )
                if is_restoring:
                    tables.append(table)
                else:
                    excluded_tables.append(f"{table.database}.{table.name}")

            if excluded_tables:
                logging.debug(
                    f'Excluding specified tables from restoring: {", ".join(excluded_tables)}.'
                )
        else:
            logging.debug("Picking all tables from backup.")

        self._check_zookeeper_for_restore(sources, databases, tables)

        with self._context.locker(
            distributed=not sources.schema_only, operation="RESTORE"
        ):
            self._restore(
                sources=sources,
                db_names=databases,
                tables=tables,
                replica_name=replica_name,
                cloud_storage_source_bucket=cloud_storage_source_bucket,
                cloud_storage_source_path=cloud_storage_source_path,
                cloud_storage_source_endpoint=cloud_storage_source_endpoint,
                skip_cloud_storage=skip_cloud_storage,
                restore_detached_parts=restore_detached_parts,
                clean_zookeeper_mode=clean_zookeeper_mode,
                keep_going=keep_going,
                restore_tables_in_replicated_database=restore_tables_in_replicated_database,
            )

    def restore_from_replica(
        self,
        source_host: str,
        sources: BackupSources,
        replica_name: str | None = None,
    ) -> None:
        """
        Restore from a live replica of the shard instead of a backup.
        """
        source_ctl = ClickhouseCTL(
            {
                **self._context.ch_ctl_conf,
                "host": source_host,
                "settings": SOURCE_SETTINGS,
            },
            self._context.main_conf,
            self._context.config,
        )
        self._context.backup_layout = ReplicaLayout(
            self._context.config_root,
            self._context.ch_ctl,
            source_ctl,
            sources,
            replica_name,
        )
        self.restore(
            sources=sources,
            backup_name=source_host,
            databases=[],
            exclude_databases=[],
            replica_name=replica_name,
            clean_zookeeper_mode=CleanZooKeeperMode.REPLICA_ONLY,
        )

    def _check_zookeeper_for_restore(
        self,
        sources: BackupSources,
        databases: Sequence[str],
        tables: Sequence[TableMetadata],
    ) -> None:
        """Fail early if selected schemas require unavailable ZooKeeper or Keeper."""
        if not sources.schemas_included():
            return

        if self._context.config["force_non_replicated"]:
            return

        replicated_databases = [
            database
            for database in (
                self._context.backup_meta.get_database(name) for name in databases
            )
            if database.is_replicated_db_engine()
        ]

        tables_to_restore = tables or [
            table
            for database in databases
            for table in self._context.backup_meta.get_tables(database)
        ]
        replicated_tables = [
            table
            for table in tables_to_restore
            if Table.engine_is_replicated(table.engine)
        ]

        if not replicated_databases and not replicated_tables:
            return

        try:
            self._context.ch_ctl.check_zookeeper_available()
        except (ClickhouseError, requests.exceptions.RequestException) as error:
            replicated_objects = []
            if replicated_databases:
                replicated_objects.append(
                    "databases: "
                    + ", ".join(
                        f"`{database.name}`" for database in replicated_databases
                    )
                )
            if replicated_tables:
                replicated_objects.append(
                    "tables: "
                    + ", ".join(
                        f"`{table.database}`.`{table.name}`"
                        for table in replicated_tables
                    )
                )
            raise ClickhouseBackupError(
                f"Restore requires ZooKeeper or ClickHouse Keeper because we have replicated {', '.join(replicated_objects)}. "
                f"Availability check through ClickHouse failed: {error}"
            ) from error

    def adopt_cloud_storage(self, backup_name: str) -> None:
        """Record operator-confirmed legacy ownership under the shared writer lock."""
        if self._context.lock_conf.get("zk_flock") is not True:
            raise StorageError(
                "Legacy adoption requires lock.zk_flock shared by all writers"
            )
        with self._context.locker(operation="ADOPT"):
            self._context.backup_layout.claim_cloud_storage(backup_name, adopt=True)

    def delete(
        self,
        backup_name: str,
        purge_partial: bool,
        delete_cloud_storage_data: bool = False,
        *,
        delete_unreadable: bool = False,
    ) -> tuple[str | None, str | None]:
        """
        Delete the named backup and optionally purge other incomplete backups.
        """
        delete_unreadable = delete_unreadable or self._context.config.get(
            "delete_unreadable", False
        )
        found = False
        deleting_backups = []
        retained_backups = []
        unreadable: dict[str, str] = {}
        copying_since = self._copying_since()
        with self._context.locker(operation="DELETE"):
            layout = self._context.backup_layout
            pending = layout.get_pending_deletions()
            backups = layout.get_backups(
                use_light_meta=True, unreadable=unreadable, strict_inventory=True
            )
            if backup_name == "LAST":
                if unreadable or not backups:
                    raise StorageError(
                        "Cannot resolve LAST with missing or unreadable backups"
                    )
                backup_name = backups[0].name
            for i, backup in enumerate(backups):
                if backup.name == backup_name:
                    deleting_backups.append(backup)
                    found = True
                    continue

                if (
                    purge_partial
                    and backup.state != BackupState.CREATED
                    and not _is_partial_copy(backup)
                    and i != 0
                    and not _is_being_copied(backup, copying_since)
                ):
                    deleting_backups.append(backup)
                else:
                    retained_backups.append(backup)

            if blocking := set(unreadable) - {backup_name}:
                raise StorageError(
                    f"Failed to read metadata of backups: {', '.join(sorted(blocking))}. "
                    f"Backup {backup_name} cannot be deleted: references of these backups "
                    "to data parts are unknown. Repair their metadata before retrying."
                )
            if (
                not found
                and backup_name not in unreadable
                and backup_name not in pending
            ):
                raise BackupNotFound(backup_name)

            pending_names = (
                set(pending) if purge_partial else set(pending) & {backup_name}
            )
            broken = (
                {backup_name: unreadable[backup_name]}
                if backup_name in unreadable
                else {}
            )
            results = self._delete_batch(
                deleting_backups,
                retained_backups,
                pending_names,
                broken,
                delete_unreadable,
                delete_cloud_storage_data,
            )
            result = results.get(backup_name, (None, None))

        return result

    def copy(
        self,
        target_config: Config,
        backup_names: Sequence[str] | None = None,
        *,
        keep_going: bool = False,
        report: CopyReport | None = None,
        command_start: datetime | None = None,
    ) -> tuple[Sequence[str], Sequence[str]]:
        """
        Copy backups to the storage specified in the target config.
        If backup names are not specified, all backups in the state "created" are copied.
        Copy scopes are taken from the target configuration, all of them by default.

        Return names of copied backups and names of backups skipped as already copied.
        The optional report also receives partial results when copying raises.
        """
        # Freeze both window bounds before storage setup and lock acquisition.
        if command_start is None:
            command_start = utcnow()
        if report is not None:
            report.error = None
            report.backups.clear()
        try:
            self._validate_config(target_config)
            copier = BackupCopier(
                config=self._config,
                target_config=target_config,
                layout=self._context.backup_layout,
                target_layout=BackupLayout(target_config),
                ch_config=self._context.ch_config,
            )
            with storage_locks([self._config, target_config], operation="COPY"):
                return copier.copy(
                    backup_names,
                    keep_going=keep_going,
                    report=report,
                    command_start=command_start,
                )
        except BaseException as e:
            if report is not None:
                report.error = f"{type(e).__name__}: {e}"
            raise

    def check(
        self,
        backup_names: Sequence[str] | None = None,
        deep: bool = False,
        workers: int = 4,
        tmp_dir: str | None = None,
    ) -> builtins.list[BackupCheckResult]:
        """
        Check that backups are readable and their data is in place, reading them only.
        If backup names are not specified, all backups are checked.
        """
        return BackupChecker(self._config, deep, workers, tmp_dir).check(backup_names)

    def purge(
        self,
        delete_cloud_storage_data: bool = False,
        *,
        delete_unreadable: bool = False,
    ) -> tuple[Sequence[str], str | None]:
        """
        Purge backups.

        If a created backup or partial copy is retained but none is complete,
        keep the newest complete backup: it may hold the last copy of data
        the retained backups lack.
        """
        delete_unreadable = delete_unreadable or self._context.config.get(
            "delete_unreadable", False
        )
        retain_time = self._context.config["retain_time"]
        retain_count = self._context.config["retain_count"]

        deleted_backup_names: list[str] = []

        has_retention = bool(retain_time) or retain_count is not None

        retain_time_limit = None
        if retain_time:
            retain_time_limit = now() - timedelta(**retain_time)

        retained_backups: list[BackupMetadata] = []
        deleting_backups: list[BackupMetadata] = []
        copying_since = self._copying_since()

        with self._context.locker(operation="PURGE"):
            layout = self._context.backup_layout
            pending = layout.get_pending_deletions()
            if not has_retention and not pending:
                return [], "Retain policies are not specified."
            unreadable: dict[str, str] = {}
            backups = layout.get_backups(
                use_light_meta=True,
                unreadable=unreadable,
                strict_inventory=True,
            )
            if unreadable and (not delete_unreadable or not has_retention):
                raise StorageError(
                    f"Cannot collect references: unreadable metadata of {', '.join(sorted(unreadable))}"
                )
            for backup in backups:
                if not has_retention:
                    retained_backups.append(backup)
                    continue
                if retain_count is not None and retain_count > 0:
                    logging.info(
                        "Preserving backup per retain count policy: {}, state {}",
                        backup.name,
                        backup.state,
                    )
                    retained_backups.append(backup)
                    if backup.state == BackupState.CREATED or _is_partial_copy(backup):
                        retain_count -= 1
                    continue

                if retain_time_limit and backup.start_time >= retain_time_limit:
                    logging.info(
                        "Preserving backup per retain time policy: {}, state {}",
                        backup.name,
                        backup.state,
                    )
                    retained_backups.append(backup)
                    continue

                if _is_being_copied(backup, copying_since):
                    logging.info(
                        "Preserving copy of backup that is being copied: {}, state {}",
                        backup.name,
                        backup.state,
                    )
                    retained_backups.append(backup)
                    continue

                deleting_backups.append(backup)

            if any(
                backup.state == BackupState.CREATED or _is_partial_copy(backup)
                for backup in retained_backups
            ) and not any(map(_is_complete, retained_backups)):
                complete_backup = next(filter(_is_complete, deleting_backups), None)
                if complete_backup:
                    logging.info(
                        "Preserving the newest complete backup, the retained ones"
                        " hold incomplete data: {}",
                        complete_backup.name,
                    )
                    deleting_backups.remove(complete_backup)
                    retained_backups.append(complete_backup)

            results = self._delete_batch(
                deleting_backups,
                retained_backups,
                set(pending),
                unreadable,
                delete_unreadable,
                delete_cloud_storage_data,
            )
            deleted_backup_names.extend(
                name for name, (deleted, _) in results.items() if deleted
            )

        return deleted_backup_names, None

    def restore_access_control(self, backup_name: str) -> None:
        """Restore ClickHouse access control metadata."""
        self._context.backup_meta = self._get_backup(backup_name)
        with self._context.locker():
            self._access_backup_manager.restore(self._context)

    def fix_admin_user(self, dry_run: bool = True) -> None:
        """Check and fix potential duplicates of `admin` user in Keeper."""
        with self._context.locker():
            self._access_backup_manager.fix_admin_user(self._context, dry_run)

    def get_cloud_storage_metadata(
        self,
        backup_name: str,
        disk_name: str,
        local_path: str | None = None,
    ) -> bool:
        """Download cloud storage metadata to shadow directory or to a file at given path. Returns false if metadata is already present."""
        backup_meta = self._get_backup(backup_name)
        source_disk = self._context.ch_ctl.get_disk(disk_name)

        if (
            not local_path
            and self._context.backup_layout.cloud_storage_metadata_exists(
                backup_meta, source_disk
            )
        ):
            return False

        self._context.backup_layout.download_cloud_storage_metadata(
            backup_meta, source_disk, disk_name, "all", local_path
        )
        self._context.backup_layout.wait()

        return True

    def _copying_since(self) -> datetime | None:
        """Return the time after which an incomplete copy saved by the copy command is kept."""
        retain_time = self._context.config.get("retain_incomplete_copy_time")
        return now() - timedelta(**retain_time) if retain_time else None

    def _load_metadata_for_deletion(
        self, light_metadata: BackupMetadata, dedup_references: DedupReferences
    ) -> tuple[BackupMetadata, bool]:
        """Load part metadata needed to preserve references during deletion.

        If no backup references this one, allow whole-backup deletion using
        light metadata when full metadata is unavailable.
        """
        try:
            full_metadata = self._context.backup_layout.get_backup(light_metadata.name)
        except StorageError:
            if dedup_references:
                raise
            logging.exception(
                "Full metadata of backup {} is unreadable, deleting it by light metadata",
                light_metadata.name,
            )
            full_metadata = None
        if full_metadata is None and dedup_references:
            raise InvalidBackupStruct(
                f"Full metadata is missing for referenced backup {light_metadata.name}"
            )
        return full_metadata or light_metadata, full_metadata is None

    def _prepare_deletion_batch(
        self,
        backups_to_delete: Sequence[BackupMetadata],
        dedup_references: dict[str, DedupReferences],
        delete_cloud_storage_data: bool,
    ) -> builtins.list[tuple[BackupMetadata, bool, _CloudCleanupPlan]]:
        """Prepare each backup's deletion before changing any backup in the batch.

        Retain loaded metadata and planned cloud operations so execution uses
        the data validated here without downloading it again.
        A bad later backup must not cause earlier backups to be deleted.
        """
        prepared_deletions = []
        for light_metadata in backups_to_delete:
            references = dedup_references[light_metadata.name]
            backup, metadata_is_light = self._load_metadata_for_deletion(
                light_metadata, references
            )
            cloud_cleanup_action = self._prepare_backup_cloud_cleanup(
                backup, references, delete_cloud_storage_data
            )
            prepared_deletions.append((backup, metadata_is_light, cloud_cleanup_action))
        logging.info(
            "Deletion batch validated: backups={}, cloud_cleanup_actions={}; no deletion started during validation",
            len(prepared_deletions),
            sum(
                any(
                    (
                        plan.delete_external_payload,
                        plan.delete_backup_cloud_files,
                    )
                )
                for _, _, plan in prepared_deletions
            ),
        )
        return prepared_deletions

    def _execute_backup_deletion(
        self,
        backup: BackupMetadata,
        dedup_references: DedupReferences,
        metadata_is_light: bool,
        cloud_cleanup: _CloudCleanupPlan,
    ) -> tuple[str | None, str | None]:
        """
        Delete a backup, keeping the parts other backups link to.

        Without readable full metadata the backup is deleted whole by its light
        metadata, which is allowed only if nothing links to it.
        """
        logging.info(
            "Deleting backup {}, state: {}",
            backup.name,
            backup.state,
        )

        backup.state = BackupState.DELETING
        backup.exception = None

        try:
            self._context.backup_layout.upload_backup_metadata(
                backup, light_only=metadata_is_light
            )
            if cloud_cleanup.delete_external_payload:
                cloud_cleanup.delete_external_payload()
                backup.cloud_storage.source = None
                # Save completion before removing the archives that identify external
                # objects. A retry can then finish without those deletion inputs.
                self._context.backup_layout.upload_backup_metadata(
                    backup, light_only=metadata_is_light
                )
            if not dedup_references:
                logging.info("Removing backup data entirely")
                self._context.backup_layout.delete_backup(
                    backup.name,
                    lambda: self._system_unfreeze(backup),
                )
                return backup.name, None

            logging.info("Removing non-shared backup data parts")
            if cloud_cleanup.empty_link_disks:
                backup.cloud_storage.retain_disks(
                    set(backup.cloud_storage.disks) - cloud_cleanup.empty_link_disks
                )
            for db_name in backup.get_databases():
                db_dedup_references = dedup_references[db_name]
                for table in backup.get_tables(db_name):
                    self._delete_data_parts(
                        backup, table, db_dedup_references[table.name]
                    )
                    self._delete_detached_parts(backup, table)

            if cloud_cleanup.delete_backup_cloud_files:
                cloud_cleanup.delete_backup_cloud_files()
            self._context.backup_layout.wait()
            if not cloud_cleanup.keep_frozen_data:
                self._system_unfreeze(backup)
            backup.state = BackupState.PARTIALLY_DELETED
            self._context.backup_layout.upload_backup_metadata(
                backup, light_only=metadata_is_light
            )
            return (
                None,
                "Backup was partially deleted as its data is in use by subsequent backups per "
                "deduplication settings.",
            )

        except (Exception, TerminatingSignal) as e:
            logging.critical("Delete failed", exc_info=True)
            backup.state = BackupState.FAILED
            backup.exception = f"{type(e).__name__}: {e}"
            self._context.backup_layout.upload_backup_metadata(
                backup, light_only=metadata_is_light
            )
            raise

    def _delete_batch(
        self,
        deleting: builtins.list[BackupMetadata],
        retained: builtins.list[BackupMetadata],
        pending: set[str],
        unreadable: dict[str, str],
        delete_unreadable: bool,
        delete_cloud_storage_data: bool = False,
    ) -> dict[str, tuple[str | None, str | None]]:
        """Validate all retained references before changing deletion targets or intents."""
        layout = self._context.backup_layout
        normal = []
        for backup in deleting:
            if not delete_unreadable:
                normal.append(backup)
                continue
            try:
                full = layout.get_backup(backup.name)
                if full is None:
                    raise StorageError("missing_metadata")
            except Exception as error:
                unreadable[backup.name] = self._metadata_failure_reason(error)
            else:
                normal.append(backup)
        if delete_cloud_storage_data and (unreadable or pending):
            # Pending deletion can recover backup-owned data from its ownership
            # records, but cannot establish an unreadable copy's external target.
            raise InvalidBackupStruct(
                "Cannot delete external cloud storage data without readable backup metadata"
            )
        if unreadable and not delete_unreadable:
            raise StorageError(
                f"Metadata is unavailable for {', '.join(sorted(unreadable))}; use --delete-unreadable to devalue these backups"
            )
        if unreadable or pending:
            if self._context.lock_conf.get("zk_flock") is not True:
                raise StorageError(
                    "Pending deletion requires lock.zk_flock shared by all backup/copy/delete writers"
                )
        pending |= set(unreadable)
        for name in pending:
            descriptor = layout.get_cloud_storage_backup(name)
            if descriptor and descriptor.cloud_storage.source:
                self._check_cloud_deletion_overlap(
                    name, descriptor.cloud_storage.source
                )
            for location, _keys in layout.read_cloud_copy_manifest(name):
                self._check_cloud_deletion_overlap(name, location)
        referenced_by: dict[str, set[str]] = {}
        references = collect_dedup_references_for_batch_backup_deletion(
            layout=layout,
            retained_backups_light_meta=retained,
            deleting_backup_names={backup.name for backup in normal} | pending,
            referenced_by=referenced_by,
        )
        prepared_deletions = self._prepare_deletion_batch(
            normal, references, delete_cloud_storage_data
        )
        for name, reason in unreadable.items():
            layout.save_delete_pending(
                name,
                (
                    reason
                    if reason
                    in {"corrupt_metadata", "missing_metadata", "metadata_unavailable"}
                    else "metadata_unavailable"
                ),
            )
        results = {}
        for backup, metadata_is_light, cloud_cleanup in prepared_deletions:
            results[backup.name] = self._execute_backup_deletion(
                backup,
                references[backup.name],
                metadata_is_light,
                cloud_cleanup,
            )
        for name in sorted(pending):
            try:
                complete = layout.clean_pending_backup(
                    name, references[name], referenced_by.get(name, set())
                )
            except (Exception, TerminatingSignal) as error:
                with suppress(Exception):
                    layout.save_delete_pending(
                        name,
                        "retry",
                        last_error=f"{type(error).__name__}: cleanup failed",
                    )
                raise
            results[name] = (
                (name, "Frozen data was kept because backup provenance is unknown.")
                if complete
                else (
                    None,
                    f"Backup {name} is partially deleted; deletion remains pending",
                )
            )
        return results

    @staticmethod
    def _metadata_failure_reason(error: BaseException) -> str:
        """Classify missing, malformed, or unavailable metadata without persisting secrets."""
        while error.__cause__ is not None:
            error = error.__cause__
        if isinstance(error, (ValueError, KeyError, TypeError, InvalidBackupStruct)):
            return "corrupt_metadata"
        return (
            "missing_metadata"
            if str(error) == "missing_metadata"
            else "metadata_unavailable"
        )

    def _prepare_backup_cloud_cleanup(
        self, backup: BackupMetadata, references: DedupReferences, delete_external: bool
    ) -> _CloudCleanupPlan:
        # Until every scope completes, the backup retains the source installation's
        # location for resume. Only the transfer manifest identifies destination keys.
        incomplete = bool(
            backup.copied_from
            and not backup.cloud_storage.data_copied
            and backup.copy_scopes
            and any(
                backup.copy_scopes.get(scope) != CopyScopeStatus.COMPLETED
                for scope in CopyScope
            )
        )
        source = backup.cloud_storage.source
        if source and not incomplete:
            self._check_cloud_deletion_overlap(backup.name, source)
        history = (
            self._context.backup_layout.read_cloud_copy_manifest(backup.name)
            if backup.copied_from
            and not backup.cloud_storage.data_copied
            and (backup.cloud_storage.enabled or source)
            else []
        )
        for location, _keys in history:
            self._check_cloud_deletion_overlap(backup.name, location)

        retained_cloud_disks = (
            {
                part.disk_name
                for table in backup.get_tables()
                for part in table.get_parts()
                if part.disk_name in backup.cloud_storage.disks
                and part.name in references.get(part.database, {}).get(part.table, ())
            }
            if references and backup.cloud_storage.enabled
            else set()
        )
        empty_link_disks: frozenset[str] = frozenset()
        if references and source and not incomplete:
            # Once the last outgoing link disappears, a disk without an archive
            # looks like a missing cleanup input. Forget only proven empty disks.
            link_only = set(backup.cloud_storage.disks) - set(
                backup.select_disks_with_stored_cloud_parts()
            )
            empty_link_disks = frozenset(
                disk
                for disk in link_only - retained_cloud_disks
                if not self._context.backup_layout.list_cloud_storage_metadata_archives(
                    backup.get_sanitized_name(),
                    disk,
                    backup.cloud_storage.compressed,
                    "all",
                )
            )
        if retained_cloud_disks:
            logging.info(
                "Keeping cloud objects and archives of referenced backup {}",
                backup.name,
            )
            return _CloudCleanupPlan(
                empty_link_disks=empty_link_disks,
                keep_frozen_data=not backup.copied_from
                and not backup.cloud_storage.data_copied,
            )
        external_target = None
        if not incomplete and delete_external:
            external_target = self._resolve_external_cloud_deletion_target(backup)
        elif source and not incomplete:
            logging.warning(
                "Cloud storage data of backup {} stored in bucket {} under {} is not deleted",
                backup.name,
                source["bucket"],
                source["path"],
            )
        destinations = []
        if delete_external:
            for location, keys in history:
                target = self._resolve_external_cloud_deletion_target(backup, location)
                assert target is not None
                destinations.append((target[0], keys))
        return self._prepare_cloud_cleanup_plan(
            backup,
            references,
            external_target,
            history=destinations,
            empty_link_disks=empty_link_disks,
        )

    def _check_cloud_deletion_overlap(
        self, name: str, location: Mapping[str, Any]
    ) -> None:
        """Recursive removal of either backup directory also removes external keys below it."""
        storage = self._config["storage"]
        if same_storage(
            storage,
            {
                "type": storage["type"],
                "credentials": {
                    "endpoint_url": location.get("endpoint_url"),
                    "bucket": location.get("bucket"),
                },
            },
        ):
            root = self._config["backup"]["path_root"]
            for directory in (name, sanitize_backup_name(name)):
                if prefixes_overlap(
                    location["path"].strip("/"),
                    os.path.join(root, directory).strip("/"),
                ):
                    raise ConfigurationError(
                        "External cloud storage prefix overlaps backup metadata"
                    )

    def _prepare_cloud_cleanup_plan(
        self,
        backup: BackupMetadata,
        retained_part_references: DedupReferences,
        external_target: tuple[dict, str] | None,
        *,
        history: Sequence[tuple[dict, CloudObjectDeletionIndex]] = (),
        empty_link_disks: frozenset[str] = frozenset(),
    ) -> _CloudCleanupPlan:
        """Prepare whole-owner cleanup once retained backups release their cloud parts."""
        layout = self._context.backup_layout
        if retained_part_references and not (
            history
            or external_target
            or backup.cloud_storage.enabled
            or (
                backup.cloud_storage.data_copied
                and backup.count_skipped_data_tables()[0]
            )
        ):
            return _CloudCleanupPlan()
        # Only the manifest describes an unfinished copy's destination. For a
        # completed copy, include both the archives and earlier transfer plans.
        historical = []
        same_bucket = []
        for storage, keys in history:
            if external_target and same_storage(storage, external_target[0]):
                same_bucket.append(keys)
            else:
                historical.append((storage, keys))
        if external_target:
            payload_storage, object_prefix = external_target
            keys = layout.collect_cloud_object_keys_for_deletion(backup, object_prefix)
            for old_keys in same_bucket:
                keys.update(old_keys)
            historical.append((payload_storage, keys))

        def delete_payload():
            for storage, planned_keys in historical:
                self._delete_external_cloud_payload(backup.name, storage, planned_keys)

        delete_backup_cloud_files = None
        if retained_part_references and (historical or not backup.cloud_storage.source):
            # Whole-backup deletion removes these files with the rest of the backup.
            # A partially retained backup needs this separate, prepared operation.
            delete_backup_cloud_files = partial(
                layout.delete_backup_cloud_files, backup.name
            )
        return _CloudCleanupPlan(
            empty_link_disks=empty_link_disks,
            delete_external_payload=delete_payload if historical else None,
            delete_backup_cloud_files=delete_backup_cloud_files,
        )

    def _resolve_external_cloud_deletion_target(
        self, backup: BackupMetadata, recorded_location: Mapping[str, Any] | None = None
    ) -> tuple[dict, str] | None:
        """Resolve credentials and validate a recorded external copy location.

        Return None when the backup has no external copy location. Otherwise,
        check the recorded location against the configured endpoint, bucket,
        and backup-prefix constraints.
        """
        if recorded_location is None:
            recorded_location = backup.cloud_storage.source
        if not recorded_location:
            return None

        object_prefix = recorded_location.get("path", "").strip("/")
        if (
            not backup.copied_from
            or not recorded_location.get("bucket")
            or not object_prefix
            or object_prefix.split("/")[-1] != backup.name
            or any(component in (".", "..") for component in object_prefix.split("/"))
        ):
            raise ConfigurationError(
                "External cloud storage deletion requires a copied backup with a backup-specific prefix"
            )

        payload_storage = deepcopy(self._config["storage"])
        payload_credentials = payload_storage["credentials"]
        configured_copy_target = self._config["copy"]["cloud_storage"]["target"]
        configured_endpoint = (
            configured_copy_target.get("endpoint_url")
            or payload_credentials["endpoint_url"]
        )
        if normalize_s3_endpoint(configured_endpoint) != normalize_s3_endpoint(
            recorded_location.get("endpoint_url")
        ):
            raise ConfigurationError(
                "Cloud storage endpoint in backup metadata does not match the configured endpoint; "
                'set "copy.cloud_storage.target.endpoint_url" explicitly'
            )
        if (
            configured_copy_target.get("bucket")
            and configured_copy_target["bucket"] != recorded_location["bucket"]
        ):
            raise ConfigurationError(
                "Cloud storage bucket in backup metadata does not match copy.cloud_storage.target.bucket"
            )
        self._check_cloud_deletion_overlap(backup.name, recorded_location)

        payload_credentials["endpoint_url"] = configured_endpoint
        payload_credentials["bucket"] = recorded_location["bucket"]
        if configured_copy_target.get("access_key_id"):
            payload_credentials["access_key_id"] = configured_copy_target[
                "access_key_id"
            ]
            payload_credentials["secret_access_key"] = configured_copy_target.get(
                "secret_access_key"
            )
        payload_storage["proxy_resolver"] = {"uri": None, "proxy_port": None}
        return payload_storage, object_prefix

    def _delete_external_cloud_payload(
        self,
        backup_name: str,
        payload_storage: dict,
        object_keys_to_delete: CloudObjectDeletionIndex,
    ) -> None:
        """Delete planned external payload objects without changing backup metadata."""
        credentials = payload_storage["credentials"]
        payload_engine = get_storage_engine(payload_storage)

        logging.info(
            "Cloud cleanup planned: backup={}, bucket={}, planned={} object keys",
            backup_name,
            credentials["bucket"],
            len(object_keys_to_delete),
        )
        confirmed = 0
        try:
            for count in delete_key_batches(
                payload_engine,
                object_keys_to_delete,
                payload_storage["bulk_delete_chunk_size"],
            ):
                confirmed += count
        except (Exception, TerminatingSignal):
            logging.error(
                "Cloud cleanup failed: backup={}, bucket={}, confirmed={}, planned={}; "
                "the failed batch may be partially deleted; retry cleanup",
                backup_name,
                credentials["bucket"],
                confirmed,
                len(object_keys_to_delete),
            )
            raise
        logging.info(
            "Cloud cleanup completed: backup={}, bucket={}, confirmed={} object-key deletions",
            backup_name,
            credentials["bucket"],
            confirmed,
        )

    def _system_unfreeze(self, backup: BackupMetadata) -> None:
        # Copied backups don't own frozen data, it belongs to the original backup.
        if backup.copied_from:
            logging.info("Skipping unfreeze for the copied backup {}", backup.name)
            return

        self._context.ch_ctl.system_unfreeze(backup.name)

    def _delete_data_parts(
        self,
        backup: BackupMetadata,
        table: TableMetadata,
        excluded_parts: set[str] = None,
    ) -> None:
        parts = table.get_parts(excluded_parts=excluded_parts)
        unlinked_parts_on_local_disks = [
            part
            for part in parts
            if not part.link and part.disk_name not in backup.cloud_storage.disks
        ]
        self._context.backup_layout.delete_data_parts(
            backup, unlinked_parts_on_local_disks
        )
        backup.remove_parts(table, parts)

    def _delete_detached_parts(
        self, backup: BackupMetadata, table: TableMetadata
    ) -> None:
        parts = table.get_detached_parts()
        self._context.backup_layout.delete_detached_parts(backup, parts)
        backup.remove_detached_parts(table, parts)

    # pylint: disable=too-many-positional-arguments
    def _restore(
        self,
        sources: BackupSources,
        db_names: Sequence[str],
        tables: builtins.list[TableMetadata],
        replica_name: str | None = None,
        cloud_storage_source_bucket: str | None = None,
        cloud_storage_source_path: str | None = None,
        cloud_storage_source_endpoint: str | None = None,
        skip_cloud_storage: bool = False,
        restore_detached_parts: bool = False,
        clean_zookeeper_mode: CleanZooKeeperMode = CleanZooKeeperMode.DISABLED,
        keep_going: bool = False,
        restore_tables_in_replicated_database: bool = False,
    ) -> None:
        # pylint: disable=too-many-locals

        if sources.access:
            # Restore access control entities
            self._access_backup_manager.restore(self._context, reload_users=True)

        if sources.udf:
            # Restore UDF
            self._udf_backup_manager.restore(self._context)

        if sources.named_collections:
            # Restore named collections
            self._nc_backup_manager.restore(self._context)

        if sources.workload_entities:
            # Restore workload entities
            self._we_backup_manager.restore(self._context)

        if sources.http_handlers:
            self._http_handlers_backup_manager.restore(self._context)

        if sources.schemas_included():
            databases: dict[str, Database] = {
                db_name: self._context.backup_meta.get_database(db_name)
                for db_name in db_names
            }

            metadata_cleaner: MetadataCleaner | None = None

            if (
                clean_zookeeper_mode != CleanZooKeeperMode.DISABLED
                and len(self._context.zk_config.get("hosts")) > 0
            ):
                metadata_cleaner = MetadataCleaner(
                    self._context.ch_ctl,
                    self._context.zk_ctl,
                    (
                        select_replica_drop(
                            replica_name, self._context.ch_ctl.get_macros()
                        )
                        if clean_zookeeper_mode == CleanZooKeeperMode.REPLICA_ONLY
                        else None
                    ),
                    self._config["multiprocessing"]["drop_replica_threads"],
                )

            # Validate adapted schemas before DatabaseBackup can drop an entire
            # database. Reuse the prepared tables and locks through table CREATE.
            with ExitStack() as disk_stack, ExitStack() as schema_locks:
                prepared_restore = None
                disks = None
                if skip_cloud_storage or sources.data:
                    prepared_restore = self._table_backup_manager.prepare_restore(
                        self._context,
                        databases,
                        tables,
                        keep_going,
                        skip_cloud_storage,
                        restore_tables_in_replicated_database,
                        schema_locks,
                    )
                # Resolve owners and staging before database restore can drop existing data.
                if sources.data and prepared_restore and prepared_restore.tables_meta:
                    disks = disk_stack.enter_context(
                        ClickHouseTemporaryDisks(
                            self._context.ch_ctl,
                            self._context.backup_layout,
                            self._context.config_root,
                            self._context.backup_meta,
                            cloud_storage_source_bucket,
                            cloud_storage_source_path,
                            cloud_storage_source_endpoint,
                            self._context.ch_config,
                            desired_tables=prepared_restore.tables_meta,
                            use_local_copy=self._context.config_root["restore"][
                                "use_inplace_cloud_restore"
                            ],
                            skip_cloud_storage=skip_cloud_storage,
                            keep_going=keep_going,
                        )
                    )
                # Restore databases.
                restored_databases = self._database_backup_manager.restore(
                    self._context,
                    databases,
                    keep_going,
                    metadata_cleaner,
                )

                # Wait for replicated databases to sync by tracking log_ptr in ZooKeeper.
                # Always use keep_going=True here: FAILED is an expected intermediate state
                # that will be retried after table restore below.
                sync_results = wait_sync_replicated_databases(
                    self._context, restored_databases, keep_going=True
                )

                # Restore tables and data stored on local disks.
                self._table_backup_manager.restore(
                    context=self._context,
                    databases=databases,
                    disks=disks,
                    tables=tables,
                    metadata_cleaner=metadata_cleaner,
                    skip_cloud_storage=skip_cloud_storage,
                    restore_detached_parts=restore_detached_parts,
                    keep_going=keep_going,
                    restore_tables_in_replicated_database=restore_tables_in_replicated_database,
                    prepared_restore=prepared_restore,
                )

            # Retry sync for databases that failed during initial sync.
            # After table restore, broken tables should be fixed and DDL worker
            # should be able to proceed.
            failed_databases = [
                db
                for db in restored_databases
                if sync_results.get(db.name) == SyncStatus.FAILED
            ]
            if failed_databases:
                logging.info(
                    f"Retrying sync for {len(failed_databases)} database(s) that failed: {', '.join(db.name for db in failed_databases)}"
                )
                wait_sync_replicated_databases(
                    self._context, failed_databases, keep_going
                )

            if sources.data and self._context.restore_context.has_failed_parts():
                msg = "Some parts failed to restore"
                logging.warning(msg)

                if self._context.config["restore_fail_on_attach_error"]:
                    raise ClickhouseBackupError(msg)

    def _get_parts_backup(
        self, sources: BackupSources, parts_backup_name: str | None
    ) -> BackupMetadata | None:
        """
        Get the backup to take data parts from in a restore without data.
        """
        if not parts_backup_name:
            return None
        if sources.data:
            raise ClickhouseBackupError(
                "Parts from another backup work only for restore without data"
            )
        parts_backup = self._get_backup(parts_backup_name)
        if parts_backup.schema_only:
            raise ClickhouseBackupError(
                f"Backup {parts_backup_name} is schema-only and has no data parts"
            )
        if not self._context.zk_config.get("hosts"):
            logging.warning(
                f"ZooKeeper is not configured, parts of backup {parts_backup_name}"
                " are left to replicas"
            )
            return None
        if not os.access(CLICKHOUSE_BINARY, os.X_OK):
            logging.warning(
                f"{CLICKHOUSE_BINARY} is not found, parts of backup {parts_backup_name}"
                " are left to replicas"
            )
            return None
        if _zookeeper_roots_differ(
            self._context.ch_config.config, self._context.zk_config
        ):
            logging.warning(
                "ZooKeeper root_path of ch-backup differs from the root of ClickHouse,"
                f" parts of backup {parts_backup_name} may be left to replicas"
            )
        return parts_backup

    def _get_backup(
        self, backup_name: str, use_light_meta: bool = False
    ) -> BackupMetadata:
        backup = self._context.backup_layout.get_backup(backup_name, use_light_meta)
        if not backup:
            raise BackupNotFound(backup_name)

        return backup

    def _check_min_interval(self, last_backup: BackupMetadata, force: bool) -> bool:
        if force:
            return True

        min_interval = self._context.config.get("min_interval")
        if not min_interval:
            return True

        if not last_backup.end_time:
            return True

        if utcnow() - last_backup.end_time >= timedelta(**min_interval):
            return True

        return False

    def _check_schema_only_backup_skip_lock(self, sources: BackupSources) -> bool:
        if not sources.schema_only:
            return False

        skip_lock = self._context.config.get("skip_lock_for_schema_only", None)
        if not skip_lock:
            return False

        return skip_lock.get("backup", False)


def _is_complete(backup: BackupMetadata) -> bool:
    """
    Return True if a backup is created and holds all data of its tables.
    """
    return (
        backup.state == BackupState.CREATED
        and not backup.schema_only
        and not any(backup.count_skipped_data_tables())
    )


def _zookeeper_roots_differ(ch_config: dict, zk_config: dict) -> bool:
    """
    Check if ch-backup and ClickHouse use different roots in ZooKeeper.
    """
    ch_root = (ch_config.get("zookeeper") or {}).get("root") or ""
    return ch_root.rstrip("/") != (zk_config.get("root_path") or "").rstrip("/")


def _is_partial_copy(backup: BackupMetadata) -> bool:
    """Check for copied files with cloud storage still pending or failed."""
    return (
        backup.state in (BackupState.CREATING, BackupState.FAILED)
        and backup.copy_scopes.get(CopyScope.FILES) == CopyScopeStatus.COMPLETED
        and backup.copy_scopes.get(CopyScope.CLOUD_STORAGE)
        in (CopyScopeStatus.PENDING, CopyScopeStatus.FAILED)
    )


def _is_being_copied(backup: BackupMetadata, since: datetime | None) -> bool:
    """Check whether an incomplete copy needs temporary retention."""
    if (
        since is None
        or backup.state not in (BackupState.CREATING, BackupState.FAILED)
        or _is_partial_copy(backup)
    ):
        return False
    updated_at = backup.copy_updated_at
    return updated_at is not None and updated_at >= since
