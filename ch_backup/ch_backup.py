"""
Clickhouse backup logic
"""

# pylint: disable=too-many-lines

import builtins
import os
from collections import defaultdict
from contextlib import ExitStack
from copy import copy, deepcopy
from datetime import datetime, timedelta
from enum import Enum
from functools import partial
from typing import Callable, Sequence

import requests

from ch_backup import logging
from ch_backup.backup.copier import BackupCopier
from ch_backup.backup.copy_deduplication import copy_deduplication_cutoff
from ch_backup.backup.copy_report import CopyReport
from ch_backup.backup.deduplication import (
    DedupReferences,
    collect_dedup_info,
    collect_dedup_references_for_batch_backup_deletion,
)
from ch_backup.backup.layout import BackupLayout, prefetch_metadata
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
from ch_backup.storage.engine import get_storage_engine
from ch_backup.storage.engine.s3 import S3RetryingError, validate_s3_retry_config
from ch_backup.storage.object_copy import parse_server_side_copy_mode
from ch_backup.util import cached_property, chunked, now, utcnow
from ch_backup.version import get_version


class CleanZooKeeperMode(str, Enum):
    """
    What to clean in ZooKeeper
    """

    DISABLED = "disabled"
    REPLICA_ONLY = "replica-only"
    ALL_REPLICAS = "all-replicas"


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
        parse_server_side_copy_mode(config["copy"]["server_side_copy_mode"])
        copy_deduplication_cutoff(config["copy"], utcnow())
        start_time = as_datetime(config["backup"].get("deduplication_start_time"))
        if start_time is not None and start_time > utcnow():
            raise ValueError(
                "backup.deduplication_start_time must not be in the future"
            )
        if config["storage"]["type"] == "s3":
            validate_s3_retry_config(config["storage"])

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

    def inspect_cloud_owners(self, backup_name: str) -> dict:
        """Explain cloud owners and their direct dependents, including hidden generations.

        This is a diagnostic snapshot, not a deletion authorization. Unreadable metadata
        makes the dependency list incomplete; it must not imply that an owner is unused.
        """
        layout = self._context.backup_layout
        errors: dict[str, str] = {}
        backups = {
            backup.name: backup
            for backup in layout.get_backups(
                include_generations=True, unreadable=errors
            )
        }
        selected = backups.get(backup_name)
        owner_names = {
            backup.name
            for backup in backups.values()
            if backup.generation_of == backup_name
        }
        if selected:
            if (
                selected.generation_of
                or selected.select_disks_with_stored_cloud_parts()
            ):
                owner_names.add(selected.name)
            # A fully deduplicated copy can outlive its empty generation.
            # Part links below identify required owners; an unfinished copy
            # also needs its generation even before it has published parts.
            if selected.cloud_generation and (
                selected.cloud_generation in backups
                or selected.state in (BackupState.CREATING, BackupState.FAILED)
            ):
                owner_names.add(selected.cloud_generation)
            owner_names.update(
                part.link
                for part in selected.get_parts()
                if part.link and part.disk_name in selected.cloud_storage.disks
            )
        elif not owner_names and backup_name not in errors:
            raise BackupNotFound(backup_name)

        references: dict[str, set[str]] = defaultdict(set)
        pending: dict[str, set[str]] = defaultdict(set)
        for backup in backups.values():
            for part in backup.get_parts():
                if part.link and part.disk_name in backup.cloud_storage.disks:
                    references[part.link].add(backup.name)
            if backup.cloud_generation and backup.state in (
                BackupState.CREATING,
                BackupState.FAILED,
            ):
                pending[backup.cloud_generation].add(backup.name)

        if references[backup_name]:
            # Old children can still reference the legacy snapshot after public replacement.
            owner_names.add(backup_name)

        owners = []
        for name in sorted(owner_names):
            record: dict = {
                "name": name,
                "referenced_by": sorted(references[name]),
                "pending_copies": sorted(pending[name]),
            }
            try:
                owner = layout.get_backup(name, for_reference=True)
                if owner is None:
                    raise BackupNotFound(name)
            except Exception as error:
                errors[name] = str(error)
                record["metadata_error"] = str(error)
            else:
                public = backups.get(owner.generation_of or "")
                record.update(
                    generation_of=owner.generation_of,
                    current=bool(
                        public
                        and public.state
                        in (BackupState.CREATED, BackupState.PARTIALLY_DELETED)
                        and public.cloud_generation == owner.name
                    ),
                    state=owner.state.value,
                    cloud_storage=owner.cloud_storage.dump(),
                )
            owners.append(record)
        return {
            "backup": backup_name,
            "current_generation": (
                selected.cloud_generation
                if selected
                and selected.state
                in (BackupState.CREATED, BackupState.PARTIALLY_DELETED)
                else None
            ),
            "pending_generation": (
                selected.cloud_generation
                if selected
                and selected.state in (BackupState.CREATING, BackupState.FAILED)
                else None
            ),
            "owners": owners,
            "references_complete": not errors,
            "metadata_errors": errors,
        }

    def list(
        self, state: BackupState = None, *, unreadable: dict[str, str] | None = None
    ) -> Sequence[BackupMetadata]:
        """
        Get list of existing backups sorted by start timestamp.
        If unreadable is provided, collect per-backup read errors there.
        """
        backups = []
        for backup in self._context.backup_layout.get_backups(
            use_light_meta=True, unreadable=unreadable
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

    def delete(
        self,
        backup_name: str,
        purge_partial: bool,
        delete_cloud_storage_data: bool = False,
    ) -> tuple[str | None, str | None]:
        """
        Delete the named backup and optionally purge other incomplete backups.
        """
        deleting_backups = []
        retained_backups = []
        copying_since = self._copying_since()
        with self._context.locker(operation="DELETE"):
            # Other backups must be readable: their references protect shared data.
            backups = self._context.backup_layout.get_backups(
                use_light_meta=True,
                excluded_backup_names=frozenset({backup_name}),
                include_generations=True,
            )
            try:
                requested_backup = self._context.backup_layout.get_backup(
                    backup_name, use_light_meta=True
                )
            except (StorageError, S3RetryingError) as error:
                if delete_cloud_storage_data:
                    raise InvalidBackupStruct(
                        "Cannot delete external cloud storage data without readable backup metadata"
                    ) from error
                logging.exception("Cannot read metadata of backup {}", backup_name)
                requested_backup = None
            else:
                if requested_backup is None or requested_backup.name != backup_name:
                    raise BackupNotFound(backup_name)
                backups.append(requested_backup)
                backups.sort(key=lambda backup: backup.start_time, reverse=True)

            for i, backup in enumerate(backups):
                if backup.generation_of:
                    if backup.generation_of != backup_name and _is_being_copied(
                        backup, copying_since
                    ):
                        retained_backups.append(backup)
                    else:
                        deleting_backups.append(backup)
                    continue
                if backup.name == backup_name:
                    deleting_backups.append(backup)
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

            _retain_pending_cloud_generations(retained_backups, deleting_backups)
            dedup_references = collect_dedup_references_for_batch_backup_deletion(
                layout=self._context.backup_layout,
                retained_backups_light_meta=retained_backups,
                workers=self._config["multiprocessing"]["full_metadata_read_threads"],
                deleting_backup_names={
                    backup_name,
                    *(b.name for b in deleting_backups),
                },
            )

            if requested_backup is None and dedup_references[backup_name]:
                raise ClickhouseBackupError(
                    f"Backup {backup_name} has unreadable metadata and is referenced "
                    "by other backups"
                )

            prepared_deletions = self._prepare_deletion_batch(
                deleting_backups, dedup_references, delete_cloud_storage_data
            )
            result: tuple[str | None, str | None] = (None, None)
            for backup, metadata_is_light, cloud_cleanup_action in prepared_deletions:
                deleted_name, msg = self._execute_backup_deletion(
                    backup,
                    dedup_references[backup.name],
                    metadata_is_light,
                    cloud_cleanup_action,
                )
                if backup_name == backup.name:
                    result = (deleted_name, msg)
            if requested_backup is None:
                result = self._delete_unreadable(backup_name)

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

    def purge(
        self, delete_cloud_storage_data: bool = False
    ) -> tuple[Sequence[str], str | None]:
        """
        Purge backups.

        If a created backup or partial copy is retained but none is complete,
        keep the newest complete backup: it may hold the last copy of data
        the retained backups lack.
        """
        retain_time = self._context.config["retain_time"]
        retain_count = self._context.config["retain_count"]

        deleted_backup_names: list[str] = []

        if not retain_time and retain_count is None:
            logging.info("Retain policies are not specified")
            return deleted_backup_names, "Retain policies are not specified."

        retain_time_limit = None
        if retain_time:
            retain_time_limit = now() - timedelta(**retain_time)

        retained_backups: list[BackupMetadata] = []
        deleting_backups: list[BackupMetadata] = []
        copying_since = self._copying_since()

        with self._context.locker(operation="PURGE"):
            # Use light metadata in backups iteration to avoid high memory usage.
            for backup in self._context.backup_layout.get_backups(
                use_light_meta=True, include_generations=True
            ):
                if backup.generation_of:
                    if _is_being_copied(backup, copying_since):
                        logging.info(
                            "Retaining pending cloud generation {} of backup {}: incomplete-copy grace period",
                            backup.name,
                            backup.generation_of,
                        )
                        retained_backups.append(backup)
                    else:
                        deleting_backups.append(backup)
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

            _retain_pending_cloud_generations(retained_backups, deleting_backups)
            dedup_references = collect_dedup_references_for_batch_backup_deletion(
                layout=self._context.backup_layout,
                retained_backups_light_meta=retained_backups,
                workers=self._config["multiprocessing"]["full_metadata_read_threads"],
                deleting_backup_names={b.name for b in deleting_backups},
            )

            prepared_deletions = self._prepare_deletion_batch(
                deleting_backups, dedup_references, delete_cloud_storage_data
            )
            for backup, metadata_is_light, cloud_cleanup_action in prepared_deletions:
                backup_name, _ = self._execute_backup_deletion(
                    backup,
                    dedup_references[backup.name],
                    metadata_is_light,
                    cloud_cleanup_action,
                )
                if backup_name:
                    deleted_backup_names.append(backup_name)

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
            full_metadata = self._context.backup_layout.get_backup(
                light_metadata.name, for_reference=True
            )
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
    ) -> builtins.list[tuple[BackupMetadata, bool, Callable[[], None] | None]]:
        """Prepare each backup's deletion before changing any backup in the batch.

        Retain loaded metadata and cloud deletion callbacks so execution uses
        the data validated here without downloading it again.
        A bad later backup must not cause earlier backups to be deleted.
        """
        prepared_deletions = []
        ordered_backups = sorted(
            backups_to_delete, key=lambda backup: bool(backup.generation_of)
        )
        reads = (
            partial(
                self._load_metadata_for_deletion, backup, dedup_references[backup.name]
            )
            for backup in ordered_backups
        )
        workers = self._config["multiprocessing"]["full_metadata_read_threads"]
        with prefetch_metadata(reads, workers) as futures:
            for light_metadata, future in zip(ordered_backups, futures):
                backup, metadata_is_light = future.result()
                cloud_cleanup_action = self._choose_cloud_cleanup_action(
                    backup,
                    dedup_references[light_metadata.name],
                    delete_cloud_storage_data,
                )
                prepared_deletions.append(
                    (backup, metadata_is_light, cloud_cleanup_action)
                )
                del future
        logging.info(
            "Deletion batch validated: backups={}, cloud_cleanup_actions={}; no deletion started during validation",
            len(prepared_deletions),
            sum(action is not None for _, _, action in prepared_deletions),
        )
        return prepared_deletions

    def _execute_backup_deletion(
        self,
        backup: BackupMetadata,
        dedup_references: DedupReferences,
        metadata_is_light: bool,
        cloud_cleanup_action: Callable[[], None] | None,
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
            if not dedup_references:
                logging.info("Removing backup data entirely")
                if cloud_cleanup_action:
                    cloud_cleanup_action()
                    # Persist cleanup before archive deletion can remove the retry inputs.
                    self._context.backup_layout.upload_backup_metadata(
                        backup, light_only=metadata_is_light
                    )
                self._context.backup_layout.delete_backup(
                    backup.name,
                    lambda: self._system_unfreeze(backup),
                )
                return backup.name, None

            if (
                self._has_referenced_cloud_parts(backup, dedup_references)
                and cloud_cleanup_action
            ):
                cloud_cleanup_action()

            logging.info("Removing non-shared backup data parts")
            for db_name in backup.get_databases():
                db_dedup_references = dedup_references[db_name]
                for table in backup.get_tables(db_name):
                    self._delete_data_parts(
                        backup, table, db_dedup_references[table.name]
                    )
                    self._delete_detached_parts(backup, table)

            if backup.cloud_storage.enabled or (
                backup.cloud_storage.data_copied
                and backup.count_skipped_data_tables()[0]
            ):
                if self._has_referenced_cloud_parts(backup, dedup_references):
                    logging.info(
                        "Keeping cloud storage data of the backup, it is in use by subsequent backups"
                    )
                else:
                    if cloud_cleanup_action:
                        cloud_cleanup_action()
                        self._context.backup_layout.upload_backup_metadata(
                            backup, light_only=metadata_is_light
                        )
                    if not backup.cloud_storage.source:
                        self._context.backup_layout.delete_cloud_storage_data(
                            backup.name
                        )
            self._context.backup_layout.wait()
            self._system_unfreeze(backup)
            backup.state = BackupState.PARTIALLY_DELETED
            self._context.backup_layout.update_legacy_cloud_owner(backup)
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

    def _delete_unreadable(self, backup_name: str) -> tuple[str, str]:
        """
        Delete a backup with unreadable light metadata.

        It goes last, after the backups deleted along with it, so a failed deletion
        cannot leave a backup linking to it. Frozen data is kept: without metadata
        it is unknown whether the backup is a copy sharing its name with a live
        original on this host.
        """
        logging.warning("Deleting backup {} with unreadable metadata", backup_name)
        self._context.backup_layout.delete_backup(backup_name, lambda: None)
        return (
            backup_name,
            f"Frozen data of the backup was not unfrozen. If it is not a copy, run "
            f"SYSTEM UNFREEZE WITH NAME '{backup_name}' on the hosts that created it.",
        )

    def _choose_cloud_cleanup_action(
        self,
        backup: BackupMetadata,
        retained_part_references: DedupReferences,
        delete_cloud_storage_data: bool,
    ) -> Callable[[], None] | None:
        """Choose a deferred action for whole or partial cloud payload deletion.

        Read and validate the required disk metadata before returning the action.
        Preserve referenced parts. Return None when no separate cloud action is
        needed or deletion of the external payload is disabled.
        """
        if backup.cloud_generation and not backup.generation_of:
            return None
        external_target = self._resolve_external_cloud_deletion_target(
            backup, delete_cloud_storage_data
        )
        if (
            retained_part_references
            and backup.cloud_storage.enabled
            and self._has_referenced_cloud_parts(backup, retained_part_references)
        ):
            if backup.cloud_storage.data_copied:
                payload_storage = self._config["storage"]
            elif external_target:
                payload_storage, _object_prefix = external_target
            else:
                return None
            retained_part_identities = {
                (database_name, table_name, part_name)
                for database_name, tables in retained_part_references.items()
                for table_name, part_names in tables.items()
                for part_name in part_names
            }
            return self._context.backup_layout.build_unreferenced_cloud_cleanup_action(
                backup, retained_part_identities, payload_storage
            )
        if external_target:
            payload_storage, object_prefix = external_target
            object_keys_to_delete = (
                self._context.backup_layout.collect_cloud_object_keys_for_deletion(
                    backup, object_prefix
                )
            )
            return partial(
                self._delete_external_cloud_objects,
                backup,
                payload_storage,
                object_keys_to_delete,
            )
        return None

    @staticmethod
    def _has_referenced_cloud_parts(
        backup: BackupMetadata, dedup_references: DedupReferences
    ) -> bool:
        """
        Return True if parts of cloud storage disks of a backup are reused.

        Referenced cloud parts retain their disk metadata and copied objects.
        Unreferenced parts can be removed through their archived disk metadata.
        """
        return any(
            part.name in dedup_references[table.database][table.name]
            and part.disk_name in backup.cloud_storage.disks
            for table in backup.get_tables()
            for part in table.get_parts()
        )

    def _resolve_external_cloud_deletion_target(
        self, backup: BackupMetadata, delete_cloud_storage_data: bool
    ) -> tuple[dict, str] | None:
        """Resolve credentials and a validated prefix for opt-in external deletion.

        Return None when the backup has no external copy location or external
        deletion is disabled. Otherwise, check the recorded location against
        the configured endpoint, bucket, and backup-prefix constraints.
        Trusted metadata supplies destinations so purge need not configure each again.
        """
        recorded_location = backup.cloud_storage.source
        if not recorded_location:
            return None
        if not delete_cloud_storage_data:
            logging.warning(
                "Cloud storage data of backup {} stored in bucket {} under {} is not deleted",
                backup.name,
                recorded_location["bucket"],
                recorded_location["path"],
            )
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
        normalized_configured_endpoint = (
            configured_endpoint or "https://s3.amazonaws.com"
        ).rstrip("/")
        normalized_recorded_endpoint = (
            recorded_location.get("endpoint_url") or "https://s3.amazonaws.com"
        ).rstrip("/")
        # Metadata is not authenticated. Credentials must only reach a configured endpoint.
        if normalized_configured_endpoint != normalized_recorded_endpoint:
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
        normalized_backup_endpoint = (
            payload_credentials["endpoint_url"] or "https://s3.amazonaws.com"
        ).rstrip("/")
        if (
            normalized_backup_endpoint == normalized_configured_endpoint
            and payload_credentials["bucket"] == recorded_location["bucket"]
        ):
            backup_directory_prefixes = [
                os.path.join(
                    self._config["backup"]["path_root"], backup_directory_name
                ).strip("/")
                for backup_directory_name in (
                    backup.name,
                    sanitize_backup_name(backup.name),
                )
            ]
            # Exact-key cleanup permits objects and archives to share the backup directory.
            if object_prefix != backup_directory_prefixes[0] and any(
                backup_prefix == object_prefix
                or backup_prefix.startswith(object_prefix + "/")
                or object_prefix.startswith(backup_prefix + "/")
                for backup_prefix in backup_directory_prefixes
            ):
                raise ConfigurationError(
                    "External cloud storage prefix overlaps backup metadata"
                )

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

    def _delete_external_cloud_objects(
        self,
        backup: BackupMetadata,
        payload_storage: dict,
        object_keys_to_delete: Sequence[str],
    ) -> None:
        """Delete copied objects, leaving archives and backup files to layout cleanup."""
        credentials = payload_storage["credentials"]
        payload_engine = get_storage_engine(payload_storage)

        logging.info(
            "Cloud cleanup planned: backup={}, bucket={}, planned={} object keys",
            backup.name,
            credentials["bucket"],
            len(object_keys_to_delete),
        )
        confirmed = 0
        try:
            for object_key_batch in chunked(
                object_keys_to_delete, payload_storage["bulk_delete_chunk_size"]
            ):
                payload_engine.delete_files(object_key_batch)
                confirmed += len(object_key_batch)
        except (Exception, TerminatingSignal):
            logging.error(
                "Cloud cleanup failed: backup={}, bucket={}, confirmed={}, planned={}; "
                "the failed batch may be partially deleted; retry cleanup",
                backup.name,
                credentials["bucket"],
                confirmed,
                len(object_keys_to_delete),
            )
            raise
        logging.info(
            "Cloud cleanup completed: backup={}, bucket={}, confirmed={} object-key deletions",
            backup.name,
            credentials["bucket"],
            confirmed,
        )
        # Archive cleanup can fail later. A retry must not need manifests for already deleted blobs.
        backup.cloud_storage.source = None

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
            with ExitStack() as schema_locks:
                prepared_restore = None
                if skip_cloud_storage:
                    prepared_restore = self._table_backup_manager.prepare_restore(
                        self._context,
                        databases,
                        tables,
                        keep_going,
                        skip_cloud_storage,
                        restore_tables_in_replicated_database,
                        schema_locks,
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
                    schema_only=sources.schema_only,
                    tables=tables,
                    metadata_cleaner=metadata_cleaner,
                    cloud_storage_source_bucket=cloud_storage_source_bucket,
                    cloud_storage_source_path=cloud_storage_source_path,
                    cloud_storage_source_endpoint=cloud_storage_source_endpoint,
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
        and not backup.generation_of
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


def _retain_pending_cloud_generations(
    retained: builtins.list[BackupMetadata], deleting: builtins.list[BackupMetadata]
) -> None:
    """A retained partial copy still needs its transferred objects, beyond the lease window.

    Its part links name the source until publication, so ordinary link collection
    cannot protect the pending target generation. Keep that generation whole.
    """
    pending_names = {
        backup.cloud_generation
        for backup in retained
        if backup.state in (BackupState.CREATING, BackupState.FAILED)
    }
    for generation in builtins.list(deleting):
        if generation.generation_of and generation.name in pending_names:
            logging.info(
                "Retaining pending cloud generation {} of backup {}: retained incomplete copy",
                generation.name,
                generation.generation_of,
            )
            deleting.remove(generation)
            retained.append(generation)
