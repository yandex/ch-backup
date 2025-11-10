"""
Clickhouse backup logic
"""

from collections import defaultdict
from copy import copy
from datetime import timedelta
from enum import Enum
from typing import Dict, List, Optional, Sequence, Set, Tuple

from ch_backup import logging
from ch_backup.backup.deduplication import (
    DedupReferences,
    collect_dedup_info,
    collect_dedup_references_for_batch_backup_deletion,
)
from ch_backup.backup.metadata import BackupMetadata, BackupState, TableMetadata
from ch_backup.backup.sources import BackupSources
from ch_backup.backup_context import BackupContext
from ch_backup.clickhouse.metadata_cleaner import MetadataCleaner, select_replica_drop
from ch_backup.clickhouse.models import Database
from ch_backup.config import Config
from ch_backup.exceptions import (
    BackupNotFound,
    ClickhouseBackupError,
    TerminatingSignal,
)
from ch_backup.logic.access import AccessBackup
from ch_backup.logic.database import DatabaseBackup
from ch_backup.logic.named_collections import NamedCollectionsBackup
from ch_backup.logic.table import TableBackup
from ch_backup.logic.udf import UDFBackup
from ch_backup.storage.async_pipeline.stages import EncryptStage
from ch_backup.util import cached_property, now, utcnow
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
        self._config = config
        self._access_backup_manager = AccessBackup()
        self._database_backup_manager = DatabaseBackup()
        self._table_backup_manager = TableBackup()
        self._udf_backup_manager = UDFBackup()
        self._nc_backup_manager = NamedCollectionsBackup()

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
        logging.info("Reloading config.")
        del self._context
        self._config = config
        logging.info("Config reloaded.")

    def get(self, backup_name: str) -> BackupMetadata:
        """
        Get backup information.
        """
        return self._get_backup(backup_name)

    def list(self, state: BackupState = None) -> Sequence[BackupMetadata]:
        """
        Get list of existing backups sorted by start timestamp.
        """
        backups = []
        for backup in self._context.backup_layout.get_backups(use_light_meta=True):
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
    ) -> Tuple[str, Optional[str]]:
        """
        Perform backup.

        If force is True, backup.min_interval config option is ignored.
        """
        # pylint: disable=too-many-branches
        # pylint: disable=too-many-locals
        backups_with_light_meta = self._context.backup_layout.get_backups(
            use_light_meta=True
        )

        for backup in backups_with_light_meta:
            if name == backup.name:
                raise ClickhouseBackupError(f"Backup with name {name} already exists")

        logging.info(f"Backup sources: {sources}")
        assert not (db_names and tables)

        backup_labels: dict = copy(self._context.config.get("labels"))  # type: ignore
        if labels:
            backup_labels.update(labels)

        db_tables: Dict[str, list] = defaultdict(list)
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
                'Starting backup "{}" for databases: {}',
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
                if sources.schemas_included():
                    self._database_backup_manager.backup(self._context, databases)
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

        return self._context.backup_meta.name, None

    # pylint: disable=too-many-arguments,duplicate-code,too-many-positional-arguments
    def restore(
        self,
        sources: BackupSources,
        backup_name: str,
        databases: Sequence[str],
        exclude_databases: Sequence[str],
        tables: List[TableMetadata],
        exclude_tables: List[TableMetadata],
        override_replica_name: str = None,
        force_non_replicated: bool = False,
        replica_name: Optional[str] = None,
        cloud_storage_source_bucket: str = None,
        cloud_storage_source_path: str = None,
        cloud_storage_source_endpoint: str = None,
        skip_cloud_storage: bool = False,
        clean_zookeeper_mode: CleanZooKeeperMode = CleanZooKeeperMode.DISABLED,
        keep_going: bool = False,
        restore_tables_in_replicated_database: bool = False,
    ) -> None:
        """
        Restore specified backup
        """
        logging.info(f"Restore sources: {sources}")
        self._context.backup_meta = self._get_backup(backup_name)

        if (
            cloud_storage_source_bucket is None
            and sources.data
            and not skip_cloud_storage
        ):
            if self._context.backup_meta.cloud_storage.enabled:
                raise ClickhouseBackupError(
                    "Cloud storage source bucket must be set if backup has data on S3 disks"
                )

        self._context.config["override_replica_name"] = (
            override_replica_name or self._context.config.get("override_replica_name")
        )
        self._context.config["force_non_replicated"] = (
            force_non_replicated or self._context.config["force_non_replicated"]
        )

        if tables:
            logging.debug("Picking databases for specified tables.")
            databases = list(set(t.database for t in tables))

        if not databases:
            logging.debug("Picking all databases from backup.")
            databases = self._context.backup_meta.get_databases()

        if exclude_databases:
            logging.debug(
                f'Excluding specified databases from restoring: {", ".join(exclude_databases)}.'
            )
            databases = list(
                filter(lambda db_name: db_name not in exclude_databases, databases)
            )

        # check if all required databases exist in backup meta
        logging.info("Checking for presence of required databases.")

        missed_databases = [
            db_name
            for db_name in databases
            if db_name not in self._context.backup_meta.get_databases()
        ]
        if missed_databases:
            logging.critical(
                "Required databases {} were not found in backup metadata: {}",
                ", ".join(missed_databases),
                self._context.backup_meta.path,
            )
            raise ClickhouseBackupError(
                "Required databases were not found in backup metadata"
            )

        logging.info("All required databases are present in backup.")

        with self._context.locker(
            distributed=not sources.schema_only, operation="RESTORE"
        ):
            self._restore(
                sources=sources,
                db_names=databases,
                tables=tables,
                exclude_tables=exclude_tables,
                replica_name=replica_name,
                cloud_storage_source_bucket=cloud_storage_source_bucket,
                cloud_storage_source_path=cloud_storage_source_path,
                cloud_storage_source_endpoint=cloud_storage_source_endpoint,
                skip_cloud_storage=skip_cloud_storage,
                clean_zookeeper_mode=clean_zookeeper_mode,
                keep_going=keep_going,
                restore_tables_in_replicated_database=restore_tables_in_replicated_database,
            )

    def delete(
        self, backup_name: str, purge_partial: bool
    ) -> Tuple[Optional[str], Optional[str]]:
        """
        Delete the specified backup.
        """
        found = False
        deleting_backups = []
        retained_backups = []
        with self._context.locker(operation="DELETE"):
            # Use light metadata in backups iteration to avoid high memory usage.
            for i, backup in enumerate(
                self._context.backup_layout.get_backups(use_light_meta=True)
            ):
                if backup.name == backup_name:
                    deleting_backups.append(backup)
                    found = True
                    continue

                if purge_partial and backup.state != BackupState.CREATED and i != 0:
                    deleting_backups.append(backup)
                else:
                    retained_backups.append(backup)

            if not found:
                raise BackupNotFound(backup_name)

            dedup_references = collect_dedup_references_for_batch_backup_deletion(
                layout=self._context.backup_layout,
                retained_backups_light_meta=retained_backups,
                deleting_backups_light_meta=deleting_backups,
            )

            result: Tuple[Optional[str], Optional[str]] = (None, None)
            for backup in deleting_backups:
                deleted_name, msg = self._delete(backup, dedup_references[backup.name])
                if backup_name == backup.name:
                    result = (deleted_name, msg)

        return result

    def purge(self) -> Tuple[Sequence[str], Optional[str]]:
        """
        Purge backups.
        """
        retain_time = self._context.config["retain_time"]
        retain_count = self._context.config["retain_count"]

        deleted_backup_names: List[str] = []

        if not retain_time and retain_count is None:
            logging.info("Retain policies are not specified")
            return deleted_backup_names, "Retain policies are not specified."

        retain_time_limit = None
        if retain_time:
            retain_time_limit = now() - timedelta(**retain_time)

        retained_backups: List[BackupMetadata] = []
        deleting_backups: List[BackupMetadata] = []
        backup_names = self._context.backup_layout.get_backup_names()

        with self._context.locker(operation="PURGE"):
            # Use light metadata in backups iteration to avoid high memory usage.
            for backup in self._context.backup_layout.get_backups(use_light_meta=True):
                if backup.name not in backup_names:
                    logging.info("Deleting backup without metadata: {}", backup.name)
                    self._context.backup_layout.delete_backup(backup.name)
                    continue

                if retain_count > 0:
                    logging.info(
                        "Preserving backup per retain count policy: {}, state {}",
                        backup.name,
                        backup.state,
                    )
                    retained_backups.append(backup)
                    if backup.state == BackupState.CREATED:
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

                deleting_backups.append(backup)

            dedup_references = collect_dedup_references_for_batch_backup_deletion(
                layout=self._context.backup_layout,
                retained_backups_light_meta=retained_backups,
                deleting_backups_light_meta=deleting_backups,
            )

            for backup in deleting_backups:
                backup_name, _ = self._delete(backup, dedup_references[backup.name])
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
    ) -> bool:
        """Download cloud storage metadata to shadow directory. Returns false if metadata is already present."""
        backup_meta = self._get_backup(backup_name)
        source_disk = self._context.ch_ctl.get_disk(disk_name)

        if self._context.backup_layout.cloud_storage_metadata_exists(
            backup_meta, source_disk
        ):
            return False

        self._context.backup_layout.download_cloud_storage_metadata(
            backup_meta, source_disk, disk_name
        )
        self._context.backup_layout.wait()

        return True

    def _delete(
        self, backup_light_meta: BackupMetadata, dedup_references: DedupReferences
    ) -> Tuple[Optional[str], Optional[str]]:
        logging.info(
            "Deleting backup {}, state: {}",
            backup_light_meta.name,
            backup_light_meta.state,
        )
        backup = self._context.backup_layout.reload_backup(
            backup_light_meta, use_light_meta=False
        )
        backup.state = BackupState.DELETING
        self._context.backup_layout.upload_backup_metadata(backup)

        try:
            # delete whole backup prefix if its data parts are not shared with other backups
            if not dedup_references:
                logging.info("Removing backup data entirely")
                self._context.backup_layout.delete_backup(backup.name)
                self._context.ch_ctl.system_unfreeze(backup.name)
                return backup.name, None

            logging.info("Removing non-shared backup data parts")
            for db_name in backup.get_databases():
                db_dedup_references = dedup_references[db_name]
                for table in backup.get_tables(db_name):
                    self._delete_data_parts(
                        backup, table, db_dedup_references[table.name]
                    )

            self._context.ch_ctl.system_unfreeze(backup.name)
            return (
                None,
                "Backup was partially deleted as its data is in use by subsequent backups per "
                "deduplication settings.",
            )

        except (Exception, TerminatingSignal) as e:
            logging.critical("Delete failed", exc_info=True)
            backup.state = BackupState.FAILED
            backup.exception = f"{type(e).__name__}: {e}"
            raise

        finally:
            self._context.backup_layout.wait()
            if dedup_references:
                backup.state = BackupState.PARTIALLY_DELETED
                self._context.backup_layout.upload_backup_metadata(backup)

    def _delete_data_parts(
        self,
        backup: BackupMetadata,
        table: TableMetadata,
        excluded_parts: Set[str] = None,
    ) -> None:
        parts = table.get_parts(excluded_parts=excluded_parts)
        own_parts = [part for part in parts if not part.link]
        self._context.backup_layout.delete_data_parts(backup, own_parts)
        backup.remove_parts(table, parts)

    # pylint: disable=too-many-positional-arguments
    def _restore(
        self,
        sources: BackupSources,
        db_names: Sequence[str],
        tables: List[TableMetadata],
        exclude_tables: List[TableMetadata],
        replica_name: Optional[str] = None,
        cloud_storage_source_bucket: Optional[str] = None,
        cloud_storage_source_path: Optional[str] = None,
        cloud_storage_source_endpoint: Optional[str] = None,
        skip_cloud_storage: bool = False,
        clean_zookeeper_mode: CleanZooKeeperMode = CleanZooKeeperMode.DISABLED,
        keep_going: bool = False,
        restore_tables_in_replicated_database: bool = False,
    ) -> None:
        # pylint: disable=too-many-locals

        if sources.access:
            # Restore access control entities
            self._access_backup_manager.restore(self._context)

        if sources.udf:
            # Restore UDF
            self._udf_backup_manager.restore(self._context)

        if sources.named_collections:
            # Restore named collections
            self._nc_backup_manager.restore(self._context)

        if sources.schemas_included():
            databases: Dict[str, Database] = {}
            for db_name in db_names:
                db = self._context.backup_meta.get_database(db_name)
                # TODO For backward compatibility, remove when all backups rotated
                if db.engine is None:
                    db_sql = self._context.backup_layout.get_database_create_statement(
                        self._context.backup_meta, db.name
                    )
                    db.set_engine_from_sql(db_sql)
                databases[db_name] = db

            metadata_cleaner: Optional[MetadataCleaner] = None

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

            # Restore databases.
            self._database_backup_manager.restore(
                self._context,
                databases,
                keep_going,
                metadata_cleaner,
            )

            # Restore tables and data stored on local disks.
            self._table_backup_manager.restore(
                context=self._context,
                databases=databases,
                schema_only=sources.schema_only,
                tables=tables,
                exclude_tables=exclude_tables,
                metadata_cleaner=metadata_cleaner,
                cloud_storage_source_bucket=cloud_storage_source_bucket,
                cloud_storage_source_path=cloud_storage_source_path,
                cloud_storage_source_endpoint=cloud_storage_source_endpoint,
                skip_cloud_storage=skip_cloud_storage,
                keep_going=keep_going,
                restore_tables_in_replicated_database=restore_tables_in_replicated_database,
            )

            if sources.data and self._context.restore_context.has_failed_parts():
                msg = "Some parts are failed to attach"
                logging.warning(msg)

                if self._context.config["restore_fail_on_attach_error"]:
                    raise ClickhouseBackupError(msg)

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
