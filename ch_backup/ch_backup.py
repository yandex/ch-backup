"""
Clickhouse backup logic
"""
import socket
from collections import defaultdict
from copy import copy
from datetime import timedelta
from typing import Dict, List, Optional, Sequence, Set, Tuple

from ch_backup import logging
from ch_backup.backup.deduplication import (DedupReferences, collect_dedup_info,
                                            collect_dedup_references_for_batch_backup_deletion)
from ch_backup.backup.layout import BackupLayout
from ch_backup.backup.metadata import (BackupMetadata, BackupState, TableMetadata)
from ch_backup.backup.restore_context import RestoreContext
from ch_backup.backup_context import BackupContext
from ch_backup.clickhouse.config import ClickhouseConfig
from ch_backup.clickhouse.control import ClickhouseCTL
from ch_backup.clickhouse.schema import is_atomic_db_engine
from ch_backup.config import Config
from ch_backup.exceptions import BackupNotFound, ClickhouseBackupError
from ch_backup.logic.access import AccessBackup
from ch_backup.logic.cloud_storage_utils import fix_s3_oplog
from ch_backup.logic.database import DatabaseBackup
from ch_backup.logic.table import TableBackup
from ch_backup.logic.udf import UDFBackup
from ch_backup.util import now, utcnow
from ch_backup.version import get_version


# pylint: disable=too-many-instance-attributes
class ClickhouseBackup:
    """
    Clickhouse backup logic
    """

    # pylint: disable=too-many-instance-attributes

    def __init__(self, config: Config) -> None:
        self._context = BackupContext(config)
        self._context.ch_ctl_conf = config['clickhouse']
        self._context.main_conf = config['main']
        self._context.ch_ctl = ClickhouseCTL(self._context.ch_ctl_conf, self._context.main_conf)
        self._context.backup_layout = BackupLayout(config)
        self._context.config = config['backup']
        self._context.zk_config = config.get('zookeeper')
        self._context.restore_context = RestoreContext(self._context.config)
        self._context.ch_config = ClickhouseConfig(config)
        self._access_backup_manager = AccessBackup()
        self._database_backup_manager = DatabaseBackup()
        self._table_backup_manager = TableBackup()
        self._udf_backup_manager = UDFBackup()

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

    def backup(self,
               name: str,
               databases: Sequence[str] = None,
               tables: Sequence[str] = None,
               force: bool = False,
               labels: dict = None,
               schema_only: bool = False,
               backup_access_control: bool = False) -> Tuple[str, Optional[str]]:
        """
        Perform backup.

        If force is True, backup.min_interval config option is ignored.
        """
        # pylint: disable=too-many-locals,too-many-branches
        assert not (databases and tables)

        backup_labels: dict = copy(self._context.config.get('labels'))  # type: ignore
        if labels:
            backup_labels.update(labels)

        db_tables: Dict[str, list] = defaultdict(list)
        if tables:
            for table in tables or []:
                db_name, table_name = table.split('.', 1)
                db_tables[db_name].append(table_name)

            databases = list(db_tables.keys())

        if databases is None:
            databases = self._context.ch_ctl.get_databases(self._context.config['exclude_dbs'])

        backups_with_light_meta = self._context.backup_layout.get_backups(use_light_meta=True)

        last_backup = next(iter(backups_with_light_meta), None)
        if last_backup and not self._check_min_interval(last_backup, force):
            msg = 'Backup is skipped per backup.min_interval config option.'
            logging.info(msg)
            return last_backup.name, msg

        self._context.backup_meta = BackupMetadata(name=name,
                                                   path=self._context.backup_layout.get_backup_path(name),
                                                   labels=backup_labels,
                                                   version=get_version(),
                                                   ch_version=self._context.ch_ctl.get_version(),
                                                   time_format=self._context.config['time_format'],
                                                   schema_only=schema_only)

        self._context.backup_layout.upload_backup_metadata(self._context.backup_meta)

        logging.debug('Starting backup "%s" for databases: %s', self._context.backup_meta.name, ', '.join(databases))

        try:
            with self._context.locker():
                self._access_backup_manager.backup(self._context, backup_access_control=backup_access_control)
                self._udf_backup_manager.backup(self._context)
                self._database_backup_manager.backup(self._context, databases)

                dedup_info = collect_dedup_info(context=self._context,
                                                backups_with_light_meta=backups_with_light_meta,
                                                databases=databases)
                self._table_backup_manager.backup(self._context, databases, db_tables, dedup_info, schema_only)

                self._context.backup_meta.state = BackupState.CREATED
        except Exception:
            logging.critical('Backup failed', exc_info=True)
            self._context.backup_meta.state = BackupState.FAILED
            raise
        finally:
            self._context.backup_meta.update_end_time()
            self._context.backup_layout.upload_backup_metadata(self._context.backup_meta)

            if not self._context.config.get('keep_freezed_data_on_failure'):
                self._context.ch_ctl.remove_freezed_data()

        return self._context.backup_meta.name, None

    # pylint: disable=too-many-arguments,duplicate-code
    def restore(self,
                backup_name: str,
                databases: Sequence[str] = None,
                schema_only: bool = False,
                override_replica_name: str = None,
                force_non_replicated: bool = False,
                clean_zookeeper: bool = False,
                replica_name: Optional[str] = None,
                cloud_storage_source_bucket: str = None,
                cloud_storage_source_path: str = None,
                cloud_storage_source_endpoint: str = None,
                cloud_storage_latest: bool = False,
                keep_going: bool = False) -> None:
        """
        Restore specified backup
        """
        self._context.backup_meta = self._get_backup(backup_name)

        if cloud_storage_source_bucket is None and not schema_only:
            if self._context.backup_meta.has_s3_data() or self._context.backup_meta.cloud_storage.enabled:
                raise ClickhouseBackupError('Cloud storage source bucket must be set if backup has data on S3 disks')

        self._context.config['override_replica_name'] = override_replica_name or self._context.config.get(
            'override_replica_name')
        self._context.config[
            'force_non_replicated'] = force_non_replicated or self._context.config['force_non_replicated']

        if databases is None:
            databases = self._context.backup_meta.get_databases()
        else:
            # check all required databases exists in backup meta
            missed_databases = [
                db_name for db_name in databases if db_name not in self._context.backup_meta.get_databases()
            ]
            if missed_databases:
                logging.critical('Required databases %s were not found in backup metadata: %s',
                                 ', '.join(missed_databases), self._context.backup_meta.path)
                raise ClickhouseBackupError('Required databases were not found in backup metadata')
        with self._context.locker():
            self._restore(databases, schema_only, clean_zookeeper, replica_name, cloud_storage_source_bucket,
                          cloud_storage_source_path, cloud_storage_source_endpoint, cloud_storage_latest, keep_going)

    # pylint: disable=too-many-locals,too-many-nested-blocks,too-many-branches
    def fix_s3_oplog(self,
                     source_cluster_id: str = None,
                     shard: str = None,
                     cloud_storage_source_bucket: str = None,
                     cloud_storage_source_path: str = None,
                     dryrun: bool = False) -> None:
        """
        Fix S3 operations log.
        """
        if not self._context.config.get('cloud_storage'):
            return

        fix_s3_oplog(
            self._context.config['cloud_storage'],
            source_cluster_id if source_cluster_id else self._context.config['restore_from']['cid'],
            shard if shard else self._context.config['restore_from']['shard_name'],
            cloud_storage_source_bucket,
            cloud_storage_source_path,
            dryrun,
        )

    def delete(self, backup_name: str, purge_partial: bool) -> Tuple[Optional[str], Optional[str]]:
        """
        Delete the specified backup.
        """
        found = False
        deleting_backups = []
        retained_backups = []
        for backup in self._context.backup_layout.get_backups(use_light_meta=True):
            if backup.name == backup_name:
                deleting_backups.append(backup)
                found = True
                continue

            if purge_partial and backup.state != BackupState.CREATED:
                deleting_backups.append(backup)
            else:
                retained_backups.append(backup)

        if not found:
            raise BackupNotFound(backup_name)

        dedup_references = collect_dedup_references_for_batch_backup_deletion(
            layout=self._context.backup_layout,
            retained_backups_with_light_meta=retained_backups,
            deleting_backups_with_light_meta=deleting_backups)

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
        retain_time = self._context.config['retain_time']
        retain_count = self._context.config['retain_count']

        deleted_backup_names: List[str] = []

        if not retain_time and retain_count is None:
            logging.info('Retain policies are not specified')
            return deleted_backup_names, 'Retain policies are not specified.'

        retain_time_limit = None
        if retain_time:
            retain_time_limit = now() - timedelta(**retain_time)

        retained_backups: List[BackupMetadata] = []
        deleting_backups: List[BackupMetadata] = []
        backup_names = self._context.backup_layout.get_backup_names()
        for backup in self._context.backup_layout.get_backups(use_light_meta=True):
            if backup.name not in backup_names:
                logging.info('Deleting backup without metadata: %s', backup.name)
                self._context.backup_layout.delete_backup(backup.name)
                continue

            if retain_count > 0:
                logging.info('Preserving backup per retain count policy: %s, state %s', backup.name, backup.state)
                retained_backups.append(backup)
                if backup.state == BackupState.CREATED:
                    retain_count -= 1
                continue

            if retain_time_limit and backup.start_time >= retain_time_limit:
                logging.info('Preserving backup per retain time policy: %s, state %s', backup.name, backup.state)
                retained_backups.append(backup)
                continue

            deleting_backups.append(backup)

        dedup_references = collect_dedup_references_for_batch_backup_deletion(
            layout=self._context.backup_layout,
            retained_backups_with_light_meta=retained_backups,
            deleting_backups_with_light_meta=deleting_backups)

        for backup in deleting_backups:
            backup_name, _ = self._delete(backup, dedup_references[backup.name])
            if backup_name:
                deleted_backup_names.append(backup_name)

        return deleted_backup_names, None

    def restore_schema(self, source_host: str, source_port: int, exclude_dbs: List[str],
                       replica_name: Optional[str]) -> None:
        """
        Restore ClickHouse schema from replica, without s3.
        """
        source_conf = self._context.ch_ctl_conf.copy()
        source_conf.update(dict(host=source_host, port=source_port))
        source_ch_ctl = ClickhouseCTL(source_conf, self._context.main_conf)
        databases = source_ch_ctl.get_databases(exclude_dbs if exclude_dbs else self._context.config['exclude_dbs'])

        if self._context.config['override_replica_name'] is None and replica_name is not None:
            self._context.config['override_replica_name'] = replica_name
        if replica_name is None:
            replica_name = socket.getfqdn()
        with self._context.locker():
            self._udf_backup_manager.restore_schema(self._context, source_ch_ctl)
            self._database_backup_manager.restore_schema(self._context, source_ch_ctl, databases, replica_name)
            self._table_backup_manager.restore_schema(self._context, source_ch_ctl, databases, replica_name)

    def restore_access_control(self, backup_name: str) -> None:
        """Restore ClickHouse access control metadata."""
        self._context.backup_meta = self._get_backup(backup_name)
        with self._context.locker():
            self._access_backup_manager.restore(self._context)

    def _delete(self, backup_with_light_meta: BackupMetadata,
                dedup_references: DedupReferences) -> Tuple[Optional[str], Optional[str]]:
        logging.info('Deleting backup %s, state: %s', backup_with_light_meta.name, backup_with_light_meta.state)

        backup = self._context.backup_layout.reload_backup(backup_with_light_meta, use_light_meta=False)

        backup.state = BackupState.DELETING
        self._context.backup_layout.upload_backup_metadata(backup)

        try:
            # delete whole backup prefix if its data parts are not shared with other backups
            if not dedup_references:
                logging.info('Removing backup data entirely')
                self._context.backup_layout.delete_backup(backup.name)
                self._context.ch_ctl.system_unfreeze(backup.name)
                return backup.name, None

            logging.info('Removing non-shared backup data parts')
            for db_name in backup.get_databases():
                db_dedup_references = dedup_references.get(db_name, {})
                for table in backup.get_tables(db_name):
                    self._delete_data_parts(backup, table, db_dedup_references.get(table.name))

            self._context.ch_ctl.system_unfreeze(backup.name)
            return None, 'Backup was partially deleted as its data is in use by subsequent backups per ' \
                         'deduplication settings.'

        except Exception:
            logging.critical('Delete failed', exc_info=True)
            backup.state = BackupState.FAILED
            raise

        finally:
            self._context.backup_layout.wait()
            if dedup_references:
                backup.state = BackupState.PARTIALLY_DELETED
                self._context.backup_layout.upload_backup_metadata(backup)

    def _delete_data_parts(self,
                           backup: BackupMetadata,
                           table: TableMetadata,
                           excluded_parts: Set[str] = None) -> None:
        parts = table.get_parts(excluded_parts=excluded_parts)
        own_parts = [part for part in parts if not part.link]
        self._context.backup_layout.delete_data_parts(backup, own_parts)
        backup.remove_parts(table, parts)

    def _restore(self,
                 databases: Sequence[str],
                 schema_only: bool,
                 clean_zookeeper: bool = False,
                 replica_name: Optional[str] = None,
                 cloud_storage_source_bucket: Optional[str] = None,
                 cloud_storage_source_path: Optional[str] = None,
                 cloud_storage_source_endpoint: Optional[str] = None,
                 cloud_storage_latest: bool = False,
                 keep_going: bool = False) -> None:
        logging.debug('Restoring databases: %s', ', '.join(databases))

        # Restore UDF
        self._udf_backup_manager.restore(self._context)

        # Restore databases.
        self._database_backup_manager.restore(self._context, databases)

        # Restore tables and data stored on local disks.
        self._table_backup_manager.restore(self._context, databases, replica_name, cloud_storage_source_bucket,
                                           cloud_storage_source_path, cloud_storage_source_endpoint,
                                           cloud_storage_latest, clean_zookeeper, schema_only, keep_going)
        if self._context.config['restore_fail_on_attach_error'] and self._context.restore_context.has_failed_parts():
            raise ClickhouseBackupError("Some parts are failed to attach")

    def _get_backup(self, backup_name: str, use_light_meta: bool = False) -> BackupMetadata:
        backup = self._context.backup_layout.get_backup(backup_name, use_light_meta)
        if not backup:
            raise BackupNotFound(backup_name)

        return backup

    def _check_min_interval(self, last_backup: BackupMetadata, force: bool) -> bool:
        if force:
            return True

        min_interval = self._context.config.get('min_interval')
        if not min_interval:
            return True

        if not last_backup.end_time:
            return True

        if utcnow() - last_backup.end_time >= timedelta(**min_interval):
            return True

        return False

    def _is_db_atomic(self, db_name: str) -> bool:
        """
        Return True if database engine is Atomic, or False otherwise.
        """
        return is_atomic_db_engine(self._context.ch_ctl.get_database_engine(db_name))
