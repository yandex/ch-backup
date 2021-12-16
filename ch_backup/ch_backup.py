"""
Clickhouse backup logic
"""

import re
from collections import defaultdict, deque
from copy import copy
from datetime import timedelta
from itertools import chain
from os import remove
from os.path import exists, join
from time import sleep
from typing import Dict, Iterable, List, Optional, Sequence, Set, Tuple, Union

from ch_backup import logging
from ch_backup.backup.deduplication import (DatabaseDedupInfo, DedupReferences, TableDedupInfo, collect_dedup_info,
                                            collect_dedup_references_for_backup_deletion,
                                            collect_dedup_references_for_batch_backup_deletion, deduplicate_part)
from ch_backup.backup.layout import BackupLayout
from ch_backup.backup.metadata import (BackupMetadata, BackupState, TableMetadata)
from ch_backup.backup.restore_context import RestoreContext
from ch_backup.clickhouse.client import ClickhouseError
from ch_backup.clickhouse.control import ClickhouseCTL, Table
from ch_backup.config import Config
from ch_backup.exceptions import BackupNotFound, ClickhouseBackupError
from ch_backup.util import compare_schema, get_zookeeper_paths, now, utcnow
from ch_backup.version import get_version
from ch_backup.zookeeper.zookeeper import ZookeeperCTL


class ClickhouseBackup:
    """
    Clickhouse backup logic
    """
    def __init__(self, config: Config) -> None:
        self._ch_ctl_conf = config['clickhouse']
        self._ch_ctl = ClickhouseCTL(self._ch_ctl_conf)
        self._backup_layout = BackupLayout(config)
        self._config = config['backup']
        self._zk_config = config.get('zookeeper')
        self._restore_context = RestoreContext(self._config)

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
        for backup in self._backup_layout.get_backups(use_light_meta=True):
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

        backup_labels = copy(self._config.get('labels'))
        if labels:
            backup_labels.update(labels)

        db_tables: Dict[str, list] = defaultdict(list)
        if tables:
            for table in tables or []:
                db_name, table_name = table.split('.', 1)
                db_tables[db_name].append(table_name)

            databases = list(db_tables.keys())

        if databases is None:
            databases = self._ch_ctl.get_databases(self._config['exclude_dbs'])

        backups_with_light_meta = self._backup_layout.get_backups(use_light_meta=True)

        last_backup = next(iter(backups_with_light_meta), None)
        if last_backup and not self._check_min_interval(last_backup, force):
            msg = 'Backup is skipped per backup.min_interval config option.'
            logging.info(msg)
            return last_backup.name, msg

        backup_meta = BackupMetadata(name=name,
                                     path=self._backup_layout.get_backup_path(name),
                                     labels=backup_labels,
                                     version=get_version(),
                                     ch_version=self._ch_ctl.get_version(),
                                     time_format=self._config['time_format'],
                                     schema_only=schema_only)

        self._backup_layout.upload_backup_metadata(backup_meta)

        logging.debug('Starting backup "%s" for databases: %s', backup_meta.name, ', '.join(databases))

        try:
            if backup_access_control or self._config.get('backup_access_control'):
                self._backup_access_control(backup_meta)

            dedup_info = collect_dedup_info(config=self._config,
                                            layout=self._backup_layout,
                                            backups_with_light_meta=backups_with_light_meta,
                                            databases=databases,
                                            schema_only=schema_only)

            for db_name in databases:
                self._backup_database(backup_meta, db_name, db_tables[db_name], dedup_info.database(db_name),
                                      schema_only)
            backup_meta.state = BackupState.CREATED
        except Exception:
            logging.critical('Backup failed', exc_info=True)
            backup_meta.state = BackupState.FAILED
            raise
        finally:
            backup_meta.update_end_time()
            self._backup_layout.upload_backup_metadata(backup_meta)

            if not self._config.get('keep_freezed_data_on_failure'):
                self._ch_ctl.remove_freezed_data()

        return backup_meta.name, None

    # pylint: disable=too-many-arguments
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
                cloud_storage_latest: bool = False) -> None:
        """
        Restore specified backup
        """
        backup_meta = self._get_backup(backup_name)

        if backup_meta.has_s3_data() and cloud_storage_source_bucket is None and not schema_only:
            raise ClickhouseBackupError('Cloud storage source bucket must be set if backup has data on S3 disks')

        self._config['override_replica_name'] = override_replica_name or self._config.get('override_replica_name')
        self._config['force_non_replicated'] = force_non_replicated or self._config['force_non_replicated']

        if databases is None:
            databases = backup_meta.get_databases()
        else:
            # check all required databases exists in backup meta
            missed_databases = [db_name for db_name in databases if db_name not in backup_meta.get_databases()]
            if missed_databases:
                logging.critical('Required databases %s were not found in backup metadata: %s',
                                 ', '.join(missed_databases), backup_meta.path)
                raise ClickhouseBackupError('Required databases were not found in backup metadata')

        self._restore(backup_meta, databases, schema_only, clean_zookeeper, replica_name, cloud_storage_source_bucket,
                      cloud_storage_source_path, cloud_storage_latest)

    def delete(self, backup_name: str) -> Tuple[Optional[str], Optional[str]]:
        """
        Delete the specified backup.
        """
        deleting_backup = None
        retained_backups = []
        for backup in self._backup_layout.get_backups(use_light_meta=True):
            if backup.name == backup_name:
                deleting_backup = backup
                break

            retained_backups.append(backup)

        if not deleting_backup:
            raise BackupNotFound(backup_name)

        dedup_references = collect_dedup_references_for_backup_deletion(
            layout=self._backup_layout,
            retained_backups_with_light_meta=retained_backups,
            deleting_backup_with_light_meta=deleting_backup)

        return self._delete(deleting_backup, dedup_references)

    def purge(self) -> Tuple[Sequence[str], Optional[str]]:
        """
        Purge backups.
        """
        retain_time = self._config['retain_time']
        retain_count = self._config['retain_count']

        deleted_backup_names: List[str] = []

        if not retain_time and retain_count is None:
            logging.info('Retain policies are not specified')
            return deleted_backup_names, 'Retain policies are not specified.'

        retain_time_limit = None
        if retain_time:
            retain_time_limit = now() - timedelta(**retain_time)

        retained_backups: List[BackupMetadata] = []
        deleting_backups: List[BackupMetadata] = []
        backup_names = self._backup_layout.get_backup_names()
        for backup in self._backup_layout.get_backups(use_light_meta=True):
            if backup.name not in backup_names:
                logging.info('Deleting backup without metadata: %s', backup.name)
                self._backup_layout.delete_backup(backup.name)
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
            layout=self._backup_layout,
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
        source_conf = self._ch_ctl_conf.copy()
        source_conf.update(dict(host=source_host, port=source_port))
        source_ch_ctl = ClickhouseCTL(config=source_conf)
        databases = source_ch_ctl.get_databases(exclude_dbs if exclude_dbs else self._config['exclude_dbs'])

        tables: List[Table] = []
        present_databases = self._ch_ctl.get_databases()
        for database in databases:
            logging.debug('Restoring database "%s"', database)
            if not _has_embedded_metadata(database) and database not in present_databases:
                db_sql = source_ch_ctl.get_database_schema(database)
                self._ch_ctl.restore_meta(db_sql)
            tables.extend(source_ch_ctl.get_tables_ordered(database))
        self._restore_tables(self._filter_present_tables(databases, tables),
                             clean_zookeeper=True,
                             replica_name=replica_name)

    def restore_access_control(self, backup_name: str) -> None:
        """Restore ClickHouse access control metadata."""
        backup_meta = self._get_backup(backup_name)
        objects = backup_meta.get_access_control()
        for name in _get_access_control_files(objects):
            self._backup_layout.download_access_control_file(backup_meta.name, name)

    def _backup_access_control(self, backup_meta):
        objects = self._ch_ctl.get_access_control_objects()
        backup_meta.set_access_control(objects)

        # ClickHouse creates file need_rebuild_lists.mark after access management objects modification
        # to show that lists should be updated.
        mark_file = join(self._ch_ctl_conf['access_control_path'], 'need_rebuild_lists.mark')
        while exists(mark_file):
            logging.debug(f'Waiting for clickhouse rebuild access control lists. File "{mark_file}".')
            sleep(1)

        for name in _get_access_control_files(objects):
            self._backup_layout.upload_access_control_file(backup_meta.name, name)

    def _backup_database(self,
                         backup_meta: BackupMetadata,
                         db_name: str,
                         tables: Sequence[str],
                         dedup_info: DatabaseDedupInfo,
                         schema_only: bool = False) -> None:
        """
        Backup database.
        """
        logging.debug('Performing database backup for "%s"', db_name)

        if not _has_embedded_metadata(db_name):
            schema = self._ch_ctl.get_database_schema(db_name)
            self._backup_layout.upload_database_create_statement(backup_meta.name, db_name, schema)

        backup_meta.add_database(db_name)

        if not self._is_db_external(db_name):
            for table in self._ch_ctl.get_tables_ordered(db_name, tables):
                table_meta = TableMetadata(table.database, table.name, table.engine, table.uuid)
                self._backup_table_schema(backup_meta, table)
                if not schema_only:
                    # table_meta will be populated with parts info
                    self._backup_table_data(backup_meta, table, table_meta, dedup_info.table(table.name))

                backup_meta.add_table(table_meta)

        self._backup_layout.upload_backup_metadata(backup_meta)

    def _backup_table_schema(self, backup_meta: BackupMetadata, table: Table) -> None:
        """
        Backup table object.
        """
        logging.debug('Uploading table schema for "%s"."%s"', table.database, table.name)

        self._backup_layout.upload_table_create_statement(backup_meta.name, table.database, table.name,
                                                          table.create_statement)

    def _backup_table_data(self, backup_meta: BackupMetadata, table: Table, table_meta: TableMetadata,
                           dedup_info: TableDedupInfo) -> None:
        """
        Backup table with data opposed to schema only.
        """
        logging.debug('Performing table backup for "%s"."%s"', table.database, table.name)

        if not _is_merge_tree(table):
            logging.info('Skipping table backup for non MergeTree table "%s"."%s"', table.database, table.name)
            return

        # ClickHouse will place shadow data under this directory.
        # '-' character is replaced to '_' to avoid unnecessary escaping on CH side.
        backup_name = backup_meta.name.replace('-', '_')

        try:
            self._ch_ctl.freeze_table(backup_name, table)
        except ClickhouseError:
            if self._ch_ctl.does_table_exist(table.database, table.name):
                raise

            logging.warning('Table "%s"."%s" was removed by a user during backup', table.database, table.name)
            return

        for disk in self._ch_ctl.get_disks().values():
            if disk.type == 's3':
                # Save revision for S3 disks.
                revision = self._ch_ctl.read_s3_disk_revision(disk.name, backup_name)
                if revision:
                    backup_meta.s3_revisions[disk.name] = revision
                    logging.debug('Save revision %d for disk %s', revision, disk.name)

        uploaded_parts = []
        for data_path, disk in table.paths_with_disks:
            freezed_parts = self._ch_ctl.list_freezed_parts(table, disk, data_path, backup_name)

            for fpart in freezed_parts:
                logging.debug('Working on %s', fpart)

                if disk.type == 's3':
                    table_meta.add_part(fpart.to_part_metadata())
                    continue

                # trying to find part in storage
                part = deduplicate_part(self._backup_layout, fpart, dedup_info)
                if part:
                    self._ch_ctl.remove_freezed_part(fpart)
                else:
                    self._backup_layout.upload_data_part(backup_meta.name, fpart)
                    part = fpart.to_part_metadata()
                    uploaded_parts.append(part)

                table_meta.add_part(part)

        self._backup_layout.wait()

        self._validate_uploaded_parts(backup_meta, uploaded_parts)

        self._ch_ctl.remove_freezed_data()

    def _validate_uploaded_parts(self, backup_meta, uploaded_parts):
        if self._config['validate_part_after_upload']:
            invalid_parts = []

            for part in uploaded_parts:
                if not self._backup_layout.check_data_part(backup_meta.path, part):
                    invalid_parts.append(part)

            if invalid_parts:
                for part in invalid_parts:
                    logging.error(f'Uploaded part is broken, {part.database}.{part.table}: {part.name}')
                raise RuntimeError(f'Uploaded parts are broken, {", ".join(map(lambda p: p.name, invalid_parts))}')

    def _delete(self, backup_with_light_meta: BackupMetadata,
                dedup_references: DedupReferences) -> Tuple[Optional[str], Optional[str]]:
        logging.info('Deleting backup %s, state: %s', backup_with_light_meta.name, backup_with_light_meta.state)

        backup = self._backup_layout.reload_backup(backup_with_light_meta, use_light_meta=False)

        backup.state = BackupState.DELETING
        self._backup_layout.upload_backup_metadata(backup)

        try:
            # delete whole backup prefix if its data parts are not shared with other backups
            if not dedup_references:
                logging.info('Removing backup data entirely')
                self._backup_layout.delete_backup(backup.name)
                return backup.name, None

            logging.info('Removing non-shared backup data parts')
            for db_name in backup.get_databases():
                db_dedup_references = dedup_references.get(db_name, {})
                for table in backup.get_tables(db_name):
                    self._delete_data_parts(backup, table, db_dedup_references.get(table.name))

            return None, 'Backup was partially deleted as its data is in use by subsequent backups per ' \
                         'deduplication settings.'

        except Exception:
            logging.critical('Delete failed', exc_info=True)
            backup.state = BackupState.FAILED
            raise

        finally:
            self._backup_layout.wait()
            if dedup_references:
                backup.state = BackupState.PARTIALLY_DELETED
                self._backup_layout.upload_backup_metadata(backup)

    def _delete_data_parts(self,
                           backup: BackupMetadata,
                           table: TableMetadata,
                           excluded_parts: Set[str] = None) -> None:
        parts = table.get_parts(excluded_parts=excluded_parts)
        own_parts = [part for part in parts if not part.link]
        self._backup_layout.delete_data_parts(backup, own_parts)
        backup.remove_parts(table, parts)

    def _restore(self,
                 backup_meta: BackupMetadata,
                 databases: Sequence[str],
                 schema_only: bool,
                 clean_zookeeper: bool = False,
                 replica_name: Optional[str] = None,
                 cloud_storage_source_bucket: str = None,
                 cloud_storage_source_path: str = None,
                 cloud_storage_latest: bool = False) -> None:
        logging.debug('Restoring databases: %s', ', '.join(databases))
        tables_meta = list(
            chain(*[backup_meta.get_tables(db_name) for db_name in databases if not self._is_db_external(db_name)]))
        tables = list(map(lambda meta: self._get_table_from_meta(backup_meta, meta), tables_meta))
        self._restore_database_objects(backup_meta, databases)
        self._restore_tables(self._filter_present_tables(databases, tables), clean_zookeeper, replica_name)
        if not schema_only:
            if backup_meta.has_s3_data():
                assert cloud_storage_source_bucket is not None, "Cloud storage source bucket is not set"
                assert cloud_storage_source_path is not None, "Cloud storage source path is not set"
                source_bucket: str = cloud_storage_source_bucket
                source_path: str = cloud_storage_source_path
                self._restore_cloud_storage_data(backup_meta, source_bucket, source_path, cloud_storage_latest)
            self._restore_data(backup_meta, filter(_is_merge_tree, tables_meta))

    def _restore_tables(self,
                        tables: Iterable[Table],
                        clean_zookeeper: bool = False,
                        replica_name: Optional[str] = None) -> None:
        merge_tree_tables = []
        distributed_tables = []
        view_tables = []
        other_tables = []
        for table in tables:
            if _is_merge_tree(table):
                merge_tree_tables.append(table)
            elif _is_distributed(table):
                distributed_tables.append(table)
            elif _is_view(table):
                view_tables.append(table)
            else:
                other_tables.append(table)

        if clean_zookeeper and len(self._zk_config.get('hosts')) > 0:
            zk_ctl = ZookeeperCTL(self._zk_config)
            zk_ctl.delete_replica_metadata(get_zookeeper_paths(filter(_is_replicated, merge_tree_tables)),
                                           replica_name, self._ch_ctl.get_macros())

        self._restore_table_objects(chain(merge_tree_tables, other_tables, distributed_tables, view_tables))

    def _restore_database_objects(self, backup_meta: BackupMetadata, databases: Iterable[str]) -> None:
        present_databases = self._ch_ctl.get_databases()

        for db_name in databases:
            if not _has_embedded_metadata(db_name) and db_name not in present_databases:
                db_sql = self._backup_layout.get_database_create_statement(backup_meta, db_name)
                self._ch_ctl.restore_meta(db_sql)

    def _restore_table_objects(self, tables: Iterable[Table]) -> None:
        errors: List[Tuple[Union[Table], Exception]] = []
        unprocessed = deque(table for table in tables)
        while unprocessed:
            table = unprocessed.popleft()
            try:
                if _is_view(table):
                    self._restore_view_object(table)
                else:
                    self._ch_ctl.restore_meta(self._rewrite_with_explicit_uuid(table))
            except Exception as e:
                errors.append((table, e))
                unprocessed.append(table)
                if len(errors) > len(unprocessed):
                    break
            else:
                errors.clear()

        if errors:
            logging.error('Failed to restore tables:\n%s',
                          '\n'.join(f'"{v.database}"."{v.name}": {e!r}' for v, e in errors))
            failed_views_str = ', '.join(f'"{v.database}"."{v.name}"' for v, _ in errors)
            raise ClickhouseBackupError(f'Failed to restore tables: {failed_views_str}')

    def _restore_view_object(self, view: Table) -> None:
        # Create view through attach to omit checks.
        database_meta = self._ch_ctl.get_database_metadata_path(view.database)
        table_meta = join(database_meta, f'{view.name}.sql')
        try:
            create_sql = self._rewrite_view_object(view)
            with open(table_meta, 'w', encoding='utf-8') as f:
                f.write(create_sql)
            self._ch_ctl.chown_dir(database_meta)
            self._ch_ctl.attach_table(view)
        except Exception as e:
            if exists(table_meta):
                remove(table_meta)
            logging.debug(f'Failed to restore view via metadata attach, query: "{create_sql}", '
                          f'error: "{repr(e)}", fallback to create')
            self._ch_ctl.restore_meta(self._rewrite_merge_tree_object(view.create_statement))

    def _restore_cloud_storage_data(self, backup_meta: BackupMetadata, source_bucket: str, source_path: str,
                                    cloud_storage_latest: bool) -> None:
        for disk_name, revision in backup_meta.s3_revisions.items():
            logging.debug(f'Restore disk {disk_name} to revision {revision}')

            self._ch_ctl.create_s3_disk_restore_file(disk_name, revision if not cloud_storage_latest else 0,
                                                     source_bucket, source_path)

            if self._restore_context.disk_restarted(disk_name):
                logging.debug(f'Skip restoring disk {disk_name} as it has already been restored')
                continue

            try:
                self._ch_ctl.restart_disk(disk_name, self._restore_context)
            finally:
                self._restore_context.dump_state()

    def _restore_data(self, backup_meta: BackupMetadata, tables: Iterable[TableMetadata]) -> None:
        for table_meta in tables:
            try:
                logging.debug('Running table "%s.%s" data restore', table_meta.database, table_meta.name)

                self._restore_context.add_table(table_meta)
                maybe_table = self._ch_ctl.get_table(table_meta.database, table_meta.name)
                assert maybe_table is not None, f'Table not found {table_meta.database}.{table_meta.name}'
                table: Table = maybe_table

                attach_parts = []
                for part in table_meta.get_parts():
                    if self._restore_context.part_restored(part):
                        logging.debug(f'{table.database}.{table.name} part {part.name} already restored, skipping it')
                        continue

                    if part.disk_name not in backup_meta.s3_revisions.keys():
                        fs_part_path = self._ch_ctl.get_detached_part_path(table, part.disk_name, part.name)
                        self._backup_layout.download_data_part(backup_meta, part, fs_part_path)

                    attach_parts.append(part)

                self._backup_layout.wait()

                self._ch_ctl.chown_detached_table_parts(table, self._restore_context)
                for part in attach_parts:
                    try:
                        logging.debug('Attaching "%s.%s" part: %s', table_meta.database, table.name, part.name)
                        self._ch_ctl.attach_part(table, part.name)
                        self._restore_context.add_part(part)
                    except Exception as e:
                        logging.warning('Attaching "%s.%s" part %s failed: %s', table_meta.database, table.name,
                                        part.name, repr(e))
                        self._restore_context.add_failed_part(part, e)
            finally:
                self._restore_context.dump_state()

    def _get_table_from_meta(self, backup_meta: BackupMetadata, meta: TableMetadata) -> Table:
        table = Table(meta.database, meta.name, meta.engine, [], [],
                      self._backup_layout.get_table_create_statement(backup_meta, meta.database, meta.name), meta.uuid)
        table.create_statement = self._rewrite_merge_tree_object(table.create_statement)
        return table

    def _filter_present_tables(self, databases: Iterable[str], tables: Iterable[Table]) -> Iterable[Table]:
        present_tables = {}
        for database in databases:
            for table in self._ch_ctl.get_tables_ordered(database):
                present_tables[(table.database, table.name)] = table

        for table in tables:
            key = (table.database, table.name)
            if key not in present_tables:
                yield table
            elif not compare_schema(present_tables[key].create_statement, table.create_statement):
                raise ClickhouseBackupError(f'Table {key} has different schema with backup "{table.create_statement}" '
                                            f'!= "{present_tables[key].create_statement}"')

    def _get_backup(self, backup_name: str, use_light_meta: bool = False) -> BackupMetadata:
        backup = self._backup_layout.get_backup(backup_name, use_light_meta)
        if not backup:
            raise BackupNotFound(backup_name)

        return backup

    def _check_min_interval(self, last_backup: BackupMetadata, force: bool) -> bool:
        if force:
            return True

        min_interval = self._config.get('min_interval')
        if not min_interval:
            return True

        if not last_backup.end_time:
            return True

        if utcnow() - last_backup.end_time >= timedelta(**min_interval):
            return True

        return False

    def _rewrite_merge_tree_object(self, table_sql: str) -> str:
        if self._config['force_non_replicated']:
            match = re.search(r"(?P<replicated>Replicated)\S{0,20}MergeTree\((?P<params>('[^']+', '[^']+'(,\s*|))|)",
                              table_sql)
            if match:
                params = match.group('params')
                if len(params) > 0:
                    table_sql = table_sql.replace(params, '').replace(match.group('replicated'), '')
                    table_sql = table_sql.replace('MergeTree()', 'MergeTree')

        if self._config['override_replica_name']:
            match = re.search(r"Replicated\S{0,20}MergeTree\('[^']+', (?P<replica>\'\S+\')", table_sql)
            if match:
                table_sql = table_sql.replace(match.group('replica'), f"'{self._config['override_replica_name']}'")

        return table_sql

    def _rewrite_view_object(self, table: Table) -> str:
        if table.uuid and self._ch_ctl.get_database_schema(table.database).find('ENGINE = Atomic') != -1:
            # For 21.4 it's required to explicitly set inner table UUID for MV.
            inner_uuid_clause = ''
            if _is_materialized_view(table) and self._ch_ctl.match_ch_version('21.4'):
                mv_inner_table = self._ch_ctl.get_table(table.database, f'.inner_id.{table.uuid}')
                if mv_inner_table:
                    inner_uuid_clause = f"TO INNER UUID '{mv_inner_table.uuid}'"

            table_sql = re.sub(
                f'^CREATE (?P<mat>(MATERIALIZED )?)VIEW (?P<table_name>`?{table.database}`?.`?{table.name}`?) ',
                f"ATTACH \\g<mat>VIEW \\g<table_name> UUID '{table.uuid}' {inner_uuid_clause} ",
                table.create_statement)
        else:
            table_sql = re.sub(r'^CREATE (?P<mat>(MATERIALIZED )?)VIEW', r'ATTACH \g<mat>VIEW', table.create_statement)

        return self._rewrite_merge_tree_object(table_sql)

    def _rewrite_with_explicit_uuid(self, table: Table) -> str:
        table_sql = table.create_statement
        # Rewrite create statement with explicit table UUID (as was in backup).
        if table.uuid and self._ch_ctl.get_database_schema(table.database).find('ENGINE = Atomic') != -1:
            # CREATE TABLE <db-name>.<table-name> $ (...)
            # UUID clause is inserted to $ place.
            index = table.create_statement.find('(')
            assert index != -1, f'Unable to find UUID insertion point for the following create table statement: ' \
                                f'{table_sql} '
            return table_sql[:index] + f"UUID '{table.uuid}' " + table_sql[index:]
        return table_sql

    def _is_db_external(self, db_name: str) -> bool:
        """
        Return True if DB's engine is one of:
        - MySQL
        - MaterializedMySQL
        - PostgreSQL
        - MaterializedPostgreSQL
        or False otherwise
        """
        return _is_external_db_engine(self._ch_ctl.get_database_engine(db_name))


def _get_access_control_files(objects: Sequence[str]) -> chain:
    """
    Return list of file to be backuped/restored .
    """
    lists = ['users.list', 'roles.list', 'quotas.list', 'row_policies.list', 'settings_profiles.list']
    return chain(lists, map(lambda obj: f'{obj}.sql', objects))


def _has_embedded_metadata(db_name: str) -> bool:
    """
    Return True if db create statement shouldn't be uploaded and applied with restore.
    """
    return db_name in [
        'default',
        'system',
        '_temporary_and_external_tables',
        'information_schema',
        'INFORMATION_SCHEMA',
    ]


def _is_merge_tree(table: Union[Table, TableMetadata]) -> bool:
    """
    Return True if table belongs to merge tree table engine family, or False otherwise.
    """
    return table.engine.find('MergeTree') != -1


def _is_replicated(table: Union[Table, TableMetadata]) -> bool:
    """
    Return True if table belongs to replicated merge tree table engine family, or False otherwise.
    """
    return table.engine.find('Replicated') != -1


def _is_distributed(table: Union[Table, TableMetadata]) -> bool:
    """
    Return True if table has distributed engine, or False otherwise.
    """
    return table.engine == 'Distributed'


def _is_view(table: Union[Table, TableMetadata]) -> bool:
    """
    Return True if table is a view, or False otherwise.
    """
    return table.engine in ('View', 'MaterializedView')


def _is_materialized_view(table: Union[Table, TableMetadata]) -> bool:
    """
    Return True if table is a view, or False otherwise.
    """
    return table.engine == 'MaterializedView'


def _is_external_db_engine(db_engine: str) -> bool:
    """
    Return True if DB's engine is one of:
    - MySQL
    - MaterializedMySQL
    - PostgreSQL
    - MaterializedPostgreSQL
    or False otherwise
    """
    return any([
        db_engine == 'MySQL',
        db_engine == 'MaterializedMySQL',
        db_engine == 'PostgreSQL',
        db_engine == 'MaterializedPostgreSQL',
    ])
