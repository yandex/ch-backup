"""
Clickhouse backup logic
"""

import logging
from collections import defaultdict
from copy import copy
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Sequence, Tuple

from ch_backup.backup.layout import ClickhouseBackupLayout
from ch_backup.backup.metadata import BackupMetadata, BackupState, PartMetadata
from ch_backup.clickhouse.control import ClickhouseCTL, FreezedPart
from ch_backup.config import Config
from ch_backup.exceptions import ClickhouseBackupError, StorageError
from ch_backup.util import now, utc_fromtimestamp, utcnow


class ClickhouseBackup:
    """
    Clickhouse backup logic
    """

    def __init__(self, config: Config) -> None:
        self._ch_ctl = ClickhouseCTL(config['clickhouse'])
        self._backup_layout = ClickhouseBackupLayout(config, self._ch_ctl)
        self._config = config['backup']
        self._existing_backups = []  # type: List[BackupMetadata]
        self._dedup_time = None

    def get(self, backup_name: str) -> BackupMetadata:
        """
        Get backup information.
        """
        return self._get_backup_meta(backup_name)

    def list(self, all_opt=True) -> Tuple[Sequence, Sequence]:
        """
        Get list of existing backup names.
        """
        self._load_existing_backups(load_all=all_opt)

        report = []
        fields = ('name', 'state', 'start_time', 'end_time', 'size',
                  'real_size', 'ch_version')

        i_state = fields.index('state')
        for backup_meta in self._existing_backups:
            entry_report = [str(getattr(backup_meta, x, None)) for x in fields]
            entry_report[i_state] = backup_meta.state.value
            report.append(entry_report)

        return fields, report

    def backup(self, databases=None, tables=None, force=False,
               labels=None) -> Tuple[str, Optional[str]]:
        """
        Perform backup.

        If force is True, backup.min_interval config option is ignored.
        """
        assert not (databases and tables)

        backup_labels = copy(self._config.get('labels'))
        if labels:
            backup_labels.update(labels)

        db_tables = defaultdict(list)  # type: Dict[str, list]
        if tables:
            for table in tables or []:
                db_name, table_name = table.split('.', 1)
                db_tables[db_name].append(table_name)

            databases = list(db_tables.keys())

        if databases is None:
            databases = self._ch_ctl.get_all_databases(
                self._config['exclude_dbs'])

        # load existing backups if deduplication is enabled
        if self._config.get('deduplicate_parts'):
            backup_age_limit = utcnow() - timedelta(
                **self._config['deduplication_age_limit'])

            self._load_existing_backups(backup_age_limit)

        last_backup = self._get_last_backup()
        if last_backup and not self._check_min_interval(last_backup, force):
            msg = 'Backup is skipped per backup.min_interval config option'
            logging.info(msg)
            return (last_backup.name, msg)

        backup_meta = BackupMetadata(
            name=self._backup_layout.backup_name,
            path=self._backup_layout.backup_path,
            labels=backup_labels,
            ch_version=self._ch_ctl.get_version())

        backup_meta.state = BackupState.CREATING
        backup_meta.update_start_time()
        self._backup_layout.save_backup_meta(backup_meta)

        logging.debug('Starting backup "%s" for databases: %s',
                      backup_meta.name, ', '.join(databases))

        try:
            for db_name in databases:
                self._backup_database(backup_meta, db_name, db_tables[db_name])
            backup_meta.state = BackupState.CREATED
        except Exception:
            logging.critical('Backup failed', exc_info=True)
            backup_meta.state = BackupState.FAILED
            raise
        finally:
            backup_meta.update_end_time()
            self._backup_layout.save_backup_meta(backup_meta)

        return (backup_meta.name, None)

    def restore(self,
                backup_name: str,
                databases: Sequence[str] = None,
                schema_only=False) -> None:
        """
        Restore specified backup
        """
        backup_meta = self._get_backup_meta(backup_name)

        if databases is None:
            databases = backup_meta.get_databases()
        else:
            # check all required databases exists in backup meta
            missed_databases = [
                db_name for db_name in databases
                if db_name not in backup_meta.get_databases()
            ]
            if missed_databases:
                logging.critical(
                    'Required databases %s were not found in backup meta: %s',
                    ', '.join(missed_databases), backup_meta.path)
                raise ClickhouseBackupError(
                    'Required databases were not found in backup struct')

        for db_name in databases:
            self._restore_database_schema(db_name, backup_meta)
            if schema_only:
                logging.debug(
                    'Don\'t restore %s data, cause schema_only is set %r',
                    db_name, schema_only)
            else:
                self._restore_database_data(db_name, backup_meta)

    def _backup_database(self,
                         backup_meta: BackupMetadata,
                         db_name: str,
                         tables: List[str] = None):
        """
        Backup database.
        """
        logging.debug('Running database backup: %s', db_name)

        db_remote_path = self._backup_database_meta(db_name)

        backup_meta.add_database(db_name, db_remote_path)

        # get db objects ordered by mtime
        tables = self._ch_ctl.get_tables_ordered(db_name, tables)
        for table_name in tables:
            self._backup_table(backup_meta, db_name, table_name)

    def _backup_table(self, backup_meta: BackupMetadata, db_name: str,
                      table_name: str) -> None:
        """
        Backup table.
        """
        logging.debug('Running table "%s.%s" backup', db_name, table_name)

        table_remote_path = self._backup_table_meta(db_name, table_name)

        backup_meta.add_table(db_name, table_name, table_remote_path)

        partitions = self._ch_ctl.get_partitions(db_name, table_name)
        for partition in partitions:

            fpartition = self._ch_ctl.freeze_partition(partition)
            for fpart in self._ch_ctl.get_freezed_parts(fpartition):
                logging.debug('Working on data part %s', fpart)

                # trying to find part in storage
                link, existing_part = self._deduplicate_part(fpart)
                if link and existing_part:
                    part_remote_paths = existing_part.paths
                else:
                    logging.debug('Backing up data part %s', fpart.name)

                    part_remote_paths = self._backup_layout.save_part_data(
                        fpart)

                part = PartMetadata(fpart, link, part_remote_paths)

                backup_meta.add_part(part)

        logging.debug('Waiting for uploads')
        self._backup_layout.wait()

        self._ch_ctl.remove_freezed_data()

    def delete(self, backup_name: str) -> Tuple[Optional[str], Optional[str]]:
        """
        Delete specified backup
        """
        if not self._existing_backups:
            self._load_existing_backups()

        try:
            current_index = next(i
                                 for i, e in enumerate(self._existing_backups)
                                 if e.name == backup_name)
        except StopIteration:
            logging.error('Backup "%s" was not found', backup_name)
            raise ClickhouseBackupError('Required backup was not found')

        backup_meta = self._existing_backups[current_index]

        # iterate over all newer backups
        # get parts which were deduplicated to current
        # [1, 2, 3, 4] -> [3, 2, 1]
        newer_backups = self._existing_backups[current_index - 1::-1]\
            if current_index != 0 else []

        logging.debug('Running backup delete: %s', backup_name)
        prev_state = backup_meta.state
        backup_meta.state = BackupState.DELETING
        self._backup_layout.save_backup_meta(backup_meta)

        is_changed, delete_paths = self._pop_deleting_paths(
            backup_meta, newer_backups)
        is_empty = backup_meta.is_empty()

        try:
            # delete whole backup prefix if backup entry is empty
            if is_empty:
                logging.info('Running removing path of backup "%s": %s',
                             backup_name, backup_meta.path)
                self._backup_layout.delete_backup_path(backup_name)
                return backup_name, None

            if is_changed:
                # some data (not linked) parts were deleted
                if delete_paths:
                    logging.info(
                        'Running removing of deleted parts for backup "%s"',
                        backup_name)
                    self._backup_layout.delete_loaded_files(delete_paths)

                backup_meta.state = BackupState.PARTIALLY_DELETED
                return None, 'Backup was partially deleted as its data is ' \
                             'in use by subsequent backups per ' \
                             'deduplication settings.'

            logging.info('Nothing was found for deletion')
            backup_meta.state = prev_state
            return None, 'Backup was not deleted as its data is in use ' \
                         'by subsequent backups per deduplication ' \
                         'settings.'

        except Exception:
            logging.critical('Delete failed', exc_info=True)
            backup_meta.state = BackupState.FAILED
            raise

        finally:
            logging.debug('Waiting for completion of storage operations')
            self._backup_layout.wait()
            if not is_empty:
                self._backup_layout.save_backup_meta(backup_meta)

    def purge(self):
        """
        Purge backups
        """
        retain_time = self._config['retain_time']
        retain_count = self._config['retain_count']
        purge_count_backup_names = []
        purge_time_backup_names = []

        if not retain_time and retain_count is None:
            logging.info('Retain policies are not specified')
            return

        if not self._existing_backups:
            self._load_existing_backups()

        if not self._existing_backups:
            logging.debug('Existing backups are not found')
            return

        if retain_time:
            backup_age_limit = now() - timedelta(**retain_time)

            for backup_meta in self._existing_backups:
                if backup_meta.end_time < backup_age_limit:
                    purge_time_backup_names.append(backup_meta.name)

            logging.debug('Purge backups using retain time policy: %s',
                          purge_time_backup_names)

        if retain_count is not None:
            existing_backup_names = [b.name for b in self._existing_backups]
            purge_count_backup_names = existing_backup_names[retain_count:]
            logging.debug('Purge backups using retain count policy: %s',
                          purge_count_backup_names)

        if retain_count is None or not retain_time:
            purge_backup_names = purge_count_backup_names or \
                                 purge_time_backup_names
        else:
            purge_backup_names = min(
                purge_count_backup_names, purge_time_backup_names, key=len)

        used_policy = 'time' if purge_backup_names == purge_time_backup_names \
            else 'count'
        logging.info('Purging using "%s" retain policy: %s', used_policy,
                     purge_backup_names)

        purged_backups = []
        for backup_name in purge_backup_names:
            logging.debug('Purging backup: %s', backup_name)
            name, _ = self.delete(backup_name)
            if name:
                purged_backups.append(backup_name)

            self._reload_existing_backup(backup_name)

        return '\n'.join(purged_backups)

    def _backup_database_meta(self, db_name: str) -> str:
        """
        Backup database sql
        """
        db_sql_abs_path = self._ch_ctl.get_db_sql_abs_path(db_name)
        logging.debug('Making database "%s" sql backup: %s', db_name,
                      db_sql_abs_path)

        with open(db_sql_abs_path) as file_fd:
            file_contents = file_fd.read()
        metadata = file_contents.replace('ATTACH ', 'CREATE ', 1)
        return self._backup_layout.save_database_meta(db_name, metadata)

    def _backup_table_meta(self, db_name: str, table_name: str) -> str:
        """
        Backup table schema (CREATE TABLE sql) and return path to saved data
        on remote storage.
        """
        logging.debug('Making table schema backup for "%s"."%s"', db_name,
                      table_name)

        schema = self._ch_ctl.get_table_schema(db_name, table_name)

        remote_path = self._backup_layout.save_table_meta(
            db_name, table_name, schema)
        return remote_path

    def _restore_database_schema(self, db_name: str,
                                 backup_meta: BackupMetadata) -> None:
        """
        Restore database schema
        """
        logging.debug('Running database schema restore: %s', db_name)

        # restore db sql
        db_sql = self._backup_layout.download_str(
            backup_meta.get_db_sql_path(db_name))
        self._ch_ctl.restore_meta(db_sql)

        # restore table sql
        for table_sql_path in backup_meta.get_tables_sql_paths(db_name):
            table_sql = self._backup_layout.download_str(table_sql_path)
            self._ch_ctl.restore_meta(table_sql)

    def _restore_database_data(self, db_name: str,
                               backup_meta: BackupMetadata) -> None:
        """
        Restore database data
        """
        # restore table data (download and attach parts)
        for table_name in backup_meta.get_tables(db_name):
            logging.debug('Running table "%s.%s" data restore', db_name,
                          table_name)

            attach_parts = []
            for part in backup_meta.get_parts(db_name, table_name):
                logging.debug('Fetching "%s.%s" part: %s', db_name, table_name,
                              part.name)

                self._backup_layout.download_part_data(db_name, table_name,
                                                       part.name, part.paths)
                attach_parts.append(part.name)

            logging.debug('Waiting for downloads')
            self._backup_layout.wait()
            self._ch_ctl.chown_dettached_table_parts(db_name, table_name)
            for part_name in attach_parts:
                logging.debug('Attaching "%s.%s" part: %s', db_name,
                              table_name, part_name)

                self._ch_ctl.attach_part(db_name, table_name, part_name)

    def _deduplicate_part(self, fpart: FreezedPart) \
            -> Tuple[Optional[str], Optional[PartMetadata]]:
        """
        Deduplicate part if it's possible
        """
        logging.debug('Looking for deduplication of part "%s"', fpart.name)

        for backup_meta in self._existing_backups:
            existing_part = backup_meta.get_part(fpart.database, fpart.table,
                                                 fpart.name)

            if not existing_part:
                logging.debug('Part "%s" was not found in backup "%s", skip',
                              fpart.name, backup_meta.name)
                continue

            if existing_part.link:
                logging.debug('Part "%s" in backup "%s" is link, skip',
                              fpart.name, backup_meta.name)
                continue

            if existing_part.checksum != fpart.checksum:
                logging.debug(
                    'Part "%s" in backup "%s" has mismatched checksum, skip',
                    fpart.name, backup_meta.name)
                continue

            #  check if part files exist in storage
            if self._check_part_availability(existing_part):
                logging.info('Deduplicating part "%s" based on %s', fpart.name,
                             backup_meta.name)
                return backup_meta.path, existing_part

        return None, None

    def _check_part_availability(self, part: PartMetadata) -> bool:
        """
        Check if part files exist in storage
        """
        failed_part_files = [
            path for path in part.paths
            if not self._backup_layout.path_exists(path)
        ]

        if failed_part_files:
            logging.error('Some part files were not found in storage: %s',
                          ', '.join(failed_part_files))
            return False

        return True

    def _get_existing_backup_names(self) -> List[str]:
        return self._backup_layout.get_existing_backups_names()

    def _get_backup_meta(self, backup_name: str) -> BackupMetadata:
        return self._backup_layout.get_backup_meta(backup_name)

    def _get_last_backup(self) -> Optional[BackupMetadata]:
        """
        Return the last valid backup.
        """
        backups = self._get_existing_backup_names()
        for backup in sorted(backups, reverse=True):
            try:
                backup_meta = self._get_backup_meta(backup)
                if backup_meta.state == BackupState.CREATED:
                    return backup_meta
            except Exception:
                logging.warning(
                    'Failed to load metadata for backup %s',
                    backup,
                    exc_info=True)
        return None

    def _load_existing_backups(self,
                               backup_age_limit: datetime = None,
                               load_all=True) -> None:
        """
        Load all current backup entries
        """
        if backup_age_limit is None:
            backup_age_limit = utc_fromtimestamp(0)

        logging.debug('Collecting existing backups')

        existing_backups = []
        for backup_name in self._get_existing_backup_names():
            try:
                backup_meta = self._get_backup_meta(backup_name)

                if not load_all:
                    if backup_meta.state != BackupState.CREATED:
                        logging.debug(
                            'Backup "%s" is skipped due to state "%s"',
                            backup_name, backup_meta.state)
                        continue

                    if not backup_meta.end_time:
                        logging.debug(
                            'Backup "%s" has no end timestamp, skipping')
                        continue

                    if backup_meta.end_time <= backup_age_limit:
                        logging.debug(
                            'Backup "%s" is too old for given timelimit'
                            ' (%s > %s), skipping', backup_name,
                            backup_meta.end_time, backup_age_limit)
                        continue

                existing_backups.append(backup_meta)

            except Exception:
                logging.warning(
                    'Failed to load metadata for backup %s',
                    backup_name,
                    exc_info=True)

        # Sort by time (new is first)
        # we want to duplicate part based on freshest backup
        existing_backups.sort(key=lambda b: b.start_time, reverse=True)
        self._existing_backups = existing_backups

    def _reload_existing_backup(self, backup_name: str) -> None:
        """
        Reload once loaded backup entry
        """
        # considering previous selection in self._load_existing_backups()

        backup_meta = None
        for i, b in enumerate(self._existing_backups):
            if b.name == backup_name:
                try:
                    backup_meta = self._get_backup_meta(backup_name)
                    self._existing_backups[i] = backup_meta
                except StorageError:
                    self._existing_backups.pop(i)
                    return
                break

        if not backup_meta:
            raise ClickhouseBackupError(
                'Backup "{0}" was not loaded before'.format(backup_name))

    def _check_min_interval(self, last_backup: BackupMetadata,
                            force: bool) -> bool:
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

    @staticmethod
    def _pop_deleting_paths(
            backup_meta: BackupMetadata,
            newer_backups: Sequence[BackupMetadata]) -> Tuple[bool, List[str]]:
        """
        Get backup paths which are safe for delete
        """
        skip_parts = {}
        for new_backup in newer_backups:
            for part in new_backup.get_parts():
                if not part.link:
                    continue

                if not part.link.endswith(backup_meta.name):
                    continue

                part_id = (part.database, part.table, part.name)
                skip_parts[part_id] = new_backup.name

        is_changed = False
        delete_paths = []  # type: List[str]
        for part in backup_meta.get_parts():
            part_id = (part.database, part.table, part.name)

            logging.debug('Working on part "%s.%s.%s"', *part_id)
            if part_id in skip_parts:
                logging.debug(
                    'Skip removing part "%s": link from "%s"'
                    ' was found', part.name, skip_parts[part_id])
                continue

            logging.debug('Dropping part contents from meta "%s"', part.name)

            if not part.link:
                logging.debug('Scheduling deletion of part files "%s"',
                              part.name)
                delete_paths.extend(part.paths)

            backup_meta.remove_part(part)
            is_changed = True

        return is_changed, delete_paths
