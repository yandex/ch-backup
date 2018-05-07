"""
Clickhouse backup logic
"""

import logging
from collections import defaultdict
from datetime import datetime, timedelta
from functools import lru_cache

from ch_backup.clickhouse.control import ClickhouseCTL
from ch_backup.clickhouse.layout import (
    ClickhouseBackupLayout, ClickhouseBackupStructure, ClickhousePartInfo)
from ch_backup.exceptions import ClickHouseBackupError
from ch_backup.util import utcnow


class ClickhouseBackup:
    """
    Clickhouse backup logic
    """

    def __init__(self, config, ch_ctl=None, backup_layout=None):
        self._ch_ctl = ch_ctl or ClickhouseCTL(config['clickhouse'])
        self._backup_layout = backup_layout or \
            ClickhouseBackupLayout(config, ch_ctl=self._ch_ctl)
        self._config = config['backup']
        self._existing_backups = []
        self._dedup_time = None

    def get(self, backup_name):
        """
        Get backup information.
        """
        return self._get_backup_meta(backup_name)

    def list(self):
        """
        Get list of existing backup names.
        """
        return self._get_existing_backup_names()

    def backup(self, databases=None, tables=None, force=False):
        """
        Perform backup.

        If force is True, backup.min_interval config option is ignored.
        """
        assert not (databases and tables)

        db_tables = defaultdict(list)
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

        min_interval = self._config.get('min_interval')
        if min_interval and not force:
            last_backup = self._get_last_backup()
            if (last_backup and utcnow() - last_backup.end_time <
                    timedelta(**min_interval)):
                msg = 'Backup is skipped per backup.min_interval config option'
                logging.info(msg)
                return (last_backup.name, msg)

        backup_meta = ClickhouseBackupStructure(
            name=self._backup_layout.backup_name,
            path=self._backup_layout.backup_path,
            ch_version=self._ch_ctl.get_version())

        backup_meta.mark_start()

        logging.debug('Starting backup "%s" for databases: %s',
                      backup_meta.name, ', '.join(databases))

        for db_name in databases:
            self.backup_database(db_name, backup_meta, db_tables[db_name])
        self._ch_ctl.remove_shadow_data()

        backup_meta.mark_end()
        self._backup_layout.save_backup_meta(backup_meta)

        return (backup_meta.name, None)

    def restore(self, backup_name, databases=None, schema_only=False):
        """
        Restore specified backup
        """
        self._backup_layout.backup_name = backup_name
        backup_meta = self._backup_layout.get_backup_meta()

        if databases is None:
            databases = backup_meta.get_databases()
        else:
            # check all required databases exists in backup meta
            missed_databases = (db_name for db_name in databases
                                if db_name not in backup_meta.get_databases())
            if missed_databases:
                logging.critical(
                    'Required databases %s were not found in backup meta: %s',
                    ', '.join(missed_databases), backup_meta.path)
                raise ClickHouseBackupError(
                    'Required databases were not found in backup struct')

        for db_name in databases:
            self._restore_database_schema(db_name, backup_meta)
            if schema_only:
                logging.debug(
                    'Don\'t restore %s data, cause schema_only is set %r',
                    db_name, schema_only)
            else:
                self._restore_database_data(db_name, backup_meta)

    def backup_database(self, db_name, backup_meta, tables=None):
        """
        Backup database
        """
        backup_meta.add_database(db_name)

        logging.debug('Running database backup: %s', db_name)

        # get db objects ordered by mtime
        tables = self._ch_ctl.get_tables_ordered(db_name, tables)
        for table_name in tables:
            logging.debug('Running table "%s.%s" backup', db_name, table_name)

            # save table sql
            backup_meta.add_table_sql_path(db_name, table_name,
                                           self._backup_table_meta(
                                               db_name, table_name))

            parts_rows = self._ch_ctl.get_all_table_parts_info(
                db_name, table_name)

            # remove previous data from shadow path
            self._ch_ctl.remove_shadow_data()

            # freeze table parts
            try:
                self._ch_ctl.freeze_table(db_name, table_name)
            except Exception as exc:
                logging.critical('Unable to freeze: %s', exc)
                raise ClickHouseBackupError

            for part_row in parts_rows:
                part_info = ClickhousePartInfo(meta=part_row)
                logging.debug('Working on part %s: %s', part_info.name,
                              part_info)

                # calculate backup total rows and bytes count
                backup_meta.rows += int(part_info.rows)
                backup_meta.bytes += int(part_info.bytes)
                # TODO: save backup total and real (exclude deduplicated)

                # trying to find part in storage
                link, part_remote_paths = self._deduplicate_part(part_info)

                if not link:
                    # preform backup if deduplication is not available
                    logging.debug('Starting backup for "%s.%s" part: %s',
                                  db_name, table_name, part_info.name)

                    part_remote_paths = self._backup_layout.save_part_data(
                        db_name, table_name, part_info.name)

                # save part files and meta in backup struct
                backup_meta.add_part_contents(
                    db_name,
                    table_name,
                    part_info.name,
                    part_remote_paths,
                    part_info.get_contents(),
                    link=link)

            logging.debug('Waiting for uploads')
            self._backup_layout.wait()

        # save database sql
        backup_meta.set_db_sql_path(db_name,
                                    self._backup_database_meta(db_name))

    def _backup_database_meta(self, db_name):
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

    def _backup_table_meta(self, db_name, table_name):
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

    def _restore_database_schema(self, db_name, backup_meta):
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

    def _restore_database_data(self, db_name, backup_meta):
        """
        Restore database data
        """

        # restore table data (download and attach parts)
        for table_name in backup_meta.get_tables(db_name):
            logging.debug('Running table "%s.%s" data restore', db_name,
                          table_name)

            attach_parts = []
            for part_name in backup_meta.get_parts(db_name, table_name):
                logging.debug('Fetching "%s.%s" part: %s', db_name, table_name,
                              part_name)

                part_paths = backup_meta.get_part_paths(
                    db_name, table_name, part_name)

                self._backup_layout.download_part_data(db_name, table_name,
                                                       part_name, part_paths)
                attach_parts.append(part_name)

            logging.debug('Waiting for downloads')
            self._backup_layout.wait()
            self._ch_ctl.chown_dettached_table_parts(db_name, table_name)
            for part_name in attach_parts:
                logging.debug('Attaching "%s.%s" part: %s', db_name,
                              table_name, part_name)

                self._ch_ctl.attach_part(db_name, table_name, part_name)

    def _deduplicate_part(self, part_info):
        """
        Deduplicate part if it's possible
        """
        logging.debug('Looking for deduplication of part "%s"', part_info.name)

        for backup_meta in self._existing_backups:
            # load every existing backup entry
            backup_part_contents = backup_meta.get_part_contents(
                part_info.database, part_info.table, part_info.name)

            if not backup_part_contents:
                logging.debug('Part "%s" was not found in backup "%s", skip',
                              part_info.name, backup_meta.name)
                continue

            backup_part_info = ClickhousePartInfo(**backup_part_contents)

            if backup_part_info.link:
                logging.debug('Part "%s" in backup "%s" is link, skip',
                              part_info.name, backup_meta.name)
                continue

            if backup_part_info != part_info:
                logging.debug('Part "%s" in backup "%s" is differ form local',
                              part_info.name, backup_meta.name)
                continue

            #  check if part files exist in storage
            if self._check_part_availability(backup_part_info):
                logging.info('Deduplicating part "%s" based on %s',
                             part_info.name, backup_meta.name)
                return backup_meta.path, backup_part_info.paths

        return False, None

    def _check_part_availability(self, part_info):
        """
        Check if part files exist in storage
        """
        failed_part_files = [
            path for path in part_info.paths
            if not self._backup_layout.path_exists(path)
        ]

        if failed_part_files:
            logging.error('Some part files were not found in storage: %s',
                          ', '.join(failed_part_files))
            return False

        return True

    @lru_cache()
    def _get_existing_backup_names(self):
        return self._backup_layout.get_existing_backups_names()

    @lru_cache()
    def _get_backup_meta(self, backup_name):
        return self._backup_layout.get_backup_meta(backup_name)

    def _get_last_backup(self):
        """
        Return the last valid backup.
        """
        backups = self._get_existing_backup_names()
        for backup in sorted(backups, reverse=True):
            try:
                return self._get_backup_meta(backup)
            except Exception:
                logging.warning(
                    'Failed to load metadata for backup %s',
                    backup,
                    exc_info=True)
        return None

    def _load_existing_backups(self, backup_age_limit=None):
        """
        Load all current backup entries
        """
        if backup_age_limit is None:
            backup_age_limit = datetime.fromtimestamp(0)

        logging.debug('Collecting existing backups for deduplication')

        existing_backups = []
        for backup_name in self._get_existing_backup_names():
            try:
                backup_meta = self._get_backup_meta(backup_name)

                # filter old entries (see deduplication_age_limit)
                if backup_meta.end_time > backup_age_limit:
                    existing_backups.append(backup_meta)
                else:
                    logging.debug(
                        'Backup "%s" is too old for deduplication (%s > %s),'
                        ' skipping', backup_name, backup_meta.end_time,
                        backup_age_limit)
            except Exception:
                logging.warning(
                    'Failed to load metadata for backup %s',
                    backup_name,
                    exc_info=True)

        # Sort by time (new is first)
        # we want to duplicate part based on freshest backup
        existing_backups.sort(key=lambda b: b.end_time, reverse=True)
        self._existing_backups = existing_backups
