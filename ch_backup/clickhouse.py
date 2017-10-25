#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Clickhouse classes: backup meta-file structure,
                    simple clickhouse client,
                    clickhouse backup logic
"""

import copy
import json
import logging
import os
import shutil
import socket
from collections import defaultdict
from datetime import datetime, timedelta

import requests
from ch_backup.exceptions import (ClickHouseBackupError, InvalidBackupStruct,
                                  StorageError)
from ch_backup.util import chown_dir_contents, strip_query

GET_ALL_DB_TABLES_ORDERED_SQL = strip_query("""
    SELECT name
    FROM system.tables
    WHERE engine like '%MergeTree%' and database = '{db_name}'
    ORDER BY metadata_modification_time
    FORMAT JSON
""")

PART_ATTACH_SQL = strip_query("""
    ALTER TABLE {db_name}.{table_name}
    ATTACH PART '{part_name}'
""")

TABLE_FREEZE_SQL = strip_query("""
    ALTER TABLE {db_name}.{table_name}
    FREEZE PARTITION ''
""")

SHOW_DATABASES_SQL = strip_query("""
    SHOW DATABASES
    FORMAT JSON
""")

SHOW_TABLES_SQL = strip_query("""
    SHOW TABLES
    FROM {db_name}
    FORMAT JSON
""")

GET_ALL_TABLE_PARTS_INFO_SQL = strip_query("""
    SELECT *
    FROM system.parts
    WHERE active AND database == '{db_name}'
    AND table == '{table_name}'
    FORMAT JSON;
""")

CBS_DEFAULT_PNAME_FMT = '%Y%m%dT%H%M%S'
CBS_DEFAULT_DATE_FMT = '%Y-%m-%d %H:%M:%S'
CBS_DEFAULT_FNAME = 'backup_struct.json'
CBS_DEFAULT_JSON_INDENT = 4


class ClickhouseBackupStructure(object):
    """
    Clickhouse backup meta
    """

    def __init__(self, date_fmt=None, hostname=None):
        if hostname is None:
            hostname = socket.getfqdn()

        if date_fmt is None:
            date_fmt = CBS_DEFAULT_DATE_FMT

        self._meta = {
            'hostname': hostname,
            'rows': 0,
            'bytes': 0,
            'date_fmt': date_fmt,
        }

        self._databases = {}

        self.name = None

    def add_database(self, db_name):
        """
        Add database dict to backup struct
        """
        self._databases[db_name] = {
            'db_sql_path': None,
            'tables_sql_paths': [],
            'parts_paths': defaultdict(dict),
        }

    @property
    def start_time(self):
        """
        Mark backup as started
        """
        return datetime.strptime(self._meta['start_time'],
                                 self._meta['date_fmt'])

    @property
    def end_time(self):
        """
        Mark backup as finished
        """
        return datetime.strptime(self._meta['end_time'],
                                 self._meta['date_fmt'])

    def __setattr__(self, key, value):
        if key in ('_meta', '_databases'):
            return super().__setattr__(key, value)

        self._meta[key] = value

    def __getattr__(self, item):
        if item in ('_meta', '_databases'):
            return super().__getattribute__(item)
        try:
            return self._meta[item]
        except KeyError:
            raise AttributeError

    def __str__(self):
        return self.dump_json()

    def get_databases(self):
        """
        Get databases meta
        """
        return self._databases

    def mark_start(self):
        """
        Set start datetime
        """
        self._meta['start_time'] = datetime.now().strftime(
            self._meta['date_fmt'])

    def mark_end(self):
        """
        Set end datetime
        """
        self._meta['end_time'] = datetime.now().strftime(
            self._meta['date_fmt'])

    def dump_json(self):
        """
        Dump struct to json data
        """
        report = {}
        report.update({'databases': self._databases})
        report.update({'meta': self._meta})
        return json.dumps(report, indent=CBS_DEFAULT_JSON_INDENT)

    def load_json(self, data):
        """
        Load struct from json data
        """
        try:
            loaded = json.loads(data)
            self._databases = loaded['databases']
            self._meta = loaded['meta']
        except (ValueError, KeyError):
            raise InvalidBackupStruct

    def get_db_sql_path(self, db_name):
        """
        Get database sql path
        """
        return self._databases[db_name]['db_sql_path']

    def set_db_sql_path(self, db_name, path):
        """
        Set database sql path
        """
        self._databases[db_name]['db_sql_path'] = path

    def get_tables(self, db_name):
        """
        Get tables for specified database
        """
        return (table_name
                for table_name in self._databases[db_name]['parts_paths'])

    def get_tables_sql_paths(self, db_name):
        """
        Get tables sql paths
        """
        return (
            sql_path
            for _, sql_path in self._databases[db_name]['tables_sql_paths'])

    def add_table_sql_path(self, db_name, table_name, path):
        """
        Set storage path of table ddl

        path is list, order matters
        """
        self._databases[db_name]['tables_sql_paths'].append((table_name, path))

    # pylint: disable=too-many-arguments
    def add_part_contents(self,
                          db_name,
                          table_name,
                          part_name,
                          paths,
                          meta,
                          link=False):
        """
        Add part backup contents to backup struct
        """
        self._databases[db_name]['parts_paths'][table_name].update({
            part_name: {
                'link': link,
                'paths': paths,
                'meta': meta,
            },
        }, )

    def get_part_contents(self, db_name, table_name, part_name):
        """
        Get part backup contents from backup struct
        """
        try:
            return self._databases[db_name]['parts_paths'][table_name][
                part_name]
        except KeyError:
            return None

    def get_part_paths(self, db_name, table_name, part_name):
        """
        Get storage file paths of specified part
        """
        return self._databases[db_name]['parts_paths'][table_name][part_name][
            'paths']

    def get_parts(self, db_name, table_name):
        """
        Get all parts of specified database.table
        """
        return self._databases[db_name]['parts_paths'][table_name]


class ClickhousePartInfo(object):
    """
    Clickhouse part metadata and few helpers
    """

    # pylint: disable=too-few-public-methods
    def __init__(self, meta, link=None, paths=None):
        self._meta = meta
        if link is None:
            link = False
        self.link = link
        self.paths = paths

    def __getattr__(self, item):
        try:
            return self._meta[item]
        except KeyError:
            raise AttributeError

    def __eq__(self, other):
        criteria = ('modification_time', 'rows')
        for check_attr in criteria:
            if getattr(self, check_attr) != getattr(other, check_attr):
                return False
        return True

    def __str__(self):
        return str(self._meta)

    def get_contents(self):
        """
        Get part meta
        """
        return copy.deepcopy(self._meta)


class ClickhouseBackup(object):
    """
    Clickhouse backup logic
    """

    # pylint: disable=too-many-arguments,too-many-instance-attributes
    # TODO: add comments
    # TODO: mb split class

    def __init__(self,
                 config,
                 ch_ctl,
                 storage_loader,
                 path=None,
                 backup_meta_fname=None):
        self._config = config
        self._ch_ctl = ch_ctl
        self._storage_loader = storage_loader
        self._existing_backups = []
        self._dedup_time = None

        if backup_meta_fname is None:
            backup_meta_fname = CBS_DEFAULT_FNAME
        self._backup_meta_fname = backup_meta_fname

        self._backup_name_fmt = CBS_DEFAULT_PNAME_FMT
        self._storage_prefix = None
        self._cmd_path = path

    @property
    def commands(self):
        """
        Exported commands to cli
        """

        return {
            'backup': self.backup,
            'restore': self.restore,
            'list': self.list,
            'show': self.show,
        }

    def show(self, **kwargs):  # pylint: disable=unused-argument
        """
        Show backup meta struct
        """

        backup_name = self._cmd_path
        backup_meta_path = self._get_backup_meta_path(backup_name)
        backup_meta = self._load_backup_meta(backup_meta_path)
        return backup_meta

    def list(self, **kwargs):  # pylint: disable=unused-argument
        """
        List backup entries
        """

        return '\n'.join(
            sorted(self._get_existing_backups_names(), reverse=True))

    def backup(self, **kwargs):
        """
        Start backup
        """

        if kwargs['databases'] is None:
            databases = self._ch_ctl.get_all_databases(
                self._config['exclude_dbs'])
        else:
            databases = kwargs['databases'].split(',')

        # load existing backups if deduplication is enabled
        if self._config.get('deduplicate_parts'):
            backup_age_limit = datetime.now() - timedelta(
                **self._config['deduplication_age_limit'])

            self._load_existing_backups(backup_age_limit)

        backup_meta = ClickhouseBackupStructure()
        backup_meta.name = datetime.now().strftime(self._backup_name_fmt)
        backup_meta.mark_start()
        logging.debug('Starting backup "%s" for databases "%s"',
                      backup_meta.name, ', '.join(databases))

        self._storage_loader.path_prefix = self._get_backup_path(
            backup_meta.name)

        for db_name in databases:
            # run backup per db
            self.backup_database(db_name, backup_meta)
        self._ch_ctl.remove_shadow_data()

        backup_meta.mark_end()
        backup_meta_json = backup_meta.dump_json()
        logging.debug('Resultant backup meta:\n%s', backup_meta_json)
        self._save_backup_meta(backup_meta)

        return backup_meta.name

    def restore(self, **kwargs):
        """
        Restore specified backup
        """
        backup_name = self._cmd_path
        backup_meta = ClickhouseBackupStructure()
        backup_meta.load_json(self._get_backup_contents(backup_name))

        if kwargs['databases'] is None:
            databases = backup_meta.get_databases()
        else:
            databases = kwargs['databases'].split(',')

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
            self.restore_database(db_name, backup_meta)

        return backup_name

    def _get_backup_path(self, backup_name):
        """
        Get storage backup path
        """
        return os.path.join(self._config['path_root'], backup_name)

    def _get_backup_meta_path(self, backup_name):
        """
        Get storage backup meta path
        """
        return os.path.join(
            self._get_backup_path(backup_name), self._backup_meta_fname)

    def backup_database(self, db_name, backup_meta):
        """
        Backup database
        """
        backup_meta.add_database(db_name)

        # get db objects ordered by mtime
        tables = self._ch_ctl.get_all_db_tables_ordered(db_name)
        for table_name in tables:
            # save table sql
            backup_meta.add_table_sql_path(db_name, table_name,
                                           self.backup_table_meta(
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
                    part_remote_paths = self.backup_part(part_info)

                # save part files and meta in backup struct
                backup_meta.add_part_contents(
                    db_name,
                    table_name,
                    part_info.name,
                    part_remote_paths,
                    part_info.get_contents(),
                    link=link)

        # save database sql
        backup_meta.set_db_sql_path(db_name,
                                    self.backup_database_meta(db_name))

    def backup_part(self, part_info):
        """
        Backup part and return storage paths
        """
        logging.debug('Making backup of part "%s"', part_info.name)

        part_path = self._ch_ctl.get_shadow_part_abs_path(
            part_info.database, part_info.table, part_info.name)

        path_prefix = os.path.join(self._storage_loader.path_prefix, 'data',
                                   part_info.database, part_info.table,
                                   part_info.name)
        try:
            return self._storage_loader.upload_dir(
                part_path, path_prefix=path_prefix)
        except Exception as exc:
            logging.critical('Unable to upload partition: %s', exc)
            raise StorageError

    def backup_database_meta(self, db_name):
        """
        Backup database sql
        """
        db_sql_abs_path = self._ch_ctl.get_db_sql_abs_path(db_name)
        logging.debug('Making database "%s" sql backup: %s', db_name,
                      db_sql_abs_path)

        with open(db_sql_abs_path) as file_fd:
            file_contents = file_fd.read()
        metadata = file_contents.replace('ATTACH ', 'CREATE ', 1)

        db_sql_rel_path = self._ch_ctl.get_db_sql_rel_path(db_name)
        try:
            return self._storage_loader.upload_data(
                metadata, remote_path=db_sql_rel_path)
        except Exception as exc:
            logging.critical(
                'Unable to upload database %s metadata to storage: %s',
                db_name, exc)
            raise StorageError

    def backup_table_meta(self, db_name, table_name):
        """
        Backup table sql
        """
        table_sql_abs_path = self._ch_ctl.get_table_sql_abs_path(
            db_name, table_name)
        logging.debug('Making table "%s.%s" sql backup: %s', db_name,
                      table_name, table_sql_abs_path)

        with open(table_sql_abs_path) as file_fd:
            file_contents = file_fd.read()

        metadata = file_contents.replace(
            'ATTACH TABLE ',
            'CREATE TABLE {db_name}.'.format(db_name=db_name),
            1)

        table_sql_rel_path = self._ch_ctl.get_table_sql_rel_path(
            db_name, table_name)
        try:
            return self._storage_loader.upload_data(
                metadata, remote_path=table_sql_rel_path)
        except Exception as exc:
            logging.critical(
                'Unable to upload table %s.%s metadata to storage: %s',
                db_name, table_name, exc)
            raise StorageError

    def _save_backup_meta(self, backup_meta):
        """
        Upload backup meta file into storage
        """
        backup_meta.path = self._get_backup_meta_path(backup_meta.name)
        logging.debug('Saving backup meta in key: %s', backup_meta.path)

        backup_meta_json = backup_meta.dump_json()
        try:
            return self._storage_loader.upload_data(
                backup_meta_json, remote_path=backup_meta.path, path_prefix='')
        except Exception as exc:
            logging.critical('Unable to upload backup metadata to storage: %s',
                             exc)
            raise StorageError

    def _get_backup_contents(self, backup_name):
        """
        Download backup meta file from storage
        """
        remote_path = self._get_backup_meta_path(backup_name)
        try:
            return self._storage_loader.download_data(
                remote_path, path_prefix='').decode('utf-8')
        except Exception as exc:
            logging.critical(
                'Unable to download backup metadata "%s" from storage: %s',
                remote_path, exc)
            raise StorageError

    def restore_part(self, db_name, table_name, part_name, part_files):
        """
        Restore part from detached directory
        """
        logging.debug('Restoring part %s in %s.%s', part_name, db_name,
                      table_name)

        fs_part_path = self._ch_ctl.get_detached_part_abs_path(
            db_name, table_name, part_name)

        if not os.path.exists(fs_part_path):
            os.makedirs(fs_part_path, exist_ok=True)

        for part_file in part_files:
            file_name = os.path.basename(part_file)
            fs_file_part_path = os.path.join(fs_part_path, file_name)
            try:
                logging.debug('Downloading part file %s to %s', part_name,
                              fs_file_part_path)
                self._storage_loader.download_file(part_file,
                                                   fs_file_part_path)
            except Exception as exc:
                logging.critical('Unable to download part file %s: %s',
                                 part_file, exc)
                raise StorageError

        # change user-group and attach part to clickhouse
        self._ch_ctl.chown_attach_part(db_name, table_name, part_name)

    def restore_database(self, db_name, backup_meta):
        """
        Restore database
        """

        # restore db sql
        try:
            db_sql = self._storage_loader.download_data(
                backup_meta.get_db_sql_path(db_name)).decode('utf-8')
        except Exception as exc:
            logging.critical('Unable to download db sql: %s', exc)
            raise StorageError

        self._ch_ctl.restore_meta(db_sql)

        # restore table sql
        for table_sql_path in backup_meta.get_tables_sql_paths(db_name):
            try:
                table_sql = self._storage_loader.download_data(
                    table_sql_path).decode('utf-8')
            except Exception as exc:
                logging.critical('Unable to download table sql: %s', exc)
                raise StorageError
            self._ch_ctl.restore_meta(table_sql)

        # restore table data (download and attach parts)
        for table_name in backup_meta.get_tables(db_name):
            for part_name in backup_meta.get_parts(db_name, table_name):
                part_paths = backup_meta.get_part_paths(
                    db_name, table_name, part_name)
                self.restore_part(db_name, table_name, part_name, part_paths)

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
            if not self._storage_loader.path_exists(path)
        ]
        if failed_part_files:
            logging.error('Some part files were not found in storage: %s',
                          ', '.join(failed_part_files))
            return False

        return True

    def _get_dedup_path(self):
        """
        Get path for parts deduplication

        Default: dedup_path = path_root
        """
        dedup_path = self._config.get('deduplicate_path', None)
        if not dedup_path:
            dedup_path = self._config['path_root']

        return dedup_path

    def _get_existing_backups_names(self):
        """
        Get current backup entries
        """
        dedup_path = self._get_dedup_path()
        return self._storage_loader.list_dir(dedup_path, abs_path=False)

    def _load_backup_meta(self, backup_meta_path):
        """
        Download from storage and load backup meta file
        """
        backup_meta_contents = self._storage_loader.download_data(
            backup_meta_path, path_prefix='').decode('utf-8')
        backup_meta = ClickhouseBackupStructure()
        try:
            backup_meta.load_json(backup_meta_contents)
        except InvalidBackupStruct:
            logging.critical('Can not load backup meta file: %s',
                             backup_meta_path)
            raise
        return backup_meta

    def _load_existing_backups(self, backup_age_limit=None):
        """
        Load all current backup entries
        """
        if backup_age_limit is None:
            backup_age_limit = datetime.fromtimestamp(0)

        logging.debug('Collecting existing backups for deduplication')
        backup_paths = self._get_existing_backups_names()

        existing_backups = []
        for backup_name in backup_paths:
            backup_meta_path = self._get_backup_meta_path(backup_name)
            if not self._storage_loader.path_exists(backup_meta_path):
                logging.warning('Backup path without meta file was found: %s',
                                backup_meta_path)
                continue

            backup_meta = self._load_backup_meta(backup_meta_path)

            # filter old entries (see deduplication_age_limit)
            if backup_meta.end_time > backup_age_limit:
                existing_backups.append(backup_meta)
            else:
                logging.debug(
                    'Backup "%s" is too old for deduplication (%s > %s), skip',
                    backup_meta_path, backup_meta.end_time, backup_age_limit)

        # Sort by time (new is first)
        # we want to duplicate part based on freshest backup
        existing_backups.sort(key=lambda b: b.end_time, reverse=True)
        self._existing_backups = existing_backups


class ClickhouseCTL(object):
    """
    Clickhouse control tool
    """

    def __init__(self, config):
        self._config = config
        self._ch_client = ClickhouseClient(config)

        self.data_path = config['data_path']
        self.metadata_path = self._get_metadata_abs_path(self.data_path)
        self.shadow_data_path = self._get_shadow_data_abs_path(self.data_path)
        self.shadow_data_path_inc = os.path.join(self.shadow_data_path, '1')

    def chown_attach_part(self, db_name, table_name, part_name):
        """
        Chown detached part files
        """
        part_path = self.get_detached_part_abs_path(db_name, table_name,
                                                    part_name)
        self.chown_dir_contents(part_path)
        self.attach_part(db_name, table_name, part_name)

    def attach_part(self, db_name, table_name, part_name):
        """
        Attach part to database.table from dettached dir
        """
        query_sql = PART_ATTACH_SQL\
            .format(db_name=db_name,
                    table_name=table_name,
                    part_name=part_name)

        logging.debug('Attaching partition: %s', query_sql)
        self._ch_client.query(query_sql)

    def chown_dir_contents(self, dir_path):
        """
        Chown directory contents to configured owner:group
        """
        if not dir_path.startswith(self._config['data_path']):
            raise ClickHouseBackupError(
                'Trying to chown directory outside clickhouse data path')
        chown_dir_contents(self._config['user'], self._config['group'],
                           dir_path)

    def freeze_table(self, db_name, table_name):
        """
        Freeze all partitions in specified database.table
        """
        query_sql = TABLE_FREEZE_SQL.format(
            db_name=db_name, table_name=table_name)
        logging.debug('Freezing partition: %s', query_sql)

        return self._ch_client.query(query_sql)

    def remove_shadow_data(self):
        """
        Recursively delete shadow data path
        """
        if not self.shadow_data_path.startswith(self._config['data_path']):
            raise ClickHouseBackupError(
                'Trying to drop directory outside clickhouse data path')

        logging.debug('Removing shadow data path: %s', self.shadow_data_path)
        shutil.rmtree(self.shadow_data_path, ignore_errors=True)

    def get_all_databases(self, exclude_dbs=None):
        """
        Get list of all databases
        """
        result = []
        ch_resp = self._ch_client.query(SHOW_DATABASES_SQL)
        if 'data' in ch_resp:
            result = [
                row['name'] for row in ch_resp['data']
                if row['name'] not in exclude_dbs
            ]
        return result

    def get_all_db_tables(self, db_name):
        """
        Get unordered list of all database tables
        """
        result = []
        query_sql = SHOW_TABLES_SQL.format(db_name=db_name)
        logging.debug('Fetching all %s tables: %s', db_name, query_sql)
        ch_resp = self._ch_client.query(query_sql)
        if 'data' in ch_resp:
            result = [row['name'] for row in ch_resp['data']]
        return result

    def get_all_db_tables_ordered(self, db_name):
        """
        Get ordered by mtime list of all database tables
        """
        result = []
        query_sql = GET_ALL_DB_TABLES_ORDERED_SQL.format(db_name=db_name)
        logging.debug('Fetching all %s tables ordered: %s', db_name, query_sql)
        ch_resp = self._ch_client.query(query_sql)
        if 'data' in ch_resp:
            result = [row['name'] for row in ch_resp['data']]
        return result

    def get_all_table_parts_info(self, db_name, table_name):
        """
        Get dict with all table parts
        """
        query_sql = GET_ALL_TABLE_PARTS_INFO_SQL.format(
            db_name=db_name, table_name=table_name)
        logging.debug('Fetching all %s table parts: %s', db_name, query_sql)

        return self._ch_client.query(query_sql)['data']

    def restore_meta(self, query_sql):
        """
        Restore database or table meta sql
        """
        logging.debug('Restoring meta sql: %s', query_sql)
        return self._ch_client.query(query_sql)

    def get_detached_part_abs_path(self, db_name, table_name, part_name):
        """
        Get filesystem absolute path of detached part
        """
        return os.path.join(self._config['data_path'], 'data', db_name,
                            table_name, 'detached', part_name)

    def get_db_sql_abs_path(self, db_name):
        """
        Get filesystem absolute path of database meta sql
        """
        return os.path.join(self.data_path, self.get_db_sql_rel_path(db_name))

    def get_table_sql_abs_path(self, db_name, table_name):
        """
        Get filesystem absolute path of database.table meta sql
        """
        return os.path.join(self.data_path,
                            self.get_table_sql_rel_path(db_name, table_name))

    def get_shadow_part_abs_path(self, db_name, table_name, part_name):
        """
        Get freezed part absolute path
        """
        return os.path.join(self.shadow_data_path_inc, 'data', db_name,
                            table_name, part_name)

    @staticmethod
    def get_db_sql_rel_path(db_name):
        """
        Get filesystem relative path of database meta sql
        """
        return os.path.join(
            'metadata', '{db_name}.sql'.format(db_name=db_name))

    @staticmethod
    def get_table_sql_rel_path(db_name, table_name):
        """
        Get filesystem relative path of database.table meta sql
        """
        return os.path.join(
            'metadata',
            '{db_name}'.format(db_name=db_name),
            '{table_name}.sql'.format(table_name=table_name))

    @staticmethod
    def _get_metadata_abs_path(data_path):
        """
        Get filesystem metadata dir abs path
        """
        return os.path.join(data_path, 'metadata')

    @staticmethod
    def _get_shadow_data_abs_path(data_path):
        """
        Get filesystem metadata dir abs path
        """
        return os.path.join(data_path, 'shadow')


class ClickhouseClient(object):  # pylint: disable=too-few-public-methods
    """
    Simple clickhouse client
    """

    def __init__(self, config):
        self._config = config
        self._timeout = int(config.get('timeout', 3))
        self._query_url = '{proto}://{host}:{port}/?query={query}'. \
            format(
                proto=config.get('proto', 'http'),
                host=config.get('host', 'localhost'),
                port=config.get('port', 8123),
                query='{query}')

    def query(self, query_str, post_data=None, timeout=None):
        """
        Perform query to configured clickhouse endpoint
        """
        if timeout is None:
            timeout = self._timeout

        query_url = self._query_url.format(query=query_str)
        logging.debug('Clickhouse request url: %s', query_url)
        http_response = requests.post(
            query_url, data=post_data, timeout=timeout)

        try:
            http_response.raise_for_status()
        except requests.HTTPError:
            logging.critical('Error while performing request: %s',
                             http_response.text)
            raise

        try:
            return http_response.json()
        except ValueError:
            return {}
