"""
Clickhouse backup layout structures
"""

import copy
import json
import logging
import os
import socket
from collections import defaultdict
from datetime import datetime

from ch_backup.exceptions import InvalidBackupStruct, StorageError
from ch_backup.storage import StorageLoader

CBS_DEFAULT_PNAME_FMT = '%Y%m%dT%H%M%S'
CBS_DEFAULT_DATE_FMT = '%Y-%m-%d %H:%M:%S'
CBS_DEFAULT_FNAME = 'backup_struct.json'
CBS_DEFAULT_JSON_INDENT = 4


class ClickhouseBackupLayout:
    # pylint: disable=too-many-instance-attributes
    """
    Storage layout and transfer
    """

    def __init__(self, ch_ctl, config, storage_loader=None):
        self._storage_loader = storage_loader or StorageLoader(config)
        self._config = config['backup']
        self._ch_ctl = ch_ctl

        self._backup_name_fmt = CBS_DEFAULT_PNAME_FMT
        self._backup_name = datetime.now().strftime(self._backup_name_fmt)
        self._backup_meta_fname = CBS_DEFAULT_FNAME

        self.backup_path = self._get_backup_path(self._backup_name)
        self.backup_meta_path = os.path.join(self.backup_path,
                                             self._backup_meta_fname)

    @property
    def backup_name(self):
        """
        Backup name getter
        """
        return self._backup_name

    @backup_name.setter
    def backup_name(self, value):
        self._backup_name = value
        self.backup_path = self._get_backup_path(value)
        self.backup_meta_path = os.path.join(self.backup_path,
                                             self._backup_meta_fname)

    def _get_backup_path(self, backup_name):
        """
        Returns storage backup path
        """
        return os.path.join(self._config['path_root'], backup_name)

    def get_backup_meta_path(self, backup_name):
        """
        Returns backup meta path
        """
        return os.path.join(self._config['path_root'], backup_name,
                            self._backup_meta_fname)

    def save_table_meta(self, db_name, table_name, metadata):
        """
        Backup table meta (sql-file)
        """
        table_sql_rel_path = self._ch_ctl.get_table_sql_rel_path(
            db_name, table_name)
        remote_path = os.path.join(self.backup_path, table_sql_rel_path)
        try:

            future_id = self._storage_loader.upload_data(
                metadata,
                remote_path=remote_path,
                is_async=True,
                encryption=True)

            logging.debug('Saving table sql-file "%s": %s', table_sql_rel_path,
                          future_id)
            return remote_path
        except Exception as exc:
            logging.critical('Unable create async upload %s: %s',
                             table_sql_rel_path, exc)
            raise StorageError

    def save_database_meta(self, db_name, metadata):
        """
        Backup database meta (sql-file)
        """
        db_sql_rel_path = self._ch_ctl.get_db_sql_rel_path(db_name)
        remote_path = os.path.join(self.backup_path, db_sql_rel_path)
        try:
            logging.debug('Saving database sql-file "%s": %s', db_sql_rel_path,
                          self.backup_meta_path)
            self._storage_loader.upload_data(
                metadata, remote_path=remote_path, encryption=True)
            return remote_path
        except Exception as exc:
            logging.critical(
                'Unable to upload database "%s" sql-file to storage: %s',
                db_name, exc)
            raise StorageError

    def save_backup_meta(self, backup_meta):
        """
        Upload backup meta file into storage
        """
        remote_path = self.backup_meta_path
        try:
            logging.debug('Saving backup meta in key: %s', remote_path)
            result = self._storage_loader.upload_data(
                backup_meta, remote_path=remote_path)
            return result
        except Exception as exc:
            logging.critical('Unable to upload backup metadata to storage: %s',
                             exc)
            raise StorageError

    def save_part_data(self, db_name, table_name, part_name):
        """
        Backup part files and return storage paths
        """
        local_dir_path = self._ch_ctl.get_shadow_part_abs_path(
            db_name, table_name, part_name)
        remote_dir_path = os.path.join(self.backup_path, 'data', db_name,
                                       table_name, part_name)

        uploaded_files = []
        part_files = [
            f for f in os.listdir(local_dir_path)
            if os.path.isfile(os.path.join(local_dir_path, f))
        ]

        for fname in part_files:
            local_fname = os.path.join(local_dir_path, fname)
            remote_fname = os.path.join(remote_dir_path, fname)
            try:
                self._storage_loader.upload_file(
                    local_path=local_fname,
                    remote_path=remote_fname,
                    is_async=True,
                    encryption=True)
                uploaded_files.append(remote_fname)

            except Exception as exc:
                logging.critical('Unable to upload partition: %s', exc)
                raise StorageError

        return uploaded_files

    def get_backup_meta(self):
        """
        Download backup meta file from storage
        """
        remote_path = self.backup_meta_path
        try:
            result = self._storage_loader.download_data(remote_path)
            return result
        except Exception as exc:
            logging.critical(
                'Unable to download backup metadata "%s" from storage: %s',
                remote_path, exc)
            raise StorageError

    def download_str(self, remote_path):
        """
        Downloads data and tries to decode
        """
        return self._storage_loader.download_data(remote_path, encryption=True)

    def download_backup_meta(self, remote_path):
        """
        Downloads backup metadata
        """
        return self._storage_loader.download_data(remote_path)

    def download_part_data(self, db_name, table_name, part_name, part_files):
        """
        Restore part from detached directory
        """
        logging.debug('Downloading part %s in %s.%s', part_name, db_name,
                      table_name)

        fs_part_path = self._ch_ctl.get_detached_part_abs_path(
            db_name, table_name, part_name)

        if not os.path.exists(fs_part_path):
            os.makedirs(fs_part_path, exist_ok=True)

        downloaded_files = []
        for remote_path in part_files:
            file_name = os.path.basename(remote_path)
            local_path = os.path.join(fs_part_path, file_name)
            try:
                logging.debug('Downloading part file %s: %s', local_path,
                              remote_path)
                self._storage_loader.download_file(
                    remote_path=remote_path,
                    local_path=local_path,
                    is_async=True,
                    encryption=True)
                downloaded_files.append((
                    remote_path,
                    local_path,
                ))
            except Exception as exc:
                logging.critical('Unable to download part file %s: %s',
                                 remote_path, exc)
                raise StorageError

        return downloaded_files

    def get_existing_backups_names(self):
        """
        Get current backup entries
        """
        return self._storage_loader.list_dir(self._config['path_root'])

    def path_exists(self, remote_path):
        """
        Check whether storage path exists or not.
        """
        return self._storage_loader.path_exists(remote_path)

    def wait(self):
        """
        Wait for async jobs
        """
        try:
            logging.debug('Collecting async jobs')
            self._storage_loader.wait()
        except Exception as exc:
            logging.critical('Errors in async transfers: %s', exc)
            raise StorageError


class ClickhouseBackupStructure:
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
        self.path = None

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
        })

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


class ClickhousePartInfo:
    """
    Clickhouse part metadata and few helpers
    """

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
