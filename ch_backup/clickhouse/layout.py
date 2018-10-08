"""
Clickhouse backup layout structures
"""

import copy
import json
import logging
import os
import socket
from collections import defaultdict
from datetime import datetime, timezone
from enum import Enum

from ch_backup.clickhouse.control import ClickhouseCTL
from ch_backup.exceptions import (InvalidBackupStruct, StorageError,
                                  UnknownBackupStateError)
from ch_backup.storage import StorageLoader
from ch_backup.util import now, utcnow

CBS_DEFAULT_PNAME_FMT = '%Y%m%dT%H%M%S'
CBS_DEFAULT_DATE_FMT = '%Y-%m-%d %H:%M:%S %z'
CBS_DEFAULT_FNAME = 'backup_struct.json'
CBS_DEFAULT_JSON_INDENT = 4


class ClickhouseBackupState(Enum):
    """
    Valid backup states
    """

    NOT_STARTED = 'not_started'
    CREATED = 'created'
    CREATING = 'creating'
    DELETING = 'deleting'
    PARTIALLY_DELETED = 'partially_deleted'
    FAILED = 'failed'


class ClickhouseBackupLayout:
    """
    Storage layout and transfer
    """

    def __init__(self, config, ch_ctl=None, storage_loader=None):
        self._storage_loader = storage_loader or StorageLoader(config)
        self._ch_ctl = ch_ctl or ClickhouseCTL(config['clickhouse'])
        self._config = config['backup']

        self._backup_name_fmt = CBS_DEFAULT_PNAME_FMT
        self._backup_name = utcnow().strftime(self._backup_name_fmt)
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

    def _get_backup_meta_path(self, backup_name):
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
        remote_path = self._get_backup_meta_path(backup_meta.name)
        try:
            json_dump = backup_meta.dump_json()
            logging.debug('Saving backup meta in key %s:\n%s', remote_path,
                          json_dump)
            result = self._storage_loader.upload_data(
                json_dump, remote_path=remote_path)
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

    def get_backup_meta(self, backup_name=None):
        """
        Download backup meta from storage
        """
        if backup_name is None:
            path = self.backup_meta_path
        else:
            path = self._get_backup_meta_path(backup_name)

        try:
            data = self._storage_loader.download_data(path)
            return ClickhouseBackupStructure.load_json(data)
        except Exception as exc:
            logging.critical(
                'Unable to download backup metadata "%s" from storage: %s',
                path, exc)
            raise StorageError

    def download_str(self, remote_path):
        """
        Downloads data and tries to decode
        """
        return self._storage_loader.download_data(remote_path, encryption=True)

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

    def delete_loaded_files(self, delete_files):
        """
        Delete files from backup storage
        """
        # TODO: use bulk delete
        deleted_files = []
        for remote_path in delete_files:
            try:
                logging.debug('Deleting file: %s', remote_path)
                self._storage_loader.delete_file(
                    remote_path=remote_path, is_async=True)
                deleted_files.append(remote_path)
            except Exception as exc:
                logging.critical('Unable to delete file %s: %s', remote_path,
                                 exc)
                raise StorageError

        return deleted_files

    def delete_backup_path(self, backup_name=None):
        """
        Delete files from backup storage
        """
        if backup_name is None:
            path = self.backup_path
        else:
            path = self._get_backup_path(backup_name)

        delete_files = self._storage_loader.list_dir(
            path, recursive=True, absolute=True)
        return self.delete_loaded_files(delete_files)

    def get_existing_backups_names(self):
        """
        Get current backup entries
        """
        return self._storage_loader.list_dir(
            self._config['path_root'], recursive=False, absolute=False)

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
            logging.critical(
                'Errors in async transfers: %s', exc, exc_info=True)
            raise StorageError


class ClickhouseBackupStructure:
    """
    Clickhouse backup meta
    """

    # pylint: disable=too-many-instance-attributes

    def __init__(self,
                 name,
                 path,
                 ch_version,
                 date_fmt=None,
                 hostname=None,
                 labels=None):
        self.name = name
        self.labels = labels
        self.path = path
        self.ch_version = ch_version
        self.hostname = hostname or socket.getfqdn()
        self.rows = 0
        self.bytes = 0
        self.real_rows = 0
        self.real_bytes = 0
        self._state = ClickhouseBackupState.NOT_STARTED
        self.date_fmt = date_fmt or CBS_DEFAULT_DATE_FMT
        self.start_time = None
        self.end_time = None

        self._databases = {}

    def add_database(self, db_name):
        """
        Add database dict to backup struct
        """
        self._databases[db_name] = {
            'db_sql_path': None,
            'tables_sql_paths': [],
            'parts_paths': defaultdict(dict),
        }

    def __str__(self):
        return self.dump_json()

    @property
    def state(self):
        """
        Backup state
        """
        return self._state

    @state.setter
    def state(self, value):
        if value not in ClickhouseBackupState:
            raise UnknownBackupStateError
        self._state = value

    def update_start_time(self):
        """
        Set start datetime
        """
        self.start_time = now()

    def update_end_time(self):
        """
        Set end datetime
        """
        self.end_time = now()

    def dump_json(self):
        """
        Dump struct to json data
        """
        report = {
            'databases': self._databases,
            'meta': {
                'name': self.name,
                'path': self.path,
                'ch_version': self.ch_version,
                'hostname': self.hostname,
                'date_fmt': self.date_fmt,
                'start_time': self._format_time(self.start_time),
                'end_time': self._format_time(self.end_time),
                'rows': self.rows,
                'bytes': self.bytes,
                'real_rows': self.real_rows,
                'real_bytes': self.real_bytes,
                'state': self._state.value,
                'labels': self.labels,
            },
        }
        return json.dumps(report, indent=CBS_DEFAULT_JSON_INDENT)

    def _format_time(self, value):
        return value.strftime(self.date_fmt) if value else None

    @classmethod
    def load_json(cls, data):
        """
        Load struct from json data
        """
        # pylint: disable=protected-access
        try:
            loaded = json.loads(data)
            meta = loaded['meta']

            backup = ClickhouseBackupStructure(
                name=meta['name'],
                path=meta['path'],
                labels=meta.get('labels'),
                ch_version=meta.get('ch_version'),
                hostname=meta['hostname'],
                date_fmt=meta['date_fmt'])
            backup._databases = loaded['databases']
            backup.start_time = cls._load_time(meta, 'start_time')
            backup.end_time = cls._load_time(meta, 'end_time')
            backup.rows = meta['rows']
            backup.bytes = meta['bytes']

            # TODO: delete in few months
            if 'state' in meta:
                backup.real_rows = meta['real_rows']
                backup.real_bytes = meta['real_bytes']
                backup._state = ClickhouseBackupState(meta['state'])
            else:
                backup._state = ClickhouseBackupState.CREATED

            return backup

        except (ValueError, KeyError):
            raise InvalidBackupStruct

    @staticmethod
    def _load_time(meta, attr):
        result = datetime.strptime(meta[attr], meta['date_fmt'])
        if result.tzinfo is None:
            result = result.replace(tzinfo=timezone.utc)
        return result

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

    def get_databases(self):
        """
        Get databases meta
        """
        return tuple(self._databases)

    def get_tables(self, db_name):
        """
        Get tables for specified database
        """
        return tuple(self._databases[db_name]['parts_paths'])

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
    def add_part_contents(self, db_name, table_name, part_info):
        """
        Add part backup contents to backup struct
        """
        self._databases[db_name]['parts_paths'][table_name].update({
            part_info.name: {
                'link': part_info.link,
                'paths': part_info.paths,
                'meta': part_info.get_contents(),
            },
        })
        part_rows = int(part_info.rows)
        part_bytes = int(part_info.bytes)
        self.rows += part_rows
        self.bytes += part_bytes
        if not part_info.link:
            self.real_rows += part_rows
            self.real_bytes += part_bytes

    def del_part_contents(self, db_name, table_name, part_name):
        """
        Delete part contents from backup struct
        """
        part = \
            self._databases[db_name]['parts_paths'][table_name].pop(part_name)
        part_info = ClickhousePartInfo(meta=part['meta'], link=part['link'])
        part_rows = int(part_info.rows)
        part_bytes = int(part_info.bytes)
        self.rows -= part_rows
        self.bytes -= part_bytes
        if not part_info.link:
            # TODO: delete in few months
            if hasattr(self, 'real_rows'):
                self.real_rows -= part_rows
                self.real_bytes -= part_bytes

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
        return tuple(self._databases[db_name]['parts_paths'][table_name]
                     [part_name]['paths'])

    def is_part_linked(self, db_name, table_name, part_name):
        """
        Get storage file paths of specified part
        """
        return bool(self._databases[db_name]['parts_paths'][table_name]
                    [part_name]['link'])

    def get_parts(self, db_name, table_name):
        """
        Get all parts of specified database.table
        """
        return tuple(self._databases[db_name]['parts_paths'][table_name])

    def get_deduplicated_parts(self, deduplicated_to=None):
        """
        Get all deduplicated parts
        """
        deduplicated_parts = {}
        for db_name in self.get_databases():
            for table_name in self.get_tables(db_name):
                for part_name in self.get_parts(db_name, table_name):
                    content = self.get_part_contents(db_name, table_name,
                                                     part_name)
                    if not content['link']:
                        continue

                    if deduplicated_to and \
                            not content['link'].endswith(deduplicated_to):
                        continue
                    deduplicated_parts[(db_name, table_name, part_name)]\
                        = self.name
        return deduplicated_parts

    def is_empty(self):
        """
        Get storage file paths of specified part
        """
        # TODO: mb is enough to check bytes&rows
        for db_name in self.get_databases():
            for table_name in self.get_tables(db_name):
                if self.get_parts(db_name, table_name):
                    return False
        return True


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

    @property
    def bytes(self):
        """
        The size of part on disk in bytes.
        """
        # bytes_on_disk is a new name starting from ClickHouse 1.1.54380.
        return self._meta.get('bytes_on_disk', self._meta.get('bytes'))

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
