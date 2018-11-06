"""
Backup metadata.
"""
import copy
import json
import socket
from collections.__init__ import defaultdict
from datetime import datetime, timezone
from enum import Enum
from typing import Dict, Iterable, Optional, Sequence, Tuple

from ch_backup.exceptions import InvalidBackupStruct, UnknownBackupStateError
from ch_backup.util import now

CBS_DEFAULT_DATE_FMT = '%Y-%m-%d %H:%M:%S %z'


class BackupState(Enum):
    """
    Backup states.
    """

    NOT_STARTED = 'not_started'
    CREATED = 'created'
    CREATING = 'creating'
    DELETING = 'deleting'
    PARTIALLY_DELETED = 'partially_deleted'
    FAILED = 'failed'


class Part:
    """
    Part metadata.
    """

    def __init__(self, meta: dict, link=None, paths=None) -> None:
        self._meta = meta
        if link is None:
            link = False
        self.link = link
        self.paths = paths

    @property
    def bytes(self) -> int:
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

    def get_contents(self) -> dict:
        """
        Get part meta
        """
        return copy.deepcopy(self._meta)


class BackupMetadata:
    """
    Backup metadata.
    """

    # pylint: disable=too-many-instance-attributes

    def __init__(self,
                 name: str,
                 path: str,
                 ch_version: str,
                 date_fmt=None,
                 hostname=None,
                 labels=None) -> None:
        self.name = name
        self.labels = labels
        self.path = path
        self.ch_version = ch_version
        self.hostname = hostname or socket.getfqdn()
        self.rows = 0
        self.bytes = 0
        self.real_rows = 0
        self.real_bytes = 0
        self._state = BackupState.NOT_STARTED
        self.date_fmt = date_fmt or CBS_DEFAULT_DATE_FMT
        self.start_time = None  # type: Optional[datetime]
        self.end_time = None  # type: Optional[datetime]
        self._databases = {}  # type: Dict[str, dict]

    def add_database(self, db_name: str) -> None:
        """
        Add database dict to backup struct
        """
        self._databases[db_name] = {
            'db_sql_path': None,
            'tables_sql_paths': [],
            'parts_paths': defaultdict(dict),
        }

    def __str__(self) -> str:
        return self.dump_json()

    @property
    def state(self) -> BackupState:
        """
        Backup state
        """
        return self._state

    @state.setter
    def state(self, value: BackupState) -> None:
        if value not in BackupState:
            raise UnknownBackupStateError
        self._state = value

    def update_start_time(self) -> None:
        """
        Set start datetime
        """
        self.start_time = now()

    def update_end_time(self) -> None:
        """
        Set end datetime
        """
        self.end_time = now()

    def dump_json(self, indent=4) -> str:
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
        return json.dumps(report, indent=indent)

    @classmethod
    def load_json(cls, data):
        """
        Load struct from json data
        """
        # pylint: disable=protected-access
        try:
            loaded = json.loads(data)
            meta = loaded['meta']

            backup = BackupMetadata(
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
                backup._state = BackupState(meta['state'])
            else:
                backup._state = BackupState.CREATED

            return backup

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

    def add_part_contents(self, db_name: str, table_name: str,
                          part_info: Part):
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

    def del_part_contents(self, db_name: str, table_name: str,
                          part_name: str) -> None:
        """
        Delete part contents from backup struct
        """
        part = \
            self._databases[db_name]['parts_paths'][table_name].pop(part_name)
        part_info = Part(meta=part['meta'], link=part['link'])
        part_rows = int(part_info.rows)
        part_bytes = int(part_info.bytes)
        self.rows -= part_rows
        self.bytes -= part_bytes
        if not part_info.link:
            # TODO: delete in few months
            if hasattr(self, 'real_rows'):
                self.real_rows -= part_rows
                self.real_bytes -= part_bytes

    def get_part_contents(self, db_name: str, table_name: str, part_name: str):
        """
        Get part backup contents from backup struct
        """
        try:
            return self._databases[db_name]['parts_paths'][table_name][
                part_name]
        except KeyError:
            return None

    def get_part_paths(self, db_name: str, table_name: str,
                       part_name: str) -> Sequence[str]:
        """
        Get storage file paths of specified part
        """
        return tuple(self._databases[db_name]['parts_paths'][table_name]
                     [part_name]['paths'])

    def is_part_linked(self, db_name: str, table_name: str,
                       part_name: str) -> bool:
        """
        Get storage file paths of specified part
        """
        return bool(self._databases[db_name]['parts_paths'][table_name]
                    [part_name]['link'])

    def get_parts(self, db_name: str, table_name: str) -> Iterable:
        """
        Get all parts of specified database.table
        """
        return tuple(self._databases[db_name]['parts_paths'][table_name])

    def get_deduplicated_parts(self, deduplicated_to: str = None) \
            -> Dict[Tuple[str, str, str], str]:
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

    def is_empty(self) -> bool:
        """
        Return True if backup has no data.
        """
        return self.bytes == 0

    def _format_time(self, value):
        return value.strftime(self.date_fmt) if value else None

    @staticmethod
    def _load_time(meta, attr):
        attr_value = meta.get(attr)
        if not attr_value:
            return None

        result = datetime.strptime(attr_value, meta['date_fmt'])
        if result.tzinfo is None:
            result = result.replace(tzinfo=timezone.utc)

        return result
