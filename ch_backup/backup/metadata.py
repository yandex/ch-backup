"""
Backup metadata.
"""
import json
import socket
from collections.__init__ import defaultdict
from datetime import datetime, timezone
from enum import Enum
from typing import Dict, Iterable, List, Optional, Sequence

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
        return int(self._meta.get('bytes_on_disk', self._meta.get('bytes')))

    @property
    def rows(self) -> int:
        """
        The number of rows in the part.
        """
        return int(self._meta['rows'])

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

    def dump(self) -> dict:
        """
        Return part metadata as a dict.
        """
        return {
            'meta': self._meta,
            'paths': self.paths,
            'link': self.link,
        }


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
        self._state = BackupState.NOT_STARTED
        self.date_fmt = date_fmt or CBS_DEFAULT_DATE_FMT
        self.start_time = None  # type: Optional[datetime]
        self.end_time = None  # type: Optional[datetime]
        self._databases = {}  # type: Dict[str, dict]
        self.rows = 0
        self.bytes = 0
        self.real_rows = 0
        self.real_bytes = 0

    def __str__(self) -> str:
        return self.dump_json()

    @property
    def state(self) -> BackupState:
        """
        Backup state.
        """
        return self._state

    @state.setter
    def state(self, value: BackupState) -> None:
        if value not in BackupState:
            raise UnknownBackupStateError
        self._state = value

    def update_start_time(self) -> None:
        """
        Set start time to the current time.
        """
        self.start_time = now()

    def update_end_time(self) -> None:
        """
        Set end time to the current time.
        """
        self.end_time = now()

    def dump_json(self, indent=4) -> str:
        """
        Return json representation of backup metadata.
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
        Load backup metadata from json representation.
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

    def add_database(self, db_name: str, meta_remote_path: str) -> None:
        """
        Add database to backup metadata.
        """
        self._databases[db_name] = {
            'db_sql_path': meta_remote_path,
            'tables_sql_paths': [],
            'parts_paths': defaultdict(dict),
        }

    def get_databases(self) -> Sequence[str]:
        """
        Get databases.
        """
        return tuple(self._databases)

    def get_db_sql_path(self, db_name: str) -> str:
        """
        Get database sql path.
        """
        return self._databases[db_name]['db_sql_path']

    def add_table(self, db_name: str, table_name: str,
                  meta_remote_path: str) -> None:
        """
        Add table to backup metadata.
        """
        self._databases[db_name]['tables_sql_paths'].append((table_name,
                                                             meta_remote_path))

    def get_tables(self, db_name: str) -> Sequence[str]:
        """
        Get tables for the specified database.
        """
        return tuple(self._databases[db_name]['parts_paths'])

    def get_tables_sql_paths(self, db_name: str) -> Sequence[str]:
        """
        Get sql paths of database tables.
        """
        return [
            sql_path
            for _, sql_path in self._databases[db_name]['tables_sql_paths']
        ]

    def add_part(self, part: Part) -> None:
        """
        Add data part to backup metadata.
        """
        self._databases[part.database]['parts_paths'][part.table][
            part.name] = part.dump()
        self.rows += part.rows
        self.bytes += part.bytes
        if not part.link:
            self.real_rows += part.rows
            self.real_bytes += part.bytes

    def remove_part(self, part: Part) -> None:
        """
        Remove data part from backup metadata.
        """
        self._table_parts(part.database, part.table).pop(part.name)
        self.rows -= part.rows
        self.bytes -= part.bytes
        if not part.link:
            # TODO: delete in few months
            if hasattr(self, 'real_rows'):
                self.real_rows -= part.rows
                self.real_bytes -= part.bytes

    def get_part(self, db_name: str, table_name: str,
                 part_name: str) -> Optional[Part]:
        """
        Get data part.
        """
        raw_part = self._table_parts(db_name, table_name).get(part_name)
        return Part(**raw_part) if raw_part else None

    def get_parts(self, db_name: str = None,
                  table_name: str = None) -> Sequence[Part]:
        """
        Get data parts.
        """
        if table_name:
            assert db_name

        databases = [db_name] if db_name else self.get_databases()

        parts = []  # type: List[Part]
        for db in databases:
            parts.extend(self._iter_database_parts(db, table_name))

        return parts

    def is_empty(self) -> bool:
        """
        Return True if backup has no data.
        """
        return self.bytes == 0

    def _table_parts(self, db_name: str, table_name: str) -> dict:
        try:
            return self._databases[db_name]['parts_paths'][table_name]
        except KeyError:
            return {}

    def _iter_database_parts(self, db_name: str,
                             table_name: str = None) -> Iterable[Part]:
        tables = [table_name] if table_name else self.get_tables(db_name)
        for table in tables:
            for raw_part in self._table_parts(db_name, table).values():
                yield Part(**raw_part)

    def _format_time(self, value: Optional[datetime]) -> Optional[str]:
        return value.strftime(self.date_fmt) if value else None

    @staticmethod
    def _load_time(meta: dict, attr: str) -> Optional[datetime]:
        attr_value = meta.get(attr)
        if not attr_value:
            return None

        result = datetime.strptime(attr_value, meta['date_fmt'])
        if result.tzinfo is None:
            result = result.replace(tzinfo=timezone.utc)

        return result
