"""
Backup metadata.
"""
import json
import socket
from collections.__init__ import defaultdict
from datetime import datetime, timezone
from enum import Enum
from types import SimpleNamespace
from typing import Dict, Iterable, List, Optional, Sequence

from ch_backup.clickhouse.control import FreezedPart
from ch_backup.exceptions import InvalidBackupStruct, UnknownBackupStateError
from ch_backup.util import now

CBS_DEFAULT_DATE_FMT = '%Y-%m-%d %H:%M:%S %z'


class BackupState(Enum):
    """
    Backup states.
    """

    CREATED = 'created'
    CREATING = 'creating'
    DELETING = 'deleting'
    PARTIALLY_DELETED = 'partially_deleted'
    FAILED = 'failed'


class PartMetadata(SimpleNamespace):
    """
    Backup metadata for ClickHouse data part.
    """

    def __init__(self, fpart: FreezedPart, link: Optional[str],
                 paths: Sequence[str]) -> None:
        super().__init__()
        self.database = fpart.database
        self.table = fpart.table
        self.name = fpart.name
        self.size = fpart.size
        self.checksum = fpart.checksum
        self.link = link
        self.paths = paths

    def dump(self) -> dict:
        """
        Convert data part metadata to dict representation.
        """
        return {
            'meta': {
                'database': self.database,
                'table': self.table,
                'name': self.name,
                'bytes': self.size,
                'checksum': self.checksum,
            },
            'paths': self.paths,
            'link': self.link,
        }

    @classmethod
    def load(cls, value: dict) -> 'PartMetadata':
        """
        Create PartMetadata object from a dict.
        """
        meta = value['meta']

        part = cls.__new__(cls)
        part.database = meta['database']
        part.table = meta['table']
        part.name = meta['name']
        part.link = value['link']
        part.paths = value['paths']
        part.size = meta['bytes']
        part.checksum = meta['checksum']

        return part


class BackupMetadata:
    """
    Backup metadata.
    """

    # pylint: disable=too-many-instance-attributes

    def __init__(self,
                 name: str,
                 path: str,
                 version: str,
                 ch_version: str,
                 date_fmt: str = None,
                 hostname: str = None,
                 labels: dict = None) -> None:
        self.name = name
        self.labels = labels
        self.path = path
        self.version = version
        self.ch_version = ch_version
        self.hostname = hostname or socket.getfqdn()
        self._state = BackupState.CREATING
        self.date_fmt = date_fmt or CBS_DEFAULT_DATE_FMT
        self.start_time = now()
        self.end_time: Optional[datetime] = None
        self._databases: Dict[str, dict] = {}
        self.size = 0
        self.real_size = 0

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

    def update_end_time(self) -> None:
        """
        Set end time to the current time.
        """
        self.end_time = now()

    def dump_json(self) -> str:
        """
        Return json representation of backup metadata.
        """
        report = {
            'databases': self._databases,
            'meta': {
                'name': self.name,
                'path': self.path,
                'version': self.version,
                'ch_version': self.ch_version,
                'hostname': self.hostname,
                'date_fmt': self.date_fmt,
                'start_time': self._format_time(self.start_time),
                'end_time': self._format_time(self.end_time),
                'bytes': self.size,
                'real_bytes': self.real_size,
                'state': self._state.value,
                'labels': self.labels,
            },
        }
        return json.dumps(report, separators=(',', ':'))

    @classmethod
    def load_json(cls, data):
        """
        Load backup metadata from json representation.
        """
        # pylint: disable=protected-access
        try:
            loaded = json.loads(data)
            meta = loaded['meta']

            backup = cls.__new__(cls)
            backup.name = meta['name']
            backup.path = meta['path']
            backup.hostname = meta['hostname']
            backup.date_fmt = meta['date_fmt']
            backup._databases = loaded['databases']
            backup.start_time = cls._load_time(meta, 'start_time')
            backup.end_time = cls._load_time(meta, 'end_time')
            backup.size = meta['bytes']
            backup.real_size = meta['real_bytes']
            backup._state = BackupState(meta['state'])
            backup.ch_version = meta['ch_version']
            backup.labels = meta['labels']
            # get() is used for backward-compatibility with versions prior 1.0
            backup.version = meta.get('version')

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

    def add_part(self, part: PartMetadata) -> None:
        """
        Add data part to backup metadata.
        """
        self._databases[part.database]['parts_paths'][part.table][
            part.name] = part.dump()
        self.size += part.size
        if not part.link:
            self.real_size += part.size

    def remove_part(self, part: PartMetadata) -> None:
        """
        Remove data part from backup metadata.
        """
        self._table_parts(part.database, part.table).pop(part.name)
        self.size -= part.size
        if not part.link:
            self.real_size -= part.size

    def get_part(self, db_name: str, table_name: str,
                 part_name: str) -> Optional[PartMetadata]:
        """
        Get data part.
        """
        raw_part = self._table_parts(db_name, table_name).get(part_name)
        return PartMetadata.load(raw_part) if raw_part else None

    def get_parts(self, db_name: str = None,
                  table_name: str = None) -> Sequence[PartMetadata]:
        """
        Get data parts.
        """
        if table_name:
            assert db_name

        databases = [db_name] if db_name else self.get_databases()

        parts: List[PartMetadata] = []
        for db in databases:
            parts.extend(self._iter_database_parts(db, table_name))

        return parts

    def is_empty(self) -> bool:
        """
        Return True if backup has no data.
        """
        return self.size == 0

    def _table_parts(self, db_name: str, table_name: str) -> dict:
        try:
            return self._databases[db_name]['parts_paths'][table_name]
        except KeyError:
            return {}

    def _iter_database_parts(self, db_name: str,
                             table_name: str = None) -> Iterable[PartMetadata]:
        tables = [table_name] if table_name else self.get_tables(db_name)
        for table in tables:
            for raw_part in self._table_parts(db_name, table).values():
                yield PartMetadata.load(raw_part)

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
