"""
Backup metadata.
"""
import json
import os
import socket
from datetime import datetime, timezone
from enum import Enum
from types import SimpleNamespace
from typing import Dict, Iterable, List, Optional, Sequence

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

    def __init__(self,
                 database: str,
                 table: str,
                 name: str,
                 checksum: str,
                 size: int,
                 files: Sequence[str],
                 link: str = None) -> None:
        super().__init__()
        self.database = database
        self.table = table
        self.name = name
        self.size = size
        self.checksum = checksum
        self.files = files
        self.link = link


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
            backup.version = meta['version']

            return backup

        except (ValueError, KeyError):
            raise InvalidBackupStruct

    def add_database(self, db_name: str) -> None:
        """
        Add database to backup metadata.
        """
        assert db_name not in self._databases

        self._databases[db_name] = {
            'tables': {},
        }

    def get_databases(self) -> Sequence[str]:
        """
        Get databases.
        """
        return tuple(self._databases.keys())

    def add_table(self, db_name: str, table_name: str) -> None:
        """
        Add table to backup metadata.
        """
        tables = self._databases[db_name]['tables']

        assert table_name not in tables

        tables[table_name] = {
            'parts': {},
        }

    def get_tables(self, db_name: str) -> Sequence[str]:
        """
        Get tables for the specified database.
        """
        # TODO: remove backward-compatibility logic
        db = self._databases[db_name]
        if 'tables' in db:
            return tuple(db['tables'].keys())

        return tuple(db['parts_paths'].keys())

    def add_part(self, part: PartMetadata) -> None:
        """
        Add data part to backup metadata.
        """
        parts = self._databases[part.database]['tables'][part.table]['parts']

        assert part.name not in parts

        parts[part.name] = {
            'checksum': part.checksum,
            'bytes': part.size,
            'files': part.files,
            'link': part.link,
        }

        self.size += part.size
        if not part.link:
            self.real_size += part.size

    def remove_part(self, part: PartMetadata) -> None:
        """
        Remove data part from backup metadata.
        """
        # TODO: remove backward-compatibility logic
        db = self._databases[part.database]
        try:
            parts = db['tables'][part.table]['parts']
        except KeyError:
            parts = db['parts_paths'][part.table]

        del parts[part.name]

        self.size -= part.size
        if not part.link:
            self.real_size -= part.size

    def get_part(self, db_name: str, table_name: str,
                 part_name: str) -> Optional[PartMetadata]:
        """
        Get data part.
        """
        db = self._databases.get(db_name)
        if not db:
            return None

        # TODO: remove backward-compatibility logic
        try:
            if 'parts_paths' in db:
                part = db['parts_paths'][table_name][part_name]
            else:
                part = db['tables'][table_name]['parts'][part_name]
        except KeyError:
            return None

        return self._load_part(db_name, table_name, part_name, part)

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

    def _iter_database_parts(self, db_name: str,
                             table_name: str = None) -> Iterable[PartMetadata]:
        tables = [table_name] if table_name else self.get_tables(db_name)
        for table in tables:
            yield from self._iter_table_parts(db_name, table)

    def _iter_table_parts(self, db_name: str,
                          table_name: str) -> Iterable[PartMetadata]:
        db = self._databases[db_name]
        # TODO: remove backward-compatibility logic
        if 'parts_paths' in db:
            parts = db['parts_paths'].get(table_name, {})
        else:
            parts = db['tables'][table_name]['parts']

        for part_name, metadata in parts.items():
            yield self._load_part(db_name, table_name, part_name, metadata)

    def _load_part(self, db_name: str, table_name: str, part_name: str,
                   metadata: dict) -> PartMetadata:
        # TODO: remove backward-compatibility logic
        if 'meta' in metadata:
            meta = metadata['meta']
            return PartMetadata(
                database=db_name,
                table=table_name,
                name=part_name,
                checksum=meta['checksum'],
                size=meta['bytes'],
                files=[os.path.basename(p) for p in metadata['paths']],
                link=metadata['link'])

        return PartMetadata(database=db_name,
                            table=table_name,
                            name=part_name,
                            checksum=metadata['checksum'],
                            size=metadata['bytes'],
                            files=metadata['files'],
                            link=metadata['link'])

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
