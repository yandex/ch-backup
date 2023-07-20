"""
Backup metadata.
"""
import json
import socket
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional, Sequence

from ch_backup.backup.metadata.access_control_metadata import AccessControlMetadata
from ch_backup.backup.metadata.cloud_storage_metadata import CloudStorageMetadata
from ch_backup.backup.metadata.common import BackupStorageFormat
from ch_backup.backup.metadata.part_metadata import PartMetadata
from ch_backup.backup.metadata.table_metadata import TableMetadata
from ch_backup.clickhouse.models import Database
from ch_backup.exceptions import InvalidBackupStruct, UnknownBackupStateError
from ch_backup.util import now


class BackupState(Enum):
    """
    Backup states.
    """

    CREATED = "created"
    CREATING = "creating"
    DELETING = "deleting"
    PARTIALLY_DELETED = "partially_deleted"
    FAILED = "failed"


class BackupMetadata:
    """
    Backup metadata.
    """

    # pylint: disable=too-many-instance-attributes
    # pylint: disable=too-many-arguments

    def __init__(
        self,
        name: str,
        path: str,
        version: str,
        ch_version: str,
        time_format: str,
        hostname: str = None,
        labels: dict = None,
        schema_only: bool = False,
    ) -> None:
        self.name = name
        self.labels = labels
        self.path = path
        self.version = version
        self.ch_version = ch_version
        self.hostname = hostname or socket.getfqdn()
        self.time_format = time_format
        self.start_time = now()
        self.end_time: Optional[datetime] = None
        self.size = 0
        self.real_size = 0
        self.schema_only = schema_only
        self.s3_revisions: Dict[str, int] = {}  # S3 disk name -> revision counter.
        self.cloud_storage: CloudStorageMetadata = CloudStorageMetadata()

        self._state = BackupState.CREATING
        self._databases: Dict[str, dict] = {}
        self._access_control = AccessControlMetadata()
        self._user_defined_functions: List[str] = []

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

    @property
    def start_time_str(self) -> str:
        """
        String representation of backup start time.
        """
        return self._format_time(self.start_time)

    @property
    def end_time_str(self) -> Optional[str]:
        """
        String representation of backup end time.
        """
        return self._format_time(self.end_time) if self.end_time else None

    def update_end_time(self) -> None:
        """
        Set end time to the current time.
        """
        self.end_time = now()

    def dump(self, light: bool = False) -> dict:
        """
        Serialize backup metadata.
        """
        return {
            "databases": self._databases if not light else {},
            "access_controls": self._access_control.dump() if not light else {},
            "user_defined_functions": self._user_defined_functions if not light else [],
            "cloud_storage": self.cloud_storage.dump(),
            "meta": {
                "name": self.name,
                "path": self.path,
                "version": self.version,
                "ch_version": self.ch_version,
                "hostname": self.hostname,
                "time_format": self.time_format,
                "start_time": self.start_time_str,
                "end_time": self.end_time_str,
                "bytes": self.size,
                "real_bytes": self.real_size,
                "state": self._state.value,
                "labels": self.labels,
                # TODO: clean up backward-compatibility logic (delete 'date_fmt'); it's required changes in int api
                # to replace 'date_fmt' with 'time_format'.
                "date_fmt": self.time_format,
                "schema_only": self.schema_only,
                "s3_revisions": self.s3_revisions,
            },
        }

    def dump_json(self, light: bool = False) -> str:
        """
        Return json representation of backup metadata.
        """
        return json.dumps(self.dump(light), separators=(",", ":"))

    @classmethod
    def load(cls, data: dict) -> "BackupMetadata":
        """
        Deserialize backup metadata.
        """
        # pylint: disable=protected-access
        try:
            meta = data["meta"]

            backup = cls.__new__(cls)
            backup.name = meta["name"]
            backup.path = meta["path"]
            backup.hostname = meta["hostname"]
            backup.time_format = meta["time_format"]
            backup._databases = data["databases"]

            if "access_control" in data:
                # For backward compatibility
                backup._access_control = AccessControlMetadata(
                    data.get("access_control", []),
                    data.get("access_control_meta", {}),
                    BackupStorageFormat.PLAIN,
                )
            else:
                # Stored under a new name for forward compatibility
                backup._access_control = AccessControlMetadata.load(
                    data.get("access_controls", {})
                )

            backup.cloud_storage = CloudStorageMetadata.load(
                data.get("cloud_storage", {})
            )
            backup.start_time = cls._load_time(meta, "start_time")
            backup.end_time = cls._load_time(meta, "end_time")
            backup.size = meta["bytes"]
            backup.real_size = meta["real_bytes"]
            backup._state = BackupState(meta["state"])
            backup.ch_version = meta["ch_version"]
            backup.labels = meta["labels"]
            backup.version = meta["version"]
            backup.schema_only = meta.get("schema_only", False)
            backup.s3_revisions = meta.get("s3_revisions", {})
            # TODO remove after a several weeks/months, when backups rotated
            backup._user_defined_functions = data.get(
                "user_defined_functions", meta.get("user_defined_functions", [])
            )

            return backup

        except (ValueError, KeyError) as e:
            raise InvalidBackupStruct(e)

    @classmethod
    def load_json(cls, data):
        """
        Deserialize backup metadata from JSON representation.
        """
        return cls.load(json.loads(data))

    def get_databases(self) -> Sequence[str]:
        """
        Get databases.
        """
        return tuple(self._databases.keys())

    def get_database(self, db_name: str) -> Database:
        """
        Get database.
        """
        db_meta = self._databases[db_name]
        return Database(db_name, db_meta.get("engine"), db_meta.get("metadata_path"))

    def add_database(self, db: Database) -> None:
        """
        Add database to backup metadata.
        """
        assert db.name not in self._databases

        self._databases[db.name] = {
            "engine": db.engine,
            "metadata_path": db.metadata_path,
            "tables": {},
        }

    def get_tables(self, db_name: str) -> Sequence[TableMetadata]:
        """
        Get tables for the specified database.
        """
        result = []
        for table_name, raw_metadata in self._databases[db_name]["tables"].items():
            result.append(TableMetadata.load(db_name, table_name, raw_metadata))

        return result

    def get_table(self, db_name: str, table_name: str) -> TableMetadata:
        """
        Get the specified table.
        """
        return TableMetadata.load(
            db_name, table_name, self._databases[db_name]["tables"][table_name]
        )

    def add_table(self, table: TableMetadata) -> None:
        """
        Add table to backup metadata.
        """
        tables = self._databases[table.database]["tables"]

        assert table.name not in tables

        tables[table.name] = table.raw_metadata

        for part in table.get_parts():
            self.size += part.size
            if not part.link:
                self.real_size += part.size

    def add_udf(self, udf_name: str) -> None:
        """
        Add user defined function in metadata
        """
        assert udf_name not in self._user_defined_functions
        self._user_defined_functions.append(udf_name)

    def get_udf(self) -> List[str]:
        """
        Get user defined function data
        """
        return self._user_defined_functions

    def get_parts(self) -> Sequence[PartMetadata]:
        """
        Get data parts of all tables.
        """
        parts: List[PartMetadata] = []
        for db_name in self.get_databases():
            for table in self.get_tables(db_name):
                parts.extend(table.get_parts())

        return parts

    def find_part(
        self, db_name: str, table_name: str, part_name: str
    ) -> Optional[PartMetadata]:
        """
        Find and return data part. If not found, None is returned.
        """
        try:
            part = self._databases[db_name]["tables"][table_name]["parts"][part_name]
            return PartMetadata.load(db_name, table_name, part_name, part)
        except KeyError:
            return None

    def add_part(self, part: PartMetadata) -> None:
        """
        Add data part to backup metadata.
        """
        self.get_table(part.database, part.table).add_part(part)

        self.size += part.size
        if not part.link:
            self.real_size += part.size

    def remove_parts(self, table: TableMetadata, parts: List[PartMetadata]) -> None:
        """
        Remove data parts from backup metadata.
        """
        _parts = self._databases[table.database]["tables"][table.name]["parts"]

        for part in parts:
            del _parts[part.name]

            self.size -= part.size
            if not part.link:
                self.real_size -= part.size

    def is_empty(self) -> bool:
        """
        Return True if backup has no data.
        """
        return self.size == 0

    @property
    def access_control(self) -> AccessControlMetadata:
        """
        Get access control objects.
        """
        return self._access_control

    def set_access_control(self, objects: Sequence[Dict[str, Any]]) -> None:
        """
        Build and add access control objects to backup metadata.
        """
        self._access_control = AccessControlMetadata.from_ch_objects(objects)

    def has_s3_data(self) -> bool:
        """
        Return True if backup has data on S3 disks.
        TODO: could be removed after denial of storing S3 revisions
        """
        return len(self.s3_revisions) > 0

    def get_sanitized_name(self) -> str:
        """
        ClickHouse will place shadow data under this directory.
        '-' character is replaced to '_' to avoid unnecessary escaping on CH side.
        """
        return self.name.replace("-", "_")

    def _format_time(self, value: datetime) -> str:
        return value.strftime(self.time_format)

    @staticmethod
    def _load_time(meta: dict, attr: str) -> Optional[datetime]:
        attr_value = meta.get(attr)
        if not attr_value:
            return None

        result = datetime.strptime(attr_value, meta["time_format"])
        if result.tzinfo is None:
            result = result.replace(tzinfo=timezone.utc)

        return result
