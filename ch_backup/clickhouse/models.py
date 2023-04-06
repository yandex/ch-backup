"""
ClickHouse resource models.
"""

import re
from types import SimpleNamespace
from typing import List, Optional, Tuple

import ch_backup.logging


class Disk(SimpleNamespace):
    """
    ClickHouse disk.
    """
    def __init__(self, name: str, path: str, disk_type: str, cache_path: str = ''):
        super().__init__()
        self.name = name
        self.path = path
        self.type = disk_type
        self.cache_path = cache_path


class Table(SimpleNamespace):
    """
    ClickHouse table.
    """
    def __init__(self, database: str, name: str, engine: str, disks: List[Disk], data_paths: List[str],
                 create_statement: str, uuid: Optional[str]) -> None:
        super().__init__()
        self.database = database
        self.name = name
        self.engine = engine
        self.create_statement = create_statement
        self.uuid = uuid
        self.paths_with_disks = self._map_paths_to_disks(disks, data_paths)

    def _map_paths_to_disks(self, disks: List[Disk], data_paths: List[str]) -> List[Tuple[str, Disk]]:
        return list(map(lambda data_path: (data_path, self._map_path_to_disk(disks, data_path)), data_paths))

    def __hash__(self):
        return hash((self.database, self.name))

    def is_dictionary(self) -> bool:
        """
        Return True if table is dictionary.
        """
        return self.engine == 'Dictionary'

    @staticmethod
    def _map_path_to_disk(disks: List[Disk], data_path: str) -> Disk:
        matched_disks = list(filter(lambda disk: data_path.startswith(disk.path), disks))

        # Disks are sorted by their length of path.
        # We return disk with longest path matched to given data_path here.
        return matched_disks[0]


class Database(SimpleNamespace):
    """
    ClickHouse database.
    """
    def __init__(self, name: str, engine: Optional[str], metadata_path: Optional[str]) -> None:
        super().__init__()
        self.name = name
        self.engine = engine
        self.metadata_path = metadata_path

    def is_atomic(self) -> bool:
        """
        Return True if database engine is Atomic or derived.
        """
        return self.engine in ['Atomic', 'Replicated']

    def is_replicated_db_engine(self) -> bool:
        """
        Return True if database engine is Replicated, or False otherwise.
        """
        return self.engine == 'Replicated'

    def is_external_db_engine(self) -> bool:
        """
        Return True if the specified database engine is intended to use for integration with external systems.
        """
        return self.engine in ('MySQL', 'MaterializedMySQL', 'PostgreSQL', 'MaterializedPostgreSQL')

    def has_embedded_metadata(self) -> bool:
        """
        Return True if db create statement shouldn't be uploaded and applied with restore.
        """
        return self.name in [
            'default',
            'system',
            '_temporary_and_external_tables',
            'information_schema',
            'INFORMATION_SCHEMA',
        ]

    def set_engine_from_sql(self, db_sql: str) -> None:
        """
        Parse database engine from create query and set it.
        """
        match = re.search(r'(?i)Engine\s*=\s*(?P<engine>\S+)', db_sql)
        if match is None:
            ch_backup.logging.warning(f'Failed to parse engine for database "{self.name}", from query: "{db_sql}"')
        else:
            self.engine = match.group('engine')


class FrozenPart(SimpleNamespace):
    """
    Freezed data part.
    """
    def __init__(self, database: str, table: str, name: str, disk_name: str, path: str, checksum: str, size: int,
                 files: List[str]):
        super().__init__()
        self.database = database
        self.table = table
        self.name = name
        self.disk_name = disk_name
        self.path = path
        self.checksum = checksum
        self.size = size
        self.files = files
