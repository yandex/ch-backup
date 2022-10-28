"""
ClickHouse resource models.
"""

from types import SimpleNamespace
from typing import List, Optional, Tuple

from ch_backup.backup.metadata import PartMetadata


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

    @staticmethod
    def _map_path_to_disk(disks: List[Disk], data_path: str) -> Disk:
        matched_disks = list(filter(lambda disk: data_path.startswith(disk.path), disks))

        # Disks are sorted by their length of path.
        # We return disk with longest path matched to given data_path here.
        return matched_disks[0]


class FreezedPart(SimpleNamespace):
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

    def to_part_metadata(self) -> PartMetadata:
        """
        Converts to PartMetadata.
        """
        return PartMetadata(database=self.database,
                            table=self.table,
                            name=self.name,
                            checksum=self.checksum,
                            size=self.size,
                            files=self.files,
                            tarball=True,
                            disk_name=self.disk_name)
