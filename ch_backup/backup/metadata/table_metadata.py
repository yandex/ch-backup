"""
Backup metadata for ClickHouse table.
"""

from types import SimpleNamespace
from typing import List, NamedTuple, Optional, Set

from ch_backup.backup.metadata.part_metadata import PartMetadata


class PartInfo(NamedTuple):
    """
    Parsed part name.
    https://github.com/ClickHouse/ClickHouse/blob/e2821c5e8b728d1d28f9e0b98db87e0af5bc4a29/src/Storages/MergeTree/MergeTreePartInfo.cpp#L54
    """

    partition_id: str
    min_block_num: int
    max_block_num: int
    level: int
    mutation: int


class TableMetadata(SimpleNamespace):
    """
    Backup metadata for ClickHouse table.
    """

    def __init__(
        self, database: str, name: str, engine: str, uuid: Optional[str]
    ) -> None:
        super().__init__()
        self.database: str = database
        self.name: str = name
        self.raw_metadata: dict = {
            "engine": engine,
            "uuid": uuid,
            "parts": {},
        }

    @property
    def engine(self) -> str:
        """
        Return table engine.
        """
        return self.raw_metadata["engine"]

    @property
    def uuid(self) -> Optional[str]:
        """
        Return uuid of the table if not zero. Used for view restore in ch > 20.10
        """
        return self.raw_metadata["uuid"]

    def get_parts(self, *, excluded_parts: Set[str] = None) -> List[PartMetadata]:
        """
        Return data parts (sorted).
        """
        if not excluded_parts:
            excluded_parts = set()

        result = []
        for part_name, raw_metadata in self.raw_metadata["parts"].items():
            if part_name not in excluded_parts:
                result.append(
                    PartMetadata.load(self.database, self.name, part_name, raw_metadata)
                )

        def split_part_name(part: str) -> PartInfo:
            max_split = 4
            chunks = part.split("_", maxsplit=max_split)
            partition_id = ""
            level = 0
            mutation = 0
            try:
                partition_id = chunks[0]
                min_block_num = int(chunks[1])
                max_block_num = int(chunks[2])
                level = int(chunks[3])
                if max_split + 1 == len(chunks):
                    mutation = int(chunks[4])
            except (IndexError, ValueError):
                min_block_num = 0
                max_block_num = 0
            return PartInfo(partition_id, min_block_num, max_block_num, level, mutation)

        result.sort(key=lambda part: split_part_name(part.name))
        return result

    def add_part(self, part: PartMetadata) -> None:
        """
        Add data part to metadata.
        """
        assert part.database == self.database
        assert part.table == self.name
        assert part.name not in self.raw_metadata["parts"]

        self.raw_metadata["parts"][part.name] = {
            "checksum": part.checksum,
            "bytes": part.size,
            "files": part.files,
            "link": part.link,
            "tarball": part.tarball,
            "disk_name": part.disk_name,
            "encrypted": part.encrypted,
        }

    @classmethod
    def load(cls, database: str, name: str, raw_metadata: dict) -> "TableMetadata":
        """
        Deserialize table metadata.
        """
        table = cls(
            database, name, raw_metadata["engine"], raw_metadata.get("uuid", None)
        )
        table.raw_metadata["parts"] = raw_metadata["parts"]
        return table
