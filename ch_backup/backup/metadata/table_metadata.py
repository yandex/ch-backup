"""
Backup metadata for ClickHouse table.
"""

from enum import Enum
from types import SimpleNamespace
from typing import NamedTuple

from ch_backup.backup.metadata.detached_part_metadata import DetachedPartMetadata
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


def split_part_name(part: str) -> PartInfo:
    """
    Splitting part's name to get underlying information.

    Raises ValueError if the part name does not match the ClickHouse
    part name format: ``{partition_id}_{min_block}_{max_block}_{level}[_{mutation}]``.
    """

    # At most 6 fields (5 separators); excess separators stay in the last field
    # and fail numeric validation without allocating a list entry for each one.
    chunks = part.split("_", maxsplit=5)
    if (
        not chunks[0]
        or not part.isascii()
        or not all(value.isdigit() for value in chunks[1:])
    ):
        raise ValueError(
            f"Invalid part name format: '{part}'. "
            "Numeric segments must contain decimal digits only"
        )
    # Legacy format has 5 fields, or 6 with mutation:
    # minDate_maxDate_minBlock_maxBlock_level[_mutation].
    # Dates are YYYYMMDD (8 digits); partition_id is YYYYMM (6 digits).
    # Five fields can also be modern: prefer that layout if minBlock <= maxBlock.
    # Without the table format version, some names are inherently ambiguous.
    if (
        len(chunks) in (5, 6)
        and len(chunks[0]) == len(chunks[1]) == 8
        and chunks[0].isdigit()
        and (len(chunks) == 6 or int(chunks[1]) > int(chunks[2]))
    ):
        chunks = [chunks[0][:6], *chunks[2:]]
    # Modern or normalized format has 4 fields, or 5 with mutation.
    if len(chunks) not in (4, 5):
        raise ValueError(
            f"Invalid part name format: '{part}'. "
            f"Expected format: partition_id_minBlock_maxBlock_level[_mutation]"
        )
    partition_id = chunks[0]
    min_block, max_block, level = chunks[1:4]
    mutation = chunks[4] if len(chunks) == 5 else "0"
    min_block_num = int(min_block)
    max_block_num = int(max_block)
    if min_block_num > max_block_num:
        raise ValueError(
            f"Invalid part name format: '{part}'. "
            "Minimum block number exceeds maximum block number"
        )
    return PartInfo(
        partition_id,
        min_block_num,
        max_block_num,
        int(level),
        int(mutation),
    )


class DataSkippedReason(str, Enum):
    """
    Represents why the table data was left out of the backup.
    """

    UNIQUE_KEY = "unique_key"
    BACKUP_FAILED = "backup_failed"


class TableMetadata(SimpleNamespace):
    """
    Backup metadata for ClickHouse table.
    """

    # pylint: disable=too-many-positional-arguments
    def __init__(
        self,
        database: str,
        name: str,
        engine: str,
        uuid: str | None,
        data_skipped_reason: str | None = None,
    ) -> None:
        super().__init__()
        self.database: str = database
        self.name: str = name
        self.raw_metadata: dict = {
            "engine": engine,
            "uuid": uuid,
            "parts": {},
        }
        if data_skipped_reason:
            self.raw_metadata["data_skipped_reason"] = data_skipped_reason

    @property
    def data_skipped_reason(self) -> str | None:
        """
        Return why the table data was left out of the backup, or None if it was backed up.
        """
        return self.raw_metadata.get("data_skipped_reason")

    @property
    def skipped_parts(self) -> list[str]:
        """
        Return names of data parts left out of the backup.
        """
        return self.raw_metadata.get("skipped_parts", [])

    @property
    def engine(self) -> str:
        """
        Return table engine.
        """
        return self.raw_metadata["engine"]

    @property
    def uuid(self) -> str | None:
        """
        Return uuid of the table if not zero. Used for view restore in ch > 20.10
        """
        return self.raw_metadata["uuid"]

    def get_parts(self, *, excluded_parts: set[str] = None) -> list[PartMetadata]:
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
            "link_part_name": part.link_part_name,
            "tarball": part.tarball,
            "disk_name": part.disk_name,
            "encrypted": part.encrypted,
        }

    def get_detached_parts(self) -> list[DetachedPartMetadata]:
        """Return detached parts sorted by disk and original directory name."""
        result = []
        for disk_name, disk_parts in self.raw_metadata.get(
            "detached_parts", {}
        ).items():
            for part_name, raw_metadata in disk_parts.items():
                result.append(
                    DetachedPartMetadata.load(
                        self.database,
                        self.name,
                        disk_name,
                        part_name,
                        raw_metadata,
                    )
                )
        return sorted(result, key=lambda part: (part.disk_name, part.name))

    def add_detached_part(self, part: DetachedPartMetadata) -> None:
        """Add a detached part without mixing it with attachable table parts."""
        assert part.database == self.database
        assert part.table == self.name

        disk_parts = self.raw_metadata.setdefault("detached_parts", {}).setdefault(
            part.disk_name, {}
        )
        assert part.name not in disk_parts
        disk_parts[part.name] = {
            "reason": part.reason,
            "checksum": part.checksum,
            "bytes": part.size,
            "files": part.files,
            "tarball": part.tarball,
            "encrypted": part.encrypted,
            "object_storage": part.object_storage,
        }

    @classmethod
    def load(cls, database: str, name: str, raw_metadata: dict) -> "TableMetadata":
        """
        Deserialize table metadata.
        """
        table = cls(
            database,
            name,
            raw_metadata["engine"],
            raw_metadata.get("uuid", None),
            raw_metadata.get("data_skipped_reason", None),
        )
        table.raw_metadata["parts"] = raw_metadata["parts"]
        if "detached_parts" in raw_metadata:
            table.raw_metadata["detached_parts"] = raw_metadata["detached_parts"]
        if "skipped_parts" in raw_metadata:
            table.raw_metadata["skipped_parts"] = raw_metadata["skipped_parts"]
        return table
