"""Backup metadata for a detached ClickHouse data part."""

from typing import Sequence

from ch_backup.backup.metadata.part_metadata import RawMetadata
from ch_backup.clickhouse.models import DetachedPart
from ch_backup.util import Slotted


class DetachedPartMetadata(Slotted):
    """Metadata required to restore a part into a table's detached directory."""

    __slots__ = (
        "database",
        "table",
        "name",
        "reason",
        "object_storage",
        "raw_metadata",
    )

    # pylint: disable=too-many-arguments,too-many-positional-arguments
    def __init__(
        self,
        database: str,
        table: str,
        name: str,
        reason: str | None,
        checksum: str,
        size: int,
        files: Sequence[str],
        tarball: bool,
        disk_name: str,
        encrypted: bool = True,
        object_storage: bool = False,
    ) -> None:
        self.database = database
        self.table = table
        # ``name`` is the on-disk directory name. For automatically detached
        # parts it already contains the reason prefix (for example,
        # ``broken-on-start_all_1_1_0``), and must be preserved verbatim.
        self.name = name
        self.reason = reason
        self.object_storage = object_storage
        self.raw_metadata = RawMetadata(
            checksum=checksum,
            size=size,
            files=files,
            tarball=tarball,
            disk_name=disk_name,
            encrypted=encrypted,
        )

    @property
    def checksum(self) -> str:
        """Return the part checksum, or an empty string for malformed parts."""
        return self.raw_metadata.checksum

    @property
    def size(self) -> int:
        """Return the aligned size of files archived for the part.

        For object-storage disks these are local metadata files, not the
        referenced objects in the bucket.
        """
        return self.raw_metadata.size

    @property
    def files(self) -> Sequence[str]:
        """Return relative paths of files in the part."""
        return self.raw_metadata.files

    @property
    def disk_name(self) -> str:
        """Return the source disk name."""
        return self.raw_metadata.disk_name or "default"

    @property
    def deduplicated_part_name(self) -> str:
        """Return the name used by the cloud-storage copy path.

        Detached parts are intentionally never deduplicated: their original
        directory names, including a reason prefix, are part of the backup.
        """
        return self.name

    @property
    def encrypted(self) -> bool:
        """Return whether the archived part is encrypted."""
        return self.raw_metadata.encrypted

    @property
    def tarball(self) -> bool:
        """Return whether the files are stored as one tar archive."""
        return self.raw_metadata.tarball

    def with_name(self, name: str) -> "DetachedPartMetadata":
        """Return equivalent metadata for a different detached directory name."""
        return type(self)(
            database=self.database,
            table=self.table,
            name=name,
            reason=self.reason,
            checksum=self.checksum,
            size=self.size,
            files=self.files,
            tarball=self.tarball,
            disk_name=self.disk_name,
            encrypted=self.encrypted,
            object_storage=self.object_storage,
        )

    @classmethod
    def load(
        cls,
        db_name: str,
        table_name: str,
        disk_name: str,
        part_name: str,
        raw_metadata: dict,
    ) -> "DetachedPartMetadata":
        """Deserialize detached part metadata."""
        return cls(
            database=db_name,
            table=table_name,
            name=part_name,
            reason=raw_metadata.get("reason"),
            checksum=raw_metadata.get("checksum", ""),
            size=raw_metadata["bytes"],
            files=raw_metadata["files"],
            tarball=raw_metadata.get("tarball", False),
            disk_name=disk_name,
            encrypted=raw_metadata.get("encrypted", True),
            object_storage=raw_metadata.get("object_storage", False),
        )

    @classmethod
    def from_detached_part(
        cls, detached_part: DetachedPart, encrypted: bool
    ) -> "DetachedPartMetadata":
        """Convert a scanned detached part into backup metadata."""
        return cls(
            database=detached_part.database,
            table=detached_part.table,
            name=detached_part.name,
            reason=detached_part.reason,
            checksum=detached_part.checksum,
            size=detached_part.size,
            files=detached_part.files,
            tarball=True,
            disk_name=detached_part.disk_name,
            encrypted=encrypted,
            object_storage=detached_part.object_storage,
        )
