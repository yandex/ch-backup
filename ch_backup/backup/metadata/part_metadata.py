"""
Backup metadata for ClickHouse data part.
"""

from typing import Optional, Sequence

from ch_backup.clickhouse.models import FrozenPart
from ch_backup.util import Slotted


class RawMetadata(Slotted):
    """
    Raw metadata for ClickHouse data part.
    """

    __slots__ = "checksum", "size", "files", "tarball", "link", "disk_name", "encrypted"

    # pylint: disable=too-many-function-args
    def __init__(
        self,
        checksum: str,
        size: int,
        files: Sequence[str],
        tarball: bool,
        link: str = None,
        disk_name: str = None,
        encrypted: bool = True,
    ) -> None:
        self.checksum = checksum
        self.size = size
        self.files = files
        self.tarball = tarball
        self.link = link
        self.disk_name = disk_name
        self.encrypted = encrypted


class PartMetadata(Slotted):
    """
    Backup metadata for ClickHouse data part.
    """

    __slots__ = "database", "table", "name", "raw_metadata"

    # pylint: disable=too-many-arguments, too-many-function-args
    def __init__(
        self,
        database: str,
        table: str,
        name: str,
        checksum: str,
        size: int,
        files: Sequence[str],
        tarball: bool,
        link: str = None,
        disk_name: str = None,
        encrypted: bool = True,
    ) -> None:
        self.database: str = database
        self.table: str = table
        self.name: str = name
        self.raw_metadata: RawMetadata = RawMetadata(
            checksum, size, files, tarball, link, disk_name, encrypted
        )

    @property
    def checksum(self) -> str:
        """
        Return data part checksum.
        """
        return self.raw_metadata.checksum

    @property
    def size(self) -> int:
        """
        Return data part size.
        """
        return self.raw_metadata.size

    @property
    def files(self) -> Sequence[str]:
        """
        Return data part files.
        """
        return self.raw_metadata.files

    @property
    def link(self) -> Optional[str]:
        """
        For deduplicated data parts it returns link to the source backup (its path). Otherwise None is returned.
        """
        return self.raw_metadata.link

    @property
    def disk_name(self) -> str:
        """
        Return disk name where part is stored.
        """
        return self.raw_metadata.disk_name if self.raw_metadata.disk_name else "default"

    @property
    def encrypted(self) -> bool:
        """
        Return True if part is encrypted
        """
        return self.raw_metadata.encrypted

    @property
    def tarball(self) -> bool:
        """
        Returns true if part files stored as single tarball.
        """
        return self.raw_metadata.tarball

    @classmethod
    def load(
        cls, db_name: str, table_name: str, part_name: str, raw_metadata: dict
    ) -> "PartMetadata":
        """
        Deserialize data part metadata.
        """
        return cls(
            database=db_name,
            table=table_name,
            name=part_name,
            checksum=raw_metadata["checksum"],
            size=raw_metadata["bytes"],
            files=raw_metadata["files"],
            tarball=raw_metadata.get("tarball", False),
            link=raw_metadata["link"],
            disk_name=raw_metadata.get("disk_name", "default"),
            encrypted=raw_metadata.get("encrypted", True),
        )

    @classmethod
    def from_frozen_part(
        cls, frozen_part: FrozenPart, encrypted: bool
    ) -> "PartMetadata":
        """
        Converts FrozenPart to PartMetadata.
        """
        return cls(
            database=frozen_part.database,
            table=frozen_part.table,
            name=frozen_part.name,
            checksum=frozen_part.checksum,
            size=frozen_part.size,
            files=frozen_part.files,
            tarball=True,
            disk_name=frozen_part.disk_name,
            encrypted=encrypted,
        )
