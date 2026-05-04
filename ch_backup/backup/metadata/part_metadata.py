"""
Backup metadata for ClickHouse data part.
"""

import os
from typing import Optional, Sequence

from ch_backup.clickhouse.models import FrozenPart
from ch_backup.util import Slotted


def normalize_backup_link(raw_link: Optional[str]) -> Optional[str]:
    """
    Normalize the ``link`` field to a plain backup name.

    The serialized ``link`` field is intentionally stored as a full storage
    path (legacy format) so that older ch-backup versions can still read
    backups produced by this code.  In-memory we always work with a plain
    backup name; this helper performs the conversion at load time.

    ``os.path.basename`` handles both formats transparently:
    - ``"20181017T210300"``                 → ``"20181017T210300"`` (new)
    - ``"ch_backup/20181017T210300"``       → ``"20181017T210300"`` (legacy)
    - ``"/srv/backups/20181017T210300/"``   → ``"20181017T210300"`` (legacy)

    Returns ``None`` for falsy values (``None``, empty string).
    """
    if not raw_link:
        return None
    return os.path.basename(raw_link.rstrip("/"))


class RawMetadata(Slotted):
    """
    Raw metadata for ClickHouse data part.
    """

    __slots__ = "checksum", "size", "files", "tarball", "link", "disk_name", "encrypted"

    # pylint: disable=too-many-positional-arguments
    def __init__(
        self,
        checksum: str,
        size: int,
        files: Sequence[str],
        tarball: bool,
        link: Optional[str] = None,
        disk_name: Optional[str] = None,
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

    # pylint: disable=too-many-arguments, too-many-positional-arguments
    def __init__(
        self,
        database: str,
        table: str,
        name: str,
        checksum: str,
        size: int,
        files: Sequence[str],
        tarball: bool,
        link: Optional[str] = None,
        disk_name: Optional[str] = None,
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
        For deduplicated data parts returns the name of the source backup.
        For non-deduplicated parts returns None.
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
            link=normalize_backup_link(raw_metadata.get("link")),
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
