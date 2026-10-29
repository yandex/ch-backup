"""
Backup metadata for Cloud Storage.
"""

from typing import Any, TypedDict


class CloudStorageSource(TypedDict):
    """Recorded cloud location. A null endpoint selects the AWS default."""

    endpoint_url: str | None
    bucket: str
    path: str


class CloudStorageMetadata:
    """
    Backup metadata for Cloud Storage.
    """

    def __init__(
        self,
        encryption: bool = True,
        compression: bool = True,
        data_copied: bool = False,
        disks: list[str] | None = None,
        source: CloudStorageSource | None = None,
    ) -> None:
        self._encryption: bool = encryption
        self._compression: bool = compression
        self._data_copied: bool = data_copied
        self._disks: list[str] = disks or []
        self._source: CloudStorageSource | None = source

    @property
    def enabled(self) -> bool:
        """
        Return True if Cloud Storage is enabled within the backup.
        """
        return len(self._disks) > 0

    @property
    def disks(self) -> list[str]:
        """
        Return list of backed up disks names.
        """
        return self._disks

    def add_disk(self, disk_name: str) -> None:
        """
        Add disk name in backed up disks list.
        """
        if disk_name not in self._disks:
            self._disks.append(disk_name)

    def retain_disks(self, disk_names: set[str]) -> None:
        """
        Drop the disks the backup holds no data on from the backed up disks list.
        """
        self._disks = [disk for disk in self._disks if disk in disk_names]

    @property
    def encrypted(self) -> bool:
        """
        Return True if Cloud Storage backup is encrypted.
        """
        return self._encryption

    @encrypted.setter
    def encrypted(self, value: bool) -> None:
        """
        Set whether Cloud Storage backup is encrypted.
        """
        self._encryption = value

    @property
    def compressed(self) -> bool:
        """
        Return True if Cloud Storage backup is compressed.
        """
        return self._compression

    @compressed.setter
    def compressed(self, value: bool) -> None:
        """
        Set whether Cloud Storage backup is compressed.
        """
        self._compression = value

    @property
    def data_copied(self) -> bool:
        """
        Return True if Cloud Storage data is copied into the backup.
        """
        return self._data_copied

    @data_copied.setter
    def data_copied(self, value: bool) -> None:
        """
        Set whether Cloud Storage data is copied into the backup.
        """
        self._data_copied = value

    @property
    def requires_source_bucket(self) -> bool:
        """
        Return True if restore needs the bucket of the source installation.
        """
        return self.enabled and not self._data_copied

    @property
    def source(self) -> CloudStorageSource | None:
        """
        Return location of cloud storage data (keys "endpoint_url", "bucket" and "path")
        if it was copied out of the source disk, or None if the data is stored on the source disk.
        """
        return self._source

    @source.setter
    def source(self, value: CloudStorageSource | None) -> None:
        """
        Set location of cloud storage data.
        """
        self._source = value

    @classmethod
    def load(cls, data: dict[str, Any]) -> "CloudStorageMetadata":
        """
        Deserialize Cloud Storage metadata.
        """
        return cls(
            encryption=data.get("encryption", True),
            compression=data.get("compression", False),
            data_copied=data.get("data_copied", False),
            disks=data.get("disks", []),
            source=data.get("source"),
        )

    def dump(self) -> dict[str, Any]:
        """
        Serialize Cloud Storage metadata.
        """
        result: dict[str, Any] = {
            "encryption": self._encryption,
            "compression": self._compression,
            "data_copied": self._data_copied,
            "disks": self._disks,
        }
        if self._source:
            result["source"] = self._source
        return result
