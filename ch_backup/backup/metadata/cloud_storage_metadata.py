"""
Backup metadata for Cloud Storage.
"""

from typing import Any, Dict, List, Optional


class CloudStorageMetadata:
    """
    Backup metadata for Cloud Storage.
    """

    def __init__(
        self, encryption: bool = True, disks: Optional[List[str]] = None
    ) -> None:
        self._encryption: bool = encryption
        self._disks: List[str] = disks or []

    @property
    def enabled(self) -> bool:
        """
        Return True if Cloud Storage is enabled within the backup.
        """
        return len(self._disks) > 0

    @property
    def disks(self) -> List[str]:
        """
        Return list of backed up disks names.
        """
        return self._disks

    def add_disk(self, disk_name: str) -> None:
        """
        Add disk name in backed up disks list.
        """
        self._disks.append(disk_name)

    @property
    def encrypted(self) -> bool:
        """
        Return True if Cloud Storage backup is encrypted.
        """
        return self._encryption

    def encrypt(self) -> None:
        """
        Encrypt Cloud Storage data within the backup.
        """
        self._encryption = True

    @classmethod
    def load(cls, data: Dict[str, Any]) -> "CloudStorageMetadata":
        """
        Deserialize Cloud Storage metadata.
        """
        return cls(encryption=data.get("encryption", True), disks=data.get("disks", []))

    def dump(self) -> Dict[str, Any]:
        """
        Serialize Cloud Storage metadata.
        """
        return {
            "encryption": self._encryption,
            "disks": self._disks,
        }
