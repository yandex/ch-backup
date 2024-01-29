"""
Access control metadata.
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Sequence

from ch_backup.backup.metadata.common import BackupStorageFormat
from ch_backup.util import dataclass_from_dict


@dataclass
class AccessControlMetadata:
    """
    Access control metadata.
    """

    acl_ids: List[str] = field(default_factory=list)
    acl_meta: Dict[str, Any] = field(default_factory=dict)
    backup_format: BackupStorageFormat = BackupStorageFormat.TAR

    def __post_init__(self) -> None:
        self.backup_format = BackupStorageFormat(self.backup_format)

    @classmethod
    def from_ch_objects(
        cls, objects: Sequence[Dict[str, Any]]
    ) -> "AccessControlMetadata":
        """
        Create Access Control metadata from objects fetched from ClickHouse.
        """
        acl_ids, acl_meta = [], {}
        for i, item in enumerate(objects):
            acl_ids.append(item["id"])
            acl_meta[str(i)] = {"name": item["name"], "char": item["char"]}

        return cls(acl_ids, acl_meta)

    @classmethod
    def load(cls, data: Dict[str, Any]) -> "AccessControlMetadata":
        """
        Deserialize Access Control metadata.
        """
        return dataclass_from_dict(cls, data)

    def dump(self) -> Dict[str, Any]:
        """
        Serialize Access Control metadata.
        """
        return {
            "acl_ids": self.acl_ids,
            "acl_meta": self.acl_meta,
            "backup_format": self.backup_format.value,
        }
