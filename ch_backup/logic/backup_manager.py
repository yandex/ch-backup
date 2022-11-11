"""
Clickhouse backup logic abstract class
"""
from abc import ABC, abstractmethod
from typing import Any

from ch_backup.backup.layout import BackupLayout
from ch_backup.backup.metadata import BackupMetadata
from ch_backup.clickhouse.control import ClickhouseCTL


class BackupManager(ABC):
    """
    Backup manager abstract class
    """
    def __init__(self, ch_ctl: ClickhouseCTL, backup_layout: BackupLayout) -> None:
        self._ch_ctl = ch_ctl
        self._backup_layout = backup_layout

    @abstractmethod
    def backup(self, **kwargs: Any) -> None:  # type: ignore
        """
        Abstract method for backup
        """
        pass

    @abstractmethod
    def restore(self, backup_meta: BackupMetadata, **kwargs: Any) -> None:  # type: ignore
        """
        Abstract method for restore
        """
        pass
