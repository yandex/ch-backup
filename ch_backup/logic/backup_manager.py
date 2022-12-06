"""
Clickhouse backup logic abstract class
"""

from ch_backup.backup.layout import BackupLayout
from ch_backup.clickhouse.control import ClickhouseCTL


class BackupManager:
    """
    Backup manager abstract class
    """
    def __init__(self, ch_ctl: ClickhouseCTL, backup_layout: BackupLayout) -> None:
        self._ch_ctl = ch_ctl
        self._backup_layout = backup_layout
