"""
Clickhouse backup logic for access
"""
from functools import partial
from itertools import chain
from os.path import exists, join
from typing import Sequence

from humanfriendly import parse_timespan

from ch_backup import logging
from ch_backup.backup.layout import BackupLayout
from ch_backup.backup.metadata import BackupMetadata
from ch_backup.clickhouse.control import ClickhouseCTL
from ch_backup.config import Config
from ch_backup.logic.backup_manager import BackupManager
from ch_backup.util import wait_for


class AccessBackup(BackupManager):
    """
    Access backup class
    """
    def __init__(self, ch_ctl: ClickhouseCTL, backup_layout: BackupLayout, config: Config) -> None:
        super().__init__(ch_ctl, backup_layout)
        self._config = config['backup']
        self._ch_ctl_conf = config['clickhouse']

    def backup(self, backup_meta: BackupMetadata, backup_access_control: bool) -> None:
        """
        Backup access control objects: users, settings, etc
        """
        if backup_access_control or self._config.get('backup_access_control', False):
            self._backup(backup_meta)

    def restore(self, backup_meta: BackupMetadata) -> None:
        """
        Restore access control objects.
        """
        objects = backup_meta.get_access_control()
        for name in _get_access_control_files(objects):
            self._backup_layout.download_access_control_file(backup_meta.name, name)

    def _backup(self, backup_meta: BackupMetadata) -> None:
        """
        Backup method
        """
        objects = self._ch_ctl.get_access_control_objects()
        backup_meta.set_access_control(objects)

        # ClickHouse creates file need_rebuild_lists.mark after access management objects modification
        # to show that lists should be updated.
        mark_file = join(self._ch_ctl_conf['access_control_path'], 'need_rebuild_lists.mark')
        # We wait 10 minutes. Then if file stuck we make backup anyway.
        logging.debug(f'Waiting for ClickHouse to rebuild access control lists. File "{mark_file}".')
        wait_for(
            func=partial(exists, mark_file),
            timeout_s=parse_timespan('10m'),
            interval_s=1.,
        )

        if exists(mark_file):
            self._backup_layout.upload_access_control_file(backup_meta.name, 'need_rebuild_lists.mark')

        for name in _get_access_control_files(objects):
            self._backup_layout.upload_access_control_file(backup_meta.name, name)


def _get_access_control_files(objects: Sequence[str]) -> chain:
    """
    Return list of file to be backed up/restored .
    """
    lists = ['users.list', 'roles.list', 'quotas.list', 'row_policies.list', 'settings_profiles.list']
    return chain(lists, map(lambda obj: f'{obj}.sql', objects))
