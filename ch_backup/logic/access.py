"""
Clickhouse backup logic for access
"""
from itertools import chain
from os.path import exists, join
from time import sleep
from typing import Any, Sequence

from ch_backup import logging
from ch_backup.backup.layout import BackupLayout
from ch_backup.backup.metadata import BackupMetadata
from ch_backup.clickhouse.control import ClickhouseCTL
from ch_backup.config import Config
from ch_backup.logic.backup_manager import BackupManager


class AccessBackup(BackupManager):
    """
    Access backup class
    """
    def __init__(self, ch_ctl: ClickhouseCTL, backup_layout: BackupLayout, config: Config) -> None:
        super().__init__(ch_ctl, backup_layout)
        self._config = config['backup']
        self._ch_ctl_conf = config['clickhouse']

    def backup(self, **kwargs: Any) -> None:
        if kwargs['backup_access_control'] or self._config.get('backup_access_control'):
            self._backup(kwargs['backup_meta'])

    def restore(self, backup_meta: BackupMetadata, **kwargs: Any) -> None:
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
        # We wait 10 minutes. Then if file stucks we make backup anyway.
        max_iterations = 600
        while max_iterations > 0 and exists(mark_file):
            logging.debug(f'Waiting for clickhouse rebuild access control lists. File "{mark_file}".')
            sleep(1)
            max_iterations -= 1

        if exists(mark_file):
            self._backup_layout.upload_access_control_file(backup_meta.name, 'need_rebuild_lists.mark')

        for name in _get_access_control_files(objects):
            self._backup_layout.upload_access_control_file(backup_meta.name, name)


def _get_access_control_files(objects: Sequence[str]) -> chain:
    """
    Return list of file to be backuped/restored .
    """
    lists = ['users.list', 'roles.list', 'quotas.list', 'row_policies.list', 'settings_profiles.list']
    return chain(lists, map(lambda obj: f'{obj}.sql', objects))
