"""
Clickhouse backup logic for access
"""
from os.path import join
from typing import Any, Iterator, Sequence

from ch_backup.backup_context import BackupContext
from ch_backup.logic.backup_manager import BackupManager

CH_MARK_FILE = 'need_rebuild_lists.mark'


class AccessBackup(BackupManager):
    """
    Access backup class
    """
    def backup(self, context: BackupContext, **kwargs: Any) -> None:
        """
        Backup access rights
        """
        if kwargs['backup_access_control'] or context.config.get('backup_access_control'):
            self._backup(context)

    def restore(self, context: BackupContext) -> None:
        """
        Restore access rights
        """
        objects = context.backup_meta.get_access_control()
        for name in _get_access_control_files(objects):
            context.backup_layout.download_access_control_file(context.backup_meta.name, name)
        self._mark_to_rebuild(context)

    def _backup(self, context: BackupContext) -> None:
        """
        Backup method
        """
        objects = context.ch_ctl.get_access_control_objects()
        context.backup_meta.set_access_control(objects)
        for name in _get_access_control_files(objects):
            context.backup_layout.upload_access_control_file(context.backup_meta.name, name)

    def _mark_to_rebuild(self, context: BackupContext) -> None:
        """
        Creates special mark file to rebuild the lists.
        """
        mark_file = join(context.ch_ctl_conf['access_control_path'], CH_MARK_FILE)
        with open(mark_file, 'a', encoding='utf-8'):
            pass


def _get_access_control_files(objects: Sequence[str]) -> Iterator[str]:
    """
    Return list of file to be backuped/restored .
    """
    return map(lambda obj: f'{obj}.sql', objects)
