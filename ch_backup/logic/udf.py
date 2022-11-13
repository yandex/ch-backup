"""
UDF management module.
"""

from typing import List

from ch_backup import logging

from ch_backup.backup.layout import BackupLayout
from ch_backup.backup.metadata import BackupMetadata
from ch_backup.clickhouse.control import ClickhouseCTL


class UDFBackup:
    """
    UDF backup manager.
    """
    def __init__(self, ch_ctl: ClickhouseCTL, backup_layout: BackupLayout) -> None:
        self._ch_ctl = ch_ctl
        self._backup_layout = backup_layout

    @staticmethod
    def get_udf_list(backup_meta: BackupMetadata) -> List[str]:
        """
        Get list of UDFs.
        """
        return backup_meta.get_udf()

    def backup(self, backup_meta: BackupMetadata) -> None:
        """
        Backup UDFs.
        """
        if not self._ch_ctl.ch_version_ge('21.11'):
            return
        udf = self._ch_ctl.get_udf_query()
        for udf_name in udf.keys():
            backup_meta.add_udf(udf_name)

        logging.debug('Performing UDF backup for: %s', ' ,'.join(udf.keys()))
        for udf_name, udf_statement in udf.items():
            self._backup_layout.upload_udf(backup_meta.name, udf_name, udf_statement)

    def restore(self, backup_meta: BackupMetadata) -> None:
        """
        Restore UDFs.
        """
        if not self._ch_ctl.ch_version_ge('21.11'):
            return
        udf_list = self.get_udf_list(backup_meta)
        udf_on_clickhouse = self._ch_ctl.get_udf_query()
        udf_on_clickhouse_list = udf_on_clickhouse.keys()
        logging.debug('Restoring UDFs: %s', ' ,'.join(udf_list))
        for udf_name in udf_list:
            statement = self._backup_layout.get_udf_create_statement(backup_meta, udf_name)

            if udf_name in udf_on_clickhouse_list and udf_on_clickhouse[udf_name] != statement:
                self._ch_ctl.drop_udf(udf_name)
                self._ch_ctl.restore_udf(statement)

            if udf_name not in udf_on_clickhouse_list:
                self._ch_ctl.restore_udf(statement)
