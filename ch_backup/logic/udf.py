"""
Clickhouse backup logic for UDFs
"""
from typing import Any, List

from ch_backup import logging

from ch_backup.backup.metadata import BackupMetadata
from ch_backup.logic.backup_manager import BackupManager


class UDFBackup(BackupManager):
    """
    UDF backup class
    """
    @staticmethod
    def get_udf_list(backup_meta: BackupMetadata) -> List[str]:
        """
        Get UDF list
        """
        return backup_meta.get_udf()

    def backup(self, **kwargs: Any) -> None:
        if not self._ch_ctl.ch_version_ge('21.11'):
            return
        udf = self._ch_ctl.get_udf_query()
        for udf_name in udf.keys():
            kwargs['backup_meta'].add_udf(udf_name)

        logging.debug('Performing UDF backup for: %s', ' ,'.join(udf.keys()))
        for udf_name, udf_statement in udf.items():
            self._backup_layout.upload_udf(kwargs['backup_meta'].name, udf_name, udf_statement)

    def restore(self, backup_meta: BackupMetadata, **kwargs: Any) -> None:
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
