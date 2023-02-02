"""
Clickhouse backup logic for UDFs
"""
from typing import List

from ch_backup import logging
from ch_backup.backup_context import BackupContext
from ch_backup.logic.backup_manager import BackupManager


class UDFBackup(BackupManager):
    """
    UDF backup class
    """
    def backup(self, context: BackupContext) -> None:
        """
        Backup UDF objects.
        """
        if not context.ch_ctl.ch_version_ge('21.11'):
            return
        udf = context.ch_ctl.get_udf_query()
        for udf_name in udf.keys():
            context.backup_meta.add_udf(udf_name)

        logging.debug('Performing UDF backup for: %s', ' ,'.join(udf.keys()))
        for udf_name, udf_statement in udf.items():
            context.backup_layout.upload_udf(context.backup_meta.name, udf_name, udf_statement)

    def restore(self, context: BackupContext) -> None:
        """
        Restore UDF objects.
        """
        if not context.ch_ctl.ch_version_ge('21.11'):
            return

        udf_list = self.get_udf_list(context)
        if not udf_list:
            return

        logging.info('Restoring UDFs: %s', ' ,'.join(udf_list))

        udf_on_clickhouse = context.ch_ctl.get_udf_query()
        udf_on_clickhouse_list = udf_on_clickhouse.keys()

        for udf_name in udf_list:
            logging.debug('Restoring UDF %s', udf_name)

            statement = context.backup_layout.get_udf_create_statement(context.backup_meta, udf_name)

            if udf_name in udf_on_clickhouse_list and udf_on_clickhouse[udf_name] != statement:
                context.ch_ctl.drop_udf(udf_name)
                context.ch_ctl.restore_udf(statement)

            if udf_name not in udf_on_clickhouse_list:
                context.ch_ctl.restore_udf(statement)

            logging.debug('UDF %s restored', udf_name)

        logging.info('All UDFs restored')

    @staticmethod
    def get_udf_list(context: BackupContext) -> List[str]:
        """
        Get UDF list
        """
        return context.backup_meta.get_udf()
