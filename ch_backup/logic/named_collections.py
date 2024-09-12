"""
Clickhouse backup logic for named collections
"""

from typing import List

from ch_backup import logging
from ch_backup.backup_context import BackupContext
from ch_backup.logic.backup_manager import BackupManager


class NamedCollectionsBackup(BackupManager):
    """
    Named collections backup class
    """

    def backup(self, context: BackupContext) -> None:
        """
        Backup named collections.
        """
        if not context.ch_ctl.ch_version_ge("22.12"):
            # CREATE NAMED COLLECTION method and system.named_collections table added only in 22.12
            # https://clickhouse.com/docs/en/whats-new/changelog/2022#experimental-feature
            logging.info(
                "Named collections is not supported for version less than 22.12"
            )
            return
        nc = context.ch_ctl.get_named_collections_query()
        for nc_name in nc:
            context.backup_meta.add_named_collection(nc_name)

        logging.debug("Performing named collections backup for: {}", " ,".join(nc))
        for nc_name in nc:
            context.backup_layout.upload_named_collections_create_statement(
                context.backup_meta.name, nc_name
            )

    def restore(self, context: BackupContext) -> None:
        """
        Restore named collections.
        """
        if not context.ch_ctl.ch_version_ge("22.12"):
            # CREATE NAMED COLLECTION method and system.named_collections table added only in 22.12
            # https://clickhouse.com/docs/en/whats-new/changelog/2022#experimental-feature
            logging.info(
                "Named collections is not supported for version less than 22.12"
            )
            return

        nc_list = self.get_named_collections_list(context)
        if not nc_list:
            return

        logging.info("Restoring named collections: {}", " ,".join(nc_list))

        nc_on_clickhouse_list = context.ch_ctl.get_named_collections_query()

        for nc_name in nc_list:
            logging.debug("Restoring named collection {}", nc_name)

            statement = context.backup_layout.get_named_collection_create_statement(
                context.backup_meta, nc_name
            )

            if nc_name in nc_on_clickhouse_list:
                nc_on_clickhouse_statement = (
                    context.backup_layout.get_local_nc_create_statement(nc_name)
                )
                if nc_on_clickhouse_statement != statement:
                    context.ch_ctl.drop_named_collection(nc_name)
                    context.ch_ctl.restore_named_collection(statement)

            if nc_name not in nc_on_clickhouse_list:
                context.ch_ctl.restore_named_collection(statement)

            logging.debug("Named collection {} restored", nc_name)

        logging.info("All named collections restored")

    @staticmethod
    def get_named_collections_list(context: BackupContext) -> List[str]:
        """
        Get named collections list
        """
        return context.backup_meta.get_named_collections()
