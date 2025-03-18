"""
Clickhouse backup logic for named collections
"""

from typing import List

from ch_backup import logging
from ch_backup.backup_context import BackupContext
from ch_backup.clickhouse.models import EncryptedFile
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

        self._validate_config(context)

        nc = context.ch_ctl.get_named_collections_query()
        for nc_name in nc:
            context.backup_meta.add_named_collection(nc_name)

        nc_config = context.ch_config.config.get("named_collections_storage")

        if not nc_config or nc_config.get("type") == "local":
            self._upload_from_local_filesystem(context, nc)

            return

        self._upload_from_zookeeper_encrypted(context, nc)

    def _upload_from_local_filesystem(
        self, context: BackupContext, named_collection_names: List[str]
    ) -> None:
        logging.debug(
            "Performing named collections backup from zookeeper for: {}",
            " ,".join(named_collection_names),
        )

        for nc_name in named_collection_names:
            context.backup_layout.upload_named_collections_ddl_from_file(
                context.backup_meta.name, nc_name
            )

    def _upload_from_zookeeper_encrypted(
        self, context: BackupContext, named_collection_names: List[str]
    ) -> None:
        logging.debug(
            "Performing named collections backup from local filesystem for: {}",
            " ,".join(named_collection_names),
        )

        nc_config = context.ch_config.config.get("named_collections_storage")

        assert nc_config

        nc_storage_path = nc_config.get("path")
        nc_storage_key_hex = nc_config.get("key_hex")

        with context.zk_ctl.zk_client as zk_client:
            for nc_name in named_collection_names:
                path = f"{context.zk_ctl.zk_root_path}{nc_storage_path}{nc_name}.sql"
                data, _ = zk_client.get(path)
                encrypted_file = EncryptedFile(data)

                decrypted_file = context.ch_ctl.decrypt_aes_128_ctr_encrypted_file(
                    encrypted_file, nc_storage_key_hex
                )

                context.backup_layout.upload_named_collections_ddl_data(
                    context.backup_meta.name, nc_name, decrypted_file
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

    def _validate_config(self, context: BackupContext) -> None:
        nc_config = context.ch_config.config.get("named_collections_storage")

        if not nc_config or nc_config.get("type") == "local":
            return

        config_type = nc_config.get("type")
        config_algorithm = nc_config.get("algorithm")

        assert (
            config_type == "zookeeper_encrypted"
        ), f"only 'local' and 'zookeeper_encrypted' named_collections_storage types supported, given: {config_type}"

        assert (
            config_algorithm == "aes_128_ctr"
        ), f"only 'aes_128_ctr' named_collections_storage algorithm is supported, given: {config_algorithm}"

    @staticmethod
    def get_named_collections_list(context: BackupContext) -> List[str]:
        """
        Get named collections list
        """
        return context.backup_meta.get_named_collections()
