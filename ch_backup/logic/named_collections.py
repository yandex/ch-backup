"""
Clickhouse backup logic for named collections
"""

import os
import posixpath
from dataclasses import dataclass, field
from enum import Enum
from typing import List

from ch_backup import logging
from ch_backup.backup_context import BackupContext
from ch_backup.clickhouse.config import ClickhouseConfig
from ch_backup.clickhouse.encryption import ClickHouseEncryption
from ch_backup.logic.backup_manager import BackupManager
from ch_backup.util import (
    chown_dir_contents,
    copy_directory_content,
    ensure_owned_directory,
    escape_metadata_file_name,
    temp_directory,
)
from ch_backup.zookeeper.zookeeper import ZookeeperCTL


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

        nc_config = NamedCollectionsStorageConfig.from_ch_config(
            context.ch_ctl_conf, context.ch_config
        )

        nc = context.ch_ctl.get_named_collections_query()

        if len(nc) == 0:
            return

        user = context.ch_ctl_conf["user"]
        group = context.ch_ctl_conf["group"]
        tmp_path = context.ch_ctl_conf["tmp_path"]

        ensure_owned_directory(tmp_path, user, group)

        with temp_directory(
            tmp_path,
            context.backup_meta.name,
        ) as backup_tmp_path:
            for nc_name in nc:
                context.backup_meta.add_named_collection(nc_name)

            if nc_config.is_local_storage():
                copy_directory_content(nc_config.storage_path, backup_tmp_path)
            elif nc_config.is_storage_zookeeper():
                self._copy_directory_content_from_zookeeper(
                    context.zk_ctl,
                    nc_config.storage_path,
                    backup_tmp_path,
                )

            if nc_config.is_encrypted():
                decryptor = ClickHouseEncryption(context.ch_ctl)
                decryptor.decrypt_directory_content(
                    backup_tmp_path,
                    nc_config.encryption_key_hex,
                )

            chown_dir_contents(user, group, backup_tmp_path)

            for nc_name in nc:
                local_path = os.path.join(
                    backup_tmp_path, f"{escape_metadata_file_name(nc_name)}.sql"
                )

                context.backup_layout.upload_named_collections_ddl_from_file(
                    local_path,
                    context.backup_meta.name,
                    nc_name,
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

    def _copy_directory_content_from_zookeeper(
        self,
        zk_ctl: ZookeeperCTL,
        from_path_dir: str,
        to_path_dir: str,
    ) -> None:
        """
        Copy all files from zookeeper directory to destination.
        """
        if to_path_dir[-1] != "/":
            to_path_dir += "/"

        with zk_ctl.zk_client as client:
            target_dir = posixpath.normpath(
                posixpath.join(zk_ctl.zk_root_path, from_path_dir)
            )
            children_names = client.get_children(path=target_dir)

            for child_name in children_names:
                subpath_from = posixpath.join(target_dir / child_name)
                child_data, _ = client.get(subpath_from)
                subpath_to = os.path.join(to_path_dir, child_name)
                if not os.path.exists(subpath_to):
                    with open(subpath_to, "xb") as f:
                        f.write(child_data)


@dataclass
class NamedCollectionsStorageConfig:
    """
    Class representing named collections storage config.
    """

    class StorageType(Enum):
        """
        Named collections storage type.
        """

        LOCAL = "local"
        LOCAL_ENCRYPTED = "local_encrypted"
        ZOOKEEPER = "zookeeper"
        ZOOKEEPER_ENCRYPTED = "zookeeper_encrypted"

    storage_type: StorageType = field(default=StorageType.LOCAL)
    storage_path: str = field(default="/")
    encryption_key_hex: str = field(default="")

    def is_local_storage(self) -> bool:
        """
        Determines if config using local filesystem for storage.
        """
        return self.storage_type in (
            self.StorageType.LOCAL,
            self.StorageType.LOCAL_ENCRYPTED,
        )

    def is_storage_zookeeper(self) -> bool:
        """
        Determines if config using zookeeper for storage.
        """
        return self.storage_type in (
            self.StorageType.ZOOKEEPER,
            self.StorageType.ZOOKEEPER_ENCRYPTED,
        )

    def is_encrypted(self) -> bool:
        """
        Determines if config using encryption.
        """
        return self.storage_type in (
            self.StorageType.LOCAL_ENCRYPTED,
            self.StorageType.ZOOKEEPER_ENCRYPTED,
        )

    @classmethod
    def from_ch_config(
        cls, ch_backup_config: dict, ch_config: ClickhouseConfig
    ) -> "NamedCollectionsStorageConfig":
        """
        Create NamedCollectionsStorageConfig from ClickhouseConfig.
        """
        nc_config = ch_config.config.get("named_collections_storage")
        if not nc_config:
            storage_path = ch_backup_config.get("named_collections_path")

            assert storage_path, "named_collections_path missing from ch-backup config"

            return NamedCollectionsStorageConfig(storage_path=storage_path)

        storage_type_from_config = nc_config.get("type")
        storage_path_from_config = nc_config.get("path")
        encryption_key_hex_from_config = nc_config.get("key_hex")

        storage_type = NamedCollectionsStorageConfig.StorageType.LOCAL
        storage_path = storage_path_from_config
        encryption_key_hex = encryption_key_hex_from_config

        if storage_type_from_config:
            storage_type = cls.StorageType(storage_type_from_config)

        return cls(storage_type, storage_path, encryption_key_hex)
