"""
Clickhouse backup logic for named collections
"""

import os
import posixpath
import re
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import List, Optional

from kazoo.exceptions import NoNodeError

from ch_backup import logging
from ch_backup.backup_context import BackupContext
from ch_backup.clickhouse.config import ClickhouseConfig
from ch_backup.clickhouse.encryption import ClickHouseEncryption, EncryptedFile
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
        nc_config = NamedCollectionsStorageConfig.from_ch_config(
            context.ch_ctl_conf, context.ch_config
        )

        for nc_name in nc_list:
            logging.debug("Restoring named collection {}", nc_name)

            statement = context.backup_layout.get_named_collection_create_statement(
                context.backup_meta, nc_name
            )

            if nc_name in nc_on_clickhouse_list:
                nc_on_clickhouse_statement = self._get_existing_create_statement(
                    context, nc_config, nc_name
                )
                if self._normalize_create_statement(
                    nc_on_clickhouse_statement
                ) == self._normalize_create_statement(statement):
                    logging.debug(
                        "Named collection {} already exists with identical create "
                        "statement, skipping",
                        nc_name,
                    )
                    continue

                logging.warning(
                    "Named collection {} already exists with a different create "
                    "statement, dropping and re-creating",
                    nc_name,
                )
                context.ch_ctl.drop_named_collection(nc_name)

            if context.ch_ctl.ch_version_ge("23.8"):
                statement = self._add_if_not_exists(statement)

            context.ch_ctl.restore_named_collection(statement)

            logging.debug("Named collection {} restored", nc_name)

        logging.info("All named collections restored")

    @staticmethod
    def get_named_collections_list(context: BackupContext) -> List[str]:
        """
        Get named collections list
        """
        return context.backup_meta.get_named_collections()

    @staticmethod
    def _add_if_not_exists(statement: str) -> str:
        """
        Add IF NOT EXISTS to a CREATE NAMED COLLECTION statement.
        """
        if re.match(
            r"^\s*CREATE\s+NAMED\s+COLLECTION\s+IF\s+NOT\s+EXISTS\b",
            statement,
            flags=re.IGNORECASE,
        ):
            return statement

        result, replacements = re.subn(
            r"^(\s*CREATE\s+NAMED\s+COLLECTION\s+)",
            r"\1IF NOT EXISTS ",
            statement,
            count=1,
            flags=re.IGNORECASE,
        )
        if replacements == 0:
            raise RuntimeError(
                "Expected CREATE NAMED COLLECTION statement, got: " f"{statement!r}"
            )
        return result

    @staticmethod
    def _normalize_create_statement(statement: Optional[str]) -> Optional[str]:
        """
        Normalize non-semantic CREATE NAMED COLLECTION modifiers.

        ClickHouse persists IF NOT EXISTS in the collection DDL, even though it
        only changes CREATE behavior. Ignore it when comparing a collection with
        the backup statement.
        """
        if statement is None:
            return None
        return re.sub(
            r"^(\s*CREATE\s+NAMED\s+COLLECTION\s+)IF\s+NOT\s+EXISTS\s+",
            r"\1",
            statement,
            count=1,
            flags=re.IGNORECASE,
        )

    @staticmethod
    def _get_existing_create_statement(
        context: BackupContext,
        nc_config: "NamedCollectionsStorageConfig",
        nc_name: str,
    ) -> Optional[str]:
        """
        Read a collection DDL from its configured storage.
        """
        filename = f"{escape_metadata_file_name(nc_name)}.sql"

        if nc_config.is_local_storage():
            try:
                data = Path(os.path.join(nc_config.storage_path, filename)).read_bytes()
            except OSError as err:
                logging.debug(
                    'Cannot load a create statement of the named collection "{}": {}',
                    nc_name,
                    str(err),
                )
                return None
        else:
            storage_path = nc_config.storage_path.lstrip("/")
            zk_path = posixpath.normpath(
                posixpath.join(context.zk_ctl.zk_root_path, storage_path, filename)
            )
            try:
                with context.zk_ctl.zk_client as client:
                    data, _ = client.get(zk_path)
            except NoNodeError:
                logging.debug(
                    'Cannot load a create statement of the named collection "{}": '
                    "ZooKeeper node {} does not exist",
                    nc_name,
                    zk_path,
                )
                return None

        if nc_config.is_encrypted():
            return EncryptedFile(data).get_decrypted_data(
                context.ch_ctl.decrypt_aes_ctr,
                nc_config.encryption_key_hex,
            )
        return data.decode("utf-8")

    def _copy_directory_content_from_zookeeper(
        self,
        zk_ctl: ZookeeperCTL,
        from_path_dir: str,
        to_path_dir: str,
    ) -> None:
        """
        Copy all files from zookeeper directory to destination.
        """
        if posixpath.isabs(from_path_dir):
            from_path_dir = from_path_dir[1:]

        with zk_ctl.zk_client as client:
            target_dir = posixpath.normpath(
                posixpath.join(zk_ctl.zk_root_path, from_path_dir)
            )
            children_names = client.get_children(path=target_dir)

            for child_name in children_names:
                subpath_from = posixpath.join(target_dir, child_name)
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
