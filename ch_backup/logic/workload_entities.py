"""
Clickhouse backup logic for workload entities (WORKLOADs and RESOURCEs)
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


class WorkloadEntitiesBackup(BackupManager):
    """
    Workload entities (WORKLOAD and RESOURCE) backup class
    """

    def backup(self, context: BackupContext) -> None:
        """
        Backup workload entities.
        """
        if not context.ch_ctl.ch_version_ge("24.11"):
            # CREATE WORKLOAD and CREATE RESOURCE SQL syntax added in 24.11
            # https://clickhouse.com/docs/en/operations/workload-scheduling
            logging.info(
                "Workload entities are not supported for version less than 24.11"
            )
            return

        we_config = WorkloadEntitiesStorageConfig.from_ch_config(
            context.ch_ctl_conf, context.ch_config
        )

        workload_entities = context.ch_ctl.get_workload_entities_query()

        if len(workload_entities) == 0:
            return

        user = context.ch_ctl_conf["user"]
        group = context.ch_ctl_conf["group"]
        tmp_path = context.ch_ctl_conf["tmp_path"]

        ensure_owned_directory(tmp_path, user, group)

        with temp_directory(
            tmp_path,
            context.backup_meta.name,
        ) as backup_tmp_path:
            for entity_name in workload_entities:
                context.backup_meta.add_workload_entity(entity_name)

            if we_config.is_local_storage():
                copy_directory_content(we_config.storage_path, backup_tmp_path)
            elif we_config.is_storage_zookeeper():
                self._copy_directory_content_from_zookeeper(
                    context.zk_ctl,
                    we_config.storage_path,
                    backup_tmp_path,
                )

            if we_config.is_encrypted():
                decryptor = ClickHouseEncryption(context.ch_ctl)
                decryptor.decrypt_directory_content(
                    backup_tmp_path,
                    we_config.encryption_key_hex,
                )

            chown_dir_contents(user, group, backup_tmp_path)

            for entity_name in workload_entities:
                local_path = os.path.join(
                    backup_tmp_path, f"{escape_metadata_file_name(entity_name)}.sql"
                )

                context.backup_layout.upload_workload_entity_ddl_from_file(
                    local_path,
                    context.backup_meta.name,
                    entity_name,
                )

    def restore(self, context: BackupContext) -> None:
        """
        Restore workload entities.
        """
        if not context.ch_ctl.ch_version_ge("24.11"):
            # CREATE WORKLOAD and CREATE RESOURCE SQL syntax added in 24.11
            # https://clickhouse.com/docs/en/operations/workload-scheduling
            logging.info(
                "Workload entities are not supported for version less than 24.11"
            )
            return

        we_list = self.get_workload_entities_list(context)
        if not we_list:
            return

        logging.info("Restoring workload entities: {}", " ,".join(we_list))

        we_on_clickhouse_list = context.ch_ctl.get_workload_entities_query()

        for entity_name in we_list:
            logging.debug("Restoring workload entity {}", entity_name)

            statement = context.backup_layout.get_workload_entity_create_statement(
                context.backup_meta, entity_name
            )

            if entity_name in we_on_clickhouse_list:
                we_on_clickhouse_statement = (
                    context.backup_layout.get_local_workload_entity_create_statement(entity_name)
                )
                if we_on_clickhouse_statement != statement:
                    # The entity already on ClickHouse is the one being dropped,
                    # so derive its type from its own create statement when
                    # available, falling back to the backup statement.
                    self._drop_workload_entity(
                        context, entity_name, we_on_clickhouse_statement or statement
                    )
                    context.ch_ctl.restore_workload_entity(statement)

            if entity_name not in we_on_clickhouse_list:
                context.ch_ctl.restore_workload_entity(statement)

            logging.debug("Workload entity {} restored", entity_name)

        logging.info("All workload entities restored")

    @staticmethod
    def get_workload_entities_list(context: BackupContext) -> List[str]:
        """
        Get workload entities list
        """
        return context.backup_meta.get_workload_entities()

    @staticmethod
    def _drop_workload_entity(
        context: BackupContext, entity_name: str, create_statement: str
    ) -> None:
        """
        Drop a workload entity, choosing WORKLOAD or RESOURCE based on its
        create statement (`CREATE WORKLOAD ...` / `CREATE RESOURCE ...`).

        Both patterns are handled explicitly so that an unknown or malformed
        statement fails loudly instead of being silently misclassified as a
        RESOURCE and issuing the wrong DROP.
        """
        normalized_statement = create_statement.lstrip().upper()

        if normalized_statement.startswith("CREATE WORKLOAD"):
            context.ch_ctl.drop_workload(entity_name)
        elif normalized_statement.startswith("CREATE RESOURCE"):
            context.ch_ctl.drop_resource(entity_name)
        else:
            logging.error(
                "Unsupported workload entity create statement for {}: {}",
                entity_name,
                create_statement,
            )
            raise ValueError(
                f"Unsupported workload entity create statement for '{entity_name}': "
                f"{create_statement!r}"
            )

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
class WorkloadEntitiesStorageConfig:
    """
    Class representing workload entities storage config.
    """

    class StorageType(Enum):
        """
        Workload entities storage type.
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
    ) -> "WorkloadEntitiesStorageConfig":
        """
        Create WorkloadEntitiesStorageConfig from ClickhouseConfig.
        """
        we_config = ch_config.config.get("workload_entity_storage")
        if not we_config:
            storage_path = ch_backup_config.get("workload_path")

            assert storage_path, "workload_path missing from ch-backup config"

            return WorkloadEntitiesStorageConfig(storage_path=storage_path)

        storage_type_from_config = we_config.get("type")
        storage_path_from_config = we_config.get("path")
        encryption_key_hex_from_config = we_config.get("key_hex")

        assert (
            storage_path_from_config
        ), "path missing from workload_entity_storage config"

        storage_type = WorkloadEntitiesStorageConfig.StorageType.LOCAL
        storage_path = storage_path_from_config
        encryption_key_hex = encryption_key_hex_from_config

        if storage_type_from_config:
            storage_type = cls.StorageType(storage_type_from_config)

        return cls(storage_type, storage_path, encryption_key_hex)
