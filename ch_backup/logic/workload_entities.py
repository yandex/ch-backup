"""
Clickhouse backup logic for workload entities (WORKLOADs and RESOURCEs)
"""

import os
import posixpath
import re
from dataclasses import dataclass, field
from enum import Enum

from ch_backup import logging
from ch_backup.backup_context import BackupContext
from ch_backup.clickhouse.config import ClickhouseConfig
from ch_backup.clickhouse.models import WorkloadEntityType
from ch_backup.logic.backup_manager import BackupManager
from ch_backup.util import (
    chown_dir_contents,
    copy_directory_content,
    ensure_owned_directory,
    escape_metadata_file_name,
    temp_directory,
)
from ch_backup.zookeeper.zookeeper import ZookeeperCTL


@dataclass
class WorkloadEntity:
    """
    Class representing workload entity.
    """

    name: str
    type: WorkloadEntityType
    create_statement: str
    parent: str | None = None

    def filename_on_disk(self) -> str:
        """
        Entities have 'workload_' or 'resource_' prefix in file name.
        """
        return f"{self.type.value}_{escape_metadata_file_name(self.name)}.sql"

    @classmethod
    def from_create_statement(cls, create_statement: str) -> "WorkloadEntity":
        """
        Create WorkloadEntity from a CREATE WORKLOAD / CREATE RESOURCE statement.

        Supported forms:
            CREATE RESOURCE <name> (...)[;]
            CREATE WORKLOAD <name> [IN <parent>] [SETTINGS ...][;]
        """
        _name = r"(`[^`]+`|[^\s;]+)"
        workload_re = re.compile(
            rf"^\s*CREATE\s+WORKLOAD\s+{_name}"
            rf"(?:\s+IN\s+{_name})?"
            r"(?:\s+SETTINGS\s+.*)?\s*;?$",
            re.IGNORECASE | re.DOTALL,
        )
        resource_re = re.compile(
            rf"^\s*CREATE\s+RESOURCE\s+{_name}\s*\(.*\)\s*;?$",
            re.IGNORECASE | re.DOTALL,
        )

        m = workload_re.match(create_statement)
        if m:
            name = m.group(1).strip("`")
            parent = m.group(2).strip("`") if m.group(2) else None
            return cls(
                name=name,
                type=WorkloadEntityType.WORKLOAD,
                create_statement=create_statement,
                parent=parent,
            )

        m = resource_re.match(create_statement)
        if m:
            name = m.group(1).strip("`")
            return cls(
                name=name,
                type=WorkloadEntityType.RESOURCE,
                create_statement=create_statement,
                parent=None,
            )

        raise RuntimeError(
            f"Cannot parse workload entity from statement: {create_statement!r}",
        )


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

        workload_entities: list[tuple[str, WorkloadEntityType]] = (
            context.ch_ctl.get_workload_entities_query()
        )

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
            for entity_name, _ in workload_entities:
                context.backup_meta.add_workload_entity(entity_name)

            we_config = WorkloadEntitiesStorageConfig.from_ch_config(
                context.ch_ctl_conf, context.ch_config
            )
            if we_config.is_local_storage():
                copy_directory_content(we_config.storage_path, backup_tmp_path)
            elif we_config.is_storage_zookeeper():
                self._write_entities_from_zookeeper_node(
                    context.zk_ctl,
                    we_config.storage_path,
                    backup_tmp_path,
                )

            chown_dir_contents(user, group, backup_tmp_path)

            for entity_name, entity_type in workload_entities:
                local_path = os.path.join(
                    backup_tmp_path,
                    WorkloadEntity(
                        name=entity_name, type=entity_type, create_statement=""
                    ).filename_on_disk(),
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

        we_list = context.backup_meta.get_workload_entities()
        if not we_list:
            return

        logging.info("Restoring workload entities: {}", ", ".join(we_list))

        we_on_clickhouse = dict(context.ch_ctl.get_workload_entities_query())

        resources_from_backup: list[WorkloadEntity] = []
        workloads_from_backup: list[WorkloadEntity] = []

        for entity_name in we_list:
            statement = context.backup_layout.get_workload_entity_create_statement(
                context.backup_meta, entity_name
            )

            entity = WorkloadEntity.from_create_statement(statement)

            if entity.type == WorkloadEntityType.RESOURCE:
                resources_from_backup.append(entity)
            else:
                workloads_from_backup.append(entity)

        sorted_workloads = self.topologically_sort_workload_entities(
            workloads_from_backup
        )

        for entity in [*resources_from_backup, *sorted_workloads]:
            self._restore_entity_with_collision_check(context, entity, we_on_clickhouse)

        logging.info("All workload entities restored")

    @staticmethod
    def _restore_entity_with_collision_check(
        context: BackupContext,
        entity: "WorkloadEntity",
        existing: dict,
    ) -> None:
        """
        Restore a single workload entity, handling collisions with existing entities on server.

        - If name collision with different type: raise RuntimeError.
        - If name collision with same type and same create statement: skip.
        - If name collision with same type but different create statement: drop + warning + restore.
        """
        if entity.name in existing:
            existing_type = existing[entity.name]
            if existing_type != entity.type:
                raise RuntimeError(
                    f"Cannot restore workload entity '{entity.name}': "
                    f"type mismatch (backup has {entity.type.value!r}, "
                    f"server has {existing_type.value!r})"
                )

            existing_statement = (
                context.backup_layout.get_local_workload_entity_create_statement(
                    entity.name
                )
            )
            if existing_statement == entity.create_statement:
                logging.debug(
                    "Workload entity {} already exists with identical create statement, skipping",
                    entity.name,
                )
                return

            logging.warning(
                "Workload entity {} already exists with a different create statement, "
                "dropping and re-creating",
                entity.name,
            )
            WorkloadEntitiesBackup._drop_workload_entity(context, entity)

        context.ch_ctl.restore_workload_entity(entity.create_statement)

    @staticmethod
    def topologically_sort_workload_entities(
        workload_entities: list[WorkloadEntity],
    ) -> list[WorkloadEntity]:
        """
        Sort WORKLOAD entities create them in the right order.
        """
        workload_map = {entity.name: entity for entity in workload_entities}
        result = []
        visited = set()

        def _walk_to_parents(entity: WorkloadEntity) -> None:
            if entity.name in visited:
                return
            visited.add(entity.name)

            parent = entity.parent
            if parent is not None:
                if parent not in workload_map:
                    raise RuntimeError(
                        f"Parent {parent} for workload {entity.name} doesn't exist in backup"
                    )
                _walk_to_parents(workload_map[parent])

            result.append(entity)

        for workload in workload_entities:
            _walk_to_parents(workload)

        return result

    @staticmethod
    def _drop_workload_entity(context: BackupContext, entity: "WorkloadEntity") -> None:
        """
        Drop a workload entity using its type to choose the correct DROP statement.
        """
        if entity.type == WorkloadEntityType.WORKLOAD:
            context.ch_ctl.drop_workload(entity.name)
        elif entity.type == WorkloadEntityType.RESOURCE:
            context.ch_ctl.drop_resource(entity.name)

    def _write_entities_from_zookeeper_node(
        self,
        zk_ctl: ZookeeperCTL,
        from_path: str,
        to_path_dir: str,
    ) -> None:
        """
        All entities are stored in a single node. Read them and write to separate files to match local storage layout.
        """
        if posixpath.isabs(from_path):
            from_path = from_path[1:]

        with zk_ctl.zk_client as client:
            from_path = posixpath.normpath(
                posixpath.join(zk_ctl.zk_root_path, from_path)
            )

            if not client.exists(from_path):
                return

            # Layout example:
            # CREATE RESOURCE s3_write (WRITE DISK s3);
            # CREATE RESOURCE s3_read (READ DISK s3);
            # CREATE WORKLOAD `all` SETTINGS max_bytes_per_second = 2147483648;
            we_create_statements_binary, _ = client.get(from_path)
            we_create_statements: list[str] = we_create_statements_binary.decode(
                "utf-8"
            ).splitlines()

            for statement in we_create_statements:
                entity = WorkloadEntity.from_create_statement(statement)
                subpath_to = os.path.join(to_path_dir, entity.filename_on_disk())
                if not os.path.exists(subpath_to):
                    with open(subpath_to, "w", encoding="utf-8") as f:
                        f.write(statement)


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
        ZOOKEEPER = "zookeeper"

    storage_type: StorageType = field(default=StorageType.LOCAL)
    storage_path: str = field(default="/")

    def is_local_storage(self) -> bool:
        """
        Determines if config using local filesystem for storage.
        """
        return self.storage_type == self.StorageType.LOCAL

    def is_storage_zookeeper(self) -> bool:
        """
        Determines if config using zookeeper for storage.
        """
        return self.storage_type == self.StorageType.ZOOKEEPER

    @classmethod
    def from_ch_config(
        cls, ch_backup_config: dict, ch_config: ClickhouseConfig
    ) -> "WorkloadEntitiesStorageConfig":
        """
        Create WorkloadEntitiesStorageConfig from ClickhouseConfig.
        """
        we_config = ch_config.config.get("workload_zookeeper_path")
        if we_config:
            return cls(storage_path=we_config, storage_type=cls.StorageType.ZOOKEEPER)
        we_config = ch_config.config.get("workload_path")
        if we_config:
            return cls(storage_path=we_config, storage_type=cls.StorageType.LOCAL)
        return cls(
            storage_path=ch_backup_config.get("workload_path", ""),
            storage_type=cls.StorageType.LOCAL,
        )
