"""
Clickhouse backup logic for workload entities (WORKLOADs and RESOURCEs)
"""

import re
from dataclasses import dataclass
from typing import Collection

from ch_backup import logging
from ch_backup.backup_context import BackupContext
from ch_backup.clickhouse.config import ClickhouseConfig
from ch_backup.clickhouse.models import WorkloadEntityType
from ch_backup.logic.backup_manager import BackupManager

CONFIG_ENTITY_NAME_RE = re.compile(
    r"\bCREATE\s+(?:OR\s+REPLACE\s+)?(?:WORKLOAD|RESOURCE)\s+"
    r"(?:IF\s+NOT\s+EXISTS\s+)?(`[^`]+`|[^\s;(]+)",
    re.IGNORECASE,
)


@dataclass
class WorkloadEntity:
    """
    Class representing workload entity.
    """

    name: str
    type: WorkloadEntityType
    create_statement: str
    parent: str | None = None

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
        Backup workload entities created by SQL.
        """
        if not context.ch_ctl.ch_version_ge("24.11"):
            # CREATE WORKLOAD and CREATE RESOURCE SQL syntax added in 24.11
            # https://clickhouse.com/docs/en/operations/workload-scheduling
            logging.info(
                "Workload entities are not supported for version less than 24.11"
            )
            return

        from_config = get_config_workload_entity_names(context.ch_config)
        for name, statement in context.ch_ctl.get_workload_entities_query().items():
            if name in from_config:
                logging.info(
                    "Skipping workload entity {} defined in the server config", name
                )
                continue
            context.backup_meta.add_workload_entity(name)
            context.backup_layout.upload_workload_entity(
                context.backup_meta.name, name, statement
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

        we_on_clickhouse = context.ch_ctl.get_workload_entities_query()
        from_config = get_config_workload_entity_names(context.ch_config)

        resources_from_backup: list[WorkloadEntity] = []
        workloads_from_backup: list[WorkloadEntity] = []

        for entity_name in we_list:
            statement = context.backup_layout.get_workload_entity_create_statement(
                context.backup_meta, entity_name
            )

            entity = WorkloadEntity.from_create_statement(statement)

            if entity.name in from_config:
                logging.info(
                    "Skipping workload entity {} defined in the server config",
                    entity.name,
                )
            elif entity.type == WorkloadEntityType.RESOURCE:
                resources_from_backup.append(entity)
            else:
                workloads_from_backup.append(entity)

        sorted_workloads = self.topologically_sort_workload_entities(
            workloads_from_backup, we_on_clickhouse
        )

        for entity in [*resources_from_backup, *sorted_workloads]:
            self._restore_entity_with_collision_check(context, entity, we_on_clickhouse)

        logging.info("All workload entities restored")

    @staticmethod
    def _restore_entity_with_collision_check(
        context: BackupContext,
        entity: "WorkloadEntity",
        existing: dict[str, str],
    ) -> None:
        """
        Restore a single workload entity, handling collisions with existing entities on server.

        - If name collision with different type: raise RuntimeError.
        - If name collision with same type and same create statement: skip.
        - If name collision with same type but different create statement: replace it
          in place, as ClickHouse does not drop entities referenced by others.
        """
        statement = entity.create_statement
        if entity.name in existing:
            existing_type = WorkloadEntity.from_create_statement(
                existing[entity.name]
            ).type
            if existing_type != entity.type:
                raise RuntimeError(
                    f"Cannot restore workload entity '{entity.name}': "
                    f"type mismatch (backup has {entity.type.value!r}, "
                    f"server has {existing_type.value!r})"
                )

            if _normalize(existing[entity.name]) == _normalize(statement):
                logging.debug(
                    "Workload entity {} already exists with identical create statement, skipping",
                    entity.name,
                )
                return

            logging.warning(
                "Workload entity {} already exists with a different create statement, "
                "replacing",
                entity.name,
            )
            statement = re.sub(
                r"^\s*CREATE\s", "CREATE OR REPLACE ", statement, flags=re.IGNORECASE
            )

        context.ch_ctl.restore_workload_entity(statement)

    @staticmethod
    def topologically_sort_workload_entities(
        workload_entities: list[WorkloadEntity],
        existing: Collection[str] = (),
    ) -> list[WorkloadEntity]:
        """
        Sort WORKLOAD entities create them in the right order.
        A parent missing in backup must exist on the server.
        """
        workload_map = {entity.name: entity for entity in workload_entities}
        result = []
        visited = set()

        def _walk_to_parents(entity: WorkloadEntity) -> None:
            if entity.name in visited:
                return
            visited.add(entity.name)

            parent = entity.parent
            if parent in workload_map:
                _walk_to_parents(workload_map[parent])
            elif parent is not None and parent not in existing:
                raise RuntimeError(
                    f"Parent {parent} for workload {entity.name} doesn't exist in backup "
                    "or on the server"
                )

            result.append(entity)

        for workload in workload_entities:
            _walk_to_parents(workload)

        return result


def get_config_workload_entity_names(ch_config: ClickhouseConfig) -> set[str]:
    """
    Return names of workload entities defined in resources_and_workloads of the server
    config. ClickHouse shows them with ones created by SQL but does not let replace them.
    """
    statements = ch_config.config.get("resources_and_workloads") or ""
    if isinstance(statements, dict):
        statements = statements.get("#text", "")
    return {name.strip("`") for name in CONFIG_ENTITY_NAME_RE.findall(statements)}


def _normalize(statement: str) -> str:
    """
    Strip whitespace and semicolons that differ between storages of an entity.
    """
    return statement.strip().rstrip(";").strip()
