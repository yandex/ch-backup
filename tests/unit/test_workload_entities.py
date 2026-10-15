"""Unit tests for workload entity backup logic."""

from types import SimpleNamespace
from typing import cast
from unittest.mock import MagicMock

import pytest

from ch_backup.backup_context import BackupContext
from ch_backup.clickhouse.models import WorkloadEntityType
from ch_backup.logic.workload_entities import WorkloadEntitiesBackup, WorkloadEntity

CONFIG_ENTITIES = """
    CREATE RESOURCE xml_s3_read (READ DISK s3);
    CREATE WORKLOAD xml_all;
    CREATE WORKLOAD xml_production IN xml_all SETTINGS weight = 3;
"""


def _context(
    on_server: dict[str, str], in_backup: dict[str, str] | None = None
) -> SimpleNamespace:
    """Helper: a context of a server with the given entities and entities from config."""
    ch_ctl = MagicMock()
    ch_ctl.ch_version_ge.return_value = True
    ch_ctl.get_workload_entities_query.return_value = on_server
    backup_meta = MagicMock()
    backup_meta.name = "backup"
    backup_meta.get_workload_entities.return_value = list(in_backup or {})
    backup_layout = MagicMock()
    backup_layout.get_workload_entity_create_statement.side_effect = lambda _, name: (
        in_backup or {}
    )[name]
    return SimpleNamespace(
        ch_ctl=ch_ctl,
        ch_config=SimpleNamespace(config={"resources_and_workloads": CONFIG_ENTITIES}),
        backup_meta=backup_meta,
        backup_layout=backup_layout,
    )


def test_backup_takes_sql_entities_without_config_ones() -> None:
    context = _context(
        {
            "xml_all": "CREATE WORKLOAD xml_all",
            "sql workload": "CREATE WORKLOAD `sql workload` IN xml_all",
        }
    )

    WorkloadEntitiesBackup().backup(cast(BackupContext, context))

    context.backup_meta.add_workload_entity.assert_called_once_with("sql workload")
    context.backup_layout.upload_workload_entity.assert_called_once_with(
        "backup", "sql workload", "CREATE WORKLOAD `sql workload` IN xml_all"
    )


def test_restore_skips_same_and_replaces_changed_entities_parents_first() -> None:
    context = _context(
        on_server={
            "xml_all": "CREATE WORKLOAD xml_all",
            "s3_read": "CREATE RESOURCE s3_read (READ DISK s3)",
            "all": "CREATE WORKLOAD `all`",
            "user": "CREATE WORKLOAD user IN `all`",
        },
        in_backup={
            "user": "CREATE WORKLOAD user IN `all` SETTINGS weight = 9\n",
            "xml_all": "CREATE WORKLOAD xml_all SETTINGS weight = 2",
            "dev": "CREATE WORKLOAD dev IN user",
            "s3_read": "CREATE RESOURCE s3_read (READ DISK s3);",
            "all": "CREATE WORKLOAD `all`\n",
            "etl": "CREATE WORKLOAD etl IN xml_all",
        },
    )

    WorkloadEntitiesBackup().restore(cast(BackupContext, context))

    assert [
        call.args[0] for call in context.ch_ctl.restore_workload_entity.call_args_list
    ] == [
        "CREATE OR REPLACE WORKLOAD user IN `all` SETTINGS weight = 9\n",
        "CREATE WORKLOAD dev IN user",
        "CREATE WORKLOAD etl IN xml_all",
    ]


def test_restore_fails_on_entity_of_another_type() -> None:
    context = _context(
        on_server={"all": "CREATE RESOURCE `all` (READ DISK s3)"},
        in_backup={"all": "CREATE WORKLOAD `all`"},
    )

    with pytest.raises(RuntimeError, match="type mismatch"):
        WorkloadEntitiesBackup().restore(cast(BackupContext, context))
    context.ch_ctl.restore_workload_entity.assert_not_called()


def test_restore_fails_on_parent_missing_everywhere() -> None:
    context = _context(on_server={}, in_backup={"dev": "CREATE WORKLOAD dev IN user"})

    with pytest.raises(RuntimeError, match="Parent user for workload dev"):
        WorkloadEntitiesBackup().restore(cast(BackupContext, context))
    context.ch_ctl.restore_workload_entity.assert_not_called()


@pytest.mark.parametrize(
    ["statement", "expected_name", "expected_type", "expected_parent"],
    [
        # Simple workload, no parent, no settings
        (
            "CREATE WORKLOAD all",
            "all",
            WorkloadEntityType.WORKLOAD,
            None,
        ),
        # Workload with backtick-quoted plain name
        (
            "CREATE WORKLOAD `all`",
            "all",
            WorkloadEntityType.WORKLOAD,
            None,
        ),
        # Workload with backtick-quoted name containing spaces
        (
            "CREATE WORKLOAD `space test`",
            "space test",
            WorkloadEntityType.WORKLOAD,
            None,
        ),
        # Workload with IN parent (plain names)
        (
            "CREATE WORKLOAD production IN all",
            "production",
            WorkloadEntityType.WORKLOAD,
            "all",
        ),
        # Workload with backtick-quoted name and backtick-quoted parent
        (
            "CREATE WORKLOAD `my workload` IN `root workload`",
            "my workload",
            WorkloadEntityType.WORKLOAD,
            "root workload",
        ),
        # Workload with SETTINGS
        (
            "CREATE WORKLOAD all SETTINGS max_bytes_per_second = 2147483648",
            "all",
            WorkloadEntityType.WORKLOAD,
            None,
        ),
        # Workload with IN and SETTINGS
        (
            "CREATE WORKLOAD production IN all SETTINGS max_bytes_per_second = 1073741824",
            "production",
            WorkloadEntityType.WORKLOAD,
            "all",
        ),
        # Workload with backtick-quoted names and SETTINGS
        (
            "CREATE WORKLOAD `space test` IN `root node` SETTINGS max_bytes_per_second = 500",
            "space test",
            WorkloadEntityType.WORKLOAD,
            "root node",
        ),
        # Workload with trailing semicolon
        (
            "CREATE WORKLOAD all;",
            "all",
            WorkloadEntityType.WORKLOAD,
            None,
        ),
        # Case-insensitive keyword
        (
            "create workload all",
            "all",
            WorkloadEntityType.WORKLOAD,
            None,
        ),
        # Simple resource
        (
            "CREATE RESOURCE s3_write (WRITE DISK s3)",
            "s3_write",
            WorkloadEntityType.RESOURCE,
            None,
        ),
        # Resource with backtick-quoted plain name
        (
            "CREATE RESOURCE `s3_write` (WRITE DISK s3)",
            "s3_write",
            WorkloadEntityType.RESOURCE,
            None,
        ),
        # Resource with backtick-quoted name containing spaces
        (
            "CREATE RESOURCE `my resource` (READ DISK s3)",
            "my resource",
            WorkloadEntityType.RESOURCE,
            None,
        ),
        # Resource with trailing semicolon
        (
            "CREATE RESOURCE s3_read (READ DISK s3);",
            "s3_read",
            WorkloadEntityType.RESOURCE,
            None,
        ),
        # Leading/trailing whitespace
        (
            "  CREATE WORKLOAD all  ",
            "all",
            WorkloadEntityType.WORKLOAD,
            None,
        ),
    ],
)
def test_from_create_statement(
    statement, expected_name, expected_type, expected_parent
):
    entity = WorkloadEntity.from_create_statement(statement)
    assert entity.name == expected_name
    assert entity.type == expected_type
    assert entity.parent == expected_parent
    assert entity.create_statement == statement
