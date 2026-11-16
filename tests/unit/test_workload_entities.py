"""Unit tests for workload entity backup logic."""

from types import SimpleNamespace
from typing import cast
from unittest.mock import MagicMock, call

import pytest

from ch_backup.backup_context import BackupContext
from ch_backup.clickhouse.client import ClickhouseError
from ch_backup.clickhouse.config import ClickhouseConfig
from ch_backup.clickhouse.models import WorkloadEntityType
from ch_backup.logic.workload_entities import (
    WorkloadEntitiesBackup,
    WorkloadEntity,
    get_config_workload_entity_names,
)

CONFIG_ENTITIES = """
    CREATE RESOURCE xml_s3_read (READ DISK s3);
    CREATE WORKLOAD xml_all;
    CREATE WORKLOAD xml_production IN xml_all SETTINGS weight = 3;
"""


@pytest.fixture(name="restore_context")
def fixture_restore_context() -> MagicMock:
    context = MagicMock()
    context.ch_ctl.ch_version_ge.return_value = True
    context.ch_config.config = {}
    context.ch_ctl.get_workload_entities.return_value = {
        "io": (WorkloadEntityType.RESOURCE, "CREATE RESOURCE io (READ ANY DISK)")
    }
    context.ch_ctl.format_query.side_effect = lambda query: query
    context.backup_meta.get_workload_entities.return_value = ["io"]
    context.backup_layout.get_workload_entity_create_statement.return_value = (
        "CREATE RESOURCE io (READ ANY DISK)"
    )
    return context


def test_restore_skips_identical_live_definition(restore_context: MagicMock) -> None:
    WorkloadEntitiesBackup().restore(restore_context)

    restore_context.ch_ctl.restore_workload_entity.assert_not_called()


@pytest.mark.parametrize(
    ("entity_type", "old", "new", "replacement"),
    [
        (
            WorkloadEntityType.RESOURCE,
            "CREATE RESOURCE io (READ ANY DISK)",
            "CREATE RESOURCE io (WRITE ANY DISK)",
            "CREATE OR REPLACE RESOURCE io (WRITE ANY DISK)",
        ),
        (
            WorkloadEntityType.WORKLOAD,
            "CREATE WORKLOAD io SETTINGS weight = 1",
            "  create workload io SETTINGS weight = 2;\n",
            "CREATE OR REPLACE workload io SETTINGS weight = 2;\n",
        ),
    ],
    ids=["resource", "workload"],
)
def test_restore_replaces_changed_definition(
    restore_context: MagicMock,
    entity_type: WorkloadEntityType,
    old: str,
    new: str,
    replacement: str,
) -> None:
    restore_context.ch_ctl.get_workload_entities.return_value = {
        "io": (entity_type, old)
    }
    restore_context.backup_layout.get_workload_entity_create_statement.return_value = (
        new
    )

    WorkloadEntitiesBackup().restore(restore_context)

    restore_context.ch_ctl.restore_workload_entity.assert_called_once_with(replacement)


def test_restore_rejects_type_mismatch(restore_context: MagicMock) -> None:
    restore_context.ch_ctl.get_workload_entities.return_value = {
        "io": (WorkloadEntityType.WORKLOAD, "CREATE WORKLOAD io")
    }

    with pytest.raises(RuntimeError, match="'io'.*type mismatch"):
        WorkloadEntitiesBackup().restore(restore_context)

    restore_context.ch_ctl.restore_workload_entity.assert_not_called()


def test_restore_reports_replacement_error(restore_context: MagicMock) -> None:
    restore_context.backup_layout.get_workload_entity_create_statement.return_value = (
        "CREATE RESOURCE io (WRITE ANY DISK)"
    )
    error = ClickhouseError("Server rejected replacement")
    restore_context.ch_ctl.restore_workload_entity.side_effect = error

    with pytest.raises(
        RuntimeError, match="'io'.*Server rejected replacement"
    ) as raised:
        WorkloadEntitiesBackup().restore(restore_context)

    assert raised.value.__cause__ is error


def test_restore_creates_missing_entities_in_dependency_order(
    restore_context: MagicMock,
) -> None:
    statements = {
        "child": "CREATE WORKLOAD child IN root SETTINGS weight = 2 FOR io",
        "root": "CREATE WORKLOAD root",
        "io": "CREATE RESOURCE io (READ ANY DISK)",
    }
    restore_context.ch_ctl.get_workload_entities.return_value = {}
    restore_context.backup_meta.get_workload_entities.return_value = list(statements)
    restore_context.backup_layout.get_workload_entity_create_statement.side_effect = (
        lambda _, name: statements[name]
    )

    WorkloadEntitiesBackup().restore(restore_context)

    assert restore_context.ch_ctl.restore_workload_entity.call_args_list == [
        call("CREATE OR REPLACE RESOURCE io (READ ANY DISK)"),
        call("CREATE OR REPLACE WORKLOAD root"),
        call("CREATE OR REPLACE WORKLOAD child IN root SETTINGS weight = 2 FOR io"),
    ]
    restore_context.ch_ctl.format_query.assert_not_called()


def test_restore_uses_existing_parent(restore_context: MagicMock) -> None:
    restore_context.ch_ctl.get_workload_entities.return_value = {
        "root": (WorkloadEntityType.WORKLOAD, "CREATE WORKLOAD root")
    }
    restore_context.backup_meta.get_workload_entities.return_value = ["child"]
    restore_context.backup_layout.get_workload_entity_create_statement.return_value = (
        "CREATE WORKLOAD child IN root"
    )

    WorkloadEntitiesBackup().restore(restore_context)

    restore_context.ch_ctl.restore_workload_entity.assert_called_once_with(
        "CREATE OR REPLACE WORKLOAD child IN root"
    )


@pytest.mark.parametrize(
    "existing",
    [
        {},
        {"root": (WorkloadEntityType.RESOURCE, "CREATE RESOURCE root (READ ANY DISK)")},
    ],
    ids=["missing-parent", "resource-parent"],
)
def test_restore_rejects_unavailable_parent(
    restore_context: MagicMock, existing: dict[str, tuple[WorkloadEntityType, str]]
) -> None:
    restore_context.ch_ctl.get_workload_entities.return_value = existing
    restore_context.backup_meta.get_workload_entities.return_value = ["child"]
    restore_context.backup_layout.get_workload_entity_create_statement.return_value = (
        "CREATE WORKLOAD child IN root"
    )

    with pytest.raises(RuntimeError, match="Parent root for workload child"):
        WorkloadEntitiesBackup().restore(restore_context)

    restore_context.ch_ctl.restore_workload_entity.assert_not_called()


def _context(
    on_server: dict[str, str], in_backup: dict[str, str] | None = None
) -> SimpleNamespace:
    """Helper: a context of a server with the given entities and entities from config."""
    ch_ctl = MagicMock()
    ch_ctl.ch_version_ge.return_value = True
    ch_ctl.get_workload_entities.return_value = {
        name: (WorkloadEntity.from_create_statement(statement).type, statement)
        for name, statement in on_server.items()
    }
    ch_ctl.format_query.side_effect = lambda query: query
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
    """
    Entities from the server config are backed up with the config.
    """
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


def test_restore_skips_config_entities() -> None:
    """
    ClickHouse does not let replace entities from the server config, so they are
    skipped even if the backup has another definition.
    """
    context = _context(
        on_server={"xml_all": "CREATE WORKLOAD xml_all"},
        in_backup={
            "xml_all": "CREATE WORKLOAD xml_all SETTINGS weight = 2",
            "xml_s3_read": "CREATE RESOURCE xml_s3_read (READ DISK s3)",
            "etl": "CREATE WORKLOAD etl IN xml_all",
        },
    )

    WorkloadEntitiesBackup().restore(cast(BackupContext, context))

    assert context.ch_ctl.restore_workload_entity.call_args_list == [
        call("CREATE OR REPLACE WORKLOAD etl IN xml_all")
    ]


@pytest.mark.parametrize(
    "config",
    [
        CONFIG_ENTITIES,
        "-- io limits\nCREATE RESOURCE IF NOT EXISTS xml_s3_read ON CLUSTER c "
        "(READ DISK s3);\ncreate or replace workload `xml_all`;\n"
        "CREATE WORKLOAD xml_production IN xml_all",
        {"@replace": "1", "#text": CONFIG_ENTITIES},
    ],
    ids=["statements", "other forms", "with attribute"],
)
def test_config_entity_names(config: str | dict) -> None:
    """
    A statement the restore can not parse must not fail a backup.
    """
    ch_config = SimpleNamespace(config={"resources_and_workloads": config})

    assert get_config_workload_entity_names(cast(ClickhouseConfig, ch_config)) == {
        "xml_s3_read",
        "xml_all",
        "xml_production",
    }


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
