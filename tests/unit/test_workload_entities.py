"""Unit tests for workload entity backup logic."""

from pathlib import Path
from types import SimpleNamespace
from typing import Sequence, Tuple, cast
from unittest.mock import MagicMock, patch

import pytest

from ch_backup.backup_context import BackupContext
from ch_backup.clickhouse.models import WorkloadEntityType
from ch_backup.logic import workload_entities
from ch_backup.logic.workload_entities import WorkloadEntitiesBackup, WorkloadEntity


def _backup_context(
    tmp_path: Path,
    storage_path: str,
    entities: Sequence[Tuple[str, WorkloadEntityType]],
    *,
    zookeeper: bool = False,
) -> SimpleNamespace:
    ch_ctl = MagicMock()
    ch_ctl.ch_version_ge.return_value = True
    ch_ctl.get_workload_entities_query.return_value = entities

    backup_meta = MagicMock()
    backup_meta.name = "backup"

    ch_config = {}
    if zookeeper:
        ch_config["workload_zookeeper_path"] = storage_path

    zk_client = MagicMock()
    zk_client.__enter__.return_value = zk_client
    zk_ctl = SimpleNamespace(zk_client=zk_client, zk_root_path="/root")

    return SimpleNamespace(
        ch_ctl=ch_ctl,
        ch_ctl_conf={
            "user": "clickhouse",
            "group": "clickhouse",
            "tmp_path": str(tmp_path),
            "workload_path": storage_path,
        },
        ch_config=SimpleNamespace(config=ch_config),
        backup_meta=backup_meta,
        backup_layout=MagicMock(),
        zk_ctl=zk_ctl,
    )


def _backup(context: SimpleNamespace) -> None:
    with (
        patch.object(workload_entities, "ensure_owned_directory"),
        patch.object(workload_entities, "chown_dir_contents"),
    ):
        WorkloadEntitiesBackup().backup(cast(BackupContext, context))


def test_backup_skips_visible_xml_entities_without_local_storage(
    tmp_path: Path,
) -> None:
    context = _backup_context(
        tmp_path,
        str(tmp_path / "missing-workload-storage"),
        [("xml_workload", WorkloadEntityType.WORKLOAD)],
    )

    with patch.object(workload_entities.logging, "info") as log_info:
        _backup(context)

    context.backup_meta.add_workload_entity.assert_not_called()
    context.backup_layout.upload_workload_entity_ddl_from_file.assert_not_called()
    log_info.assert_any_call(
        "Skipping {} workload entity {} because no matching SQL definition was found in {} storage",
        "workload",
        "xml_workload",
        "local",
    )


def test_backup_includes_only_entities_with_stored_sql_files(tmp_path: Path) -> None:
    storage_path = tmp_path / "workload"
    storage_path.mkdir()
    sql_entity = WorkloadEntity(
        name="sql workload",
        type=WorkloadEntityType.WORKLOAD,
        create_statement="CREATE WORKLOAD `sql workload`",
    )
    (storage_path / sql_entity.filename_on_disk()).write_text(
        sql_entity.create_statement, encoding="utf-8"
    )
    context = _backup_context(
        tmp_path,
        str(storage_path),
        [
            ("xml_resource", WorkloadEntityType.RESOURCE),
            (sql_entity.name, sql_entity.type),
        ],
    )

    _backup(context)

    context.backup_meta.add_workload_entity.assert_called_once_with(sql_entity.name)
    upload_call = (
        context.backup_layout.upload_workload_entity_ddl_from_file.call_args.args
    )
    assert Path(upload_call[0]).name == sql_entity.filename_on_disk()
    assert upload_call[1:] == ("backup", sql_entity.name)


def test_backup_skips_entities_when_zookeeper_data_is_missing(tmp_path: Path) -> None:
    context = _backup_context(
        tmp_path,
        "/clickhouse/workload/definitions.sql",
        [("sql_workload", WorkloadEntityType.WORKLOAD)],
        zookeeper=True,
    )
    context.zk_ctl.zk_client.exists.return_value = False

    _backup(context)

    context.zk_ctl.zk_client.exists.assert_called_once_with(
        "/root/clickhouse/workload/definitions.sql"
    )
    context.backup_meta.add_workload_entity.assert_not_called()
    context.backup_layout.upload_workload_entity_ddl_from_file.assert_not_called()


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
