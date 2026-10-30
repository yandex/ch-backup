"""Unit tests for restoring detached ClickHouse data parts."""

from typing import Any, Callable
from unittest.mock import MagicMock, patch

import pytest

from ch_backup.backup.metadata import (
    BackupMetadata,
    DetachedPartMetadata,
    TableMetadata,
)
from ch_backup.clickhouse.models import Database, Table
from ch_backup.exceptions import ClickhouseBackupError
from ch_backup.logic.table import TableBackup


def _backup_meta() -> BackupMetadata:
    backup = BackupMetadata(
        name="backup",
        version="1",
        ch_version="25.1",
        time_format="%Y-%m-%d %H:%M:%S %z",
    )
    backup.add_database(Database("db", "Atomic", None, None, None))
    backup.add_table(TableMetadata("db", "table", "MergeTree", None))
    return backup


def _table() -> Table:
    return Table("db", "table", "MergeTree", [], [], "", "", None)


def _part(
    name: str = "all_1_1_0", *, object_storage: bool = False
) -> DetachedPartMetadata:
    return DetachedPartMetadata(
        database="db",
        table="table",
        name=name,
        reason="",
        checksum="checksum",
        size=1024,
        files=["checksums.txt"],
        tarball=True,
        disk_name="object_storage" if object_storage else "default",
        object_storage=object_storage,
    )


def _context(part: DetachedPartMetadata) -> tuple[MagicMock, TableMetadata, Table]:
    context = MagicMock()
    context.backup_meta = _backup_meta()
    table_meta = context.backup_meta.get_table("db", "table")
    table_meta.add_detached_part(part)
    table = _table()
    context.ch_ctl.get_table.return_value = table
    context.ch_ctl.get_detached_part_path.side_effect = (
        lambda _table, disk, name: f"/data/{disk}/detached/{name}"
    )
    return context, table_meta, table


def _complete_download(
    _backup: Any,
    _part: Any,
    _path: str,
    callback: Callable[[], None],
) -> None:
    callback()


def test_restore_uses_try_suffix_for_conflicting_part() -> None:
    part = _part()
    context, table_meta, _ = _context(part)
    context.ch_ctl.detached_part_matches.return_value = False
    context.backup_layout.download_detached_part.side_effect = _complete_download

    with (
        patch(
            "ch_backup.logic.table.os.path.exists",
            side_effect=lambda path: path.endswith("/all_1_1_0"),
        ),
        patch.object(TableBackup, "_promote_detached_part") as promote,
    ):
        TableBackup()._restore_detached_data(  # pylint: disable=protected-access
            context,
            [table_meta],
            skip_cloud_storage=False,
            keep_going=False,
        )

    restored_part = promote.call_args.args[1]
    assert restored_part.name == "all_1_1_0_try1"
    assert restored_part.checksum == part.checksum
    assert promote.call_args.args[3] == "/data/default/detached/all_1_1_0_try1"
    context.ch_ctl.attach_part.assert_not_called()


def test_restore_is_idempotent_for_matching_existing_part() -> None:
    part = _part()
    context, table_meta, table = _context(part)
    context.ch_ctl.detached_part_matches.return_value = True

    with patch("ch_backup.logic.table.os.path.exists", return_value=True):
        TableBackup()._restore_detached_data(  # pylint: disable=protected-access
            context,
            [table_meta],
            skip_cloud_storage=False,
            keep_going=False,
        )

    context.backup_layout.download_detached_part.assert_not_called()
    context.ch_ctl.chown_detached_table_parts.assert_called_once_with(
        table, context.restore_context, disk_names=["default"]
    )
    context.ch_ctl.attach_part.assert_not_called()


def test_restore_rejects_part_name_outside_detached_directory() -> None:
    part = _part("../all_1_1_0")
    context, table_meta, _ = _context(part)

    with pytest.raises(ClickhouseBackupError, match="Invalid detached part"):
        TableBackup()._restore_detached_data(  # pylint: disable=protected-access
            context,
            [table_meta],
            skip_cloud_storage=False,
            keep_going=False,
        )

    context.backup_layout.download_detached_part.assert_not_called()


def test_cloud_restore_places_original_metadata_files_directly_in_detached() -> None:
    part = _part(object_storage=True)
    context, table_meta, _ = _context(part)
    context.backup_layout.download_detached_part.side_effect = _complete_download

    with patch.object(TableBackup, "_promote_detached_part") as promote:
        TableBackup()._restore_detached_data(  # pylint: disable=protected-access
            context,
            [table_meta],
            skip_cloud_storage=False,
            keep_going=False,
        )

    restored_part = promote.call_args.args[1]
    assert restored_part.name == part.name
    assert restored_part.checksum == part.checksum
    assert restored_part.object_storage is True
    staging_path = promote.call_args.args[2]
    assert staging_path.startswith("/data/object_storage/detached/.ch-backup-")
    assert promote.call_args.args[3] == ("/data/object_storage/detached/all_1_1_0")
    context.ch_ctl.attach_part.assert_not_called()


def test_keep_going_skips_failed_download_and_restores_next_part() -> None:
    failed = _part("all_1_1_0")
    successful = _part("all_2_2_0")
    context, table_meta, _ = _context(failed)
    table_meta.add_detached_part(successful)

    def download(
        _backup: Any,
        part: DetachedPartMetadata,
        _path: str,
        callback: Callable[[], None],
    ) -> None:
        if part.name == failed.name:
            raise PermissionError("permission denied")
        callback()

    context.backup_layout.download_detached_part.side_effect = download

    with patch.object(TableBackup, "_promote_detached_part") as promote:
        TableBackup()._restore_detached_data(  # pylint: disable=protected-access
            context,
            [table_meta],
            skip_cloud_storage=False,
            keep_going=True,
        )

    promote.assert_called_once()
    assert promote.call_args.args[1].name == successful.name
    context.ch_ctl.attach_part.assert_not_called()
