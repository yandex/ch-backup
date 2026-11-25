"""Unit tests for restoring detached ClickHouse data parts."""

from contextlib import AbstractContextManager, nullcontext
from functools import partial
from hashlib import md5
from pathlib import Path
from tarfile import BLOCKSIZE
from typing import Any, Callable
from unittest.mock import MagicMock, patch

import pytest

from ch_backup.backup.metadata import (
    BackupMetadata,
    DetachedPartMetadata,
    TableMetadata,
)
from ch_backup.clickhouse.control import ClickhouseCTL
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
    context.ch_ctl.get_detached_part_path.side_effect = partial(
        ClickhouseCTL.get_detached_part_path, context.ch_ctl
    )
    context.ch_ctl._get_table_detached_path.side_effect = (  # pylint: disable=protected-access
        lambda _table, disk: f"/data/{disk}/detached"
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


@pytest.fixture(name="detached_candidate")
def fixture_detached_candidate(tmp_path: Path) -> tuple[Path, DetachedPartMetadata]:
    part_path = tmp_path / "broken_all_1_1_0"
    part_path.mkdir()
    (part_path / "checksums.txt").write_bytes(b"checksums")
    return part_path, DetachedPartMetadata(
        "db",
        "table",
        part_path.name,
        "broken",
        md5(b"checksums").hexdigest(),  # nosec
        BLOCKSIZE,
        ["checksums.txt"],
        True,
        "default",
    )


def test_detached_match_rejects_missing_directory(detached_candidate):
    """A vanished directory must not be accepted as an already restored part."""
    part_path, part = detached_candidate
    assert ClickhouseCTL.detached_part_matches(part, str(part_path))
    (part_path / "checksums.txt").unlink()
    part_path.rmdir()

    assert not ClickhouseCTL.detached_part_matches(part, str(part_path))


@pytest.mark.parametrize(
    "operation,error",
    [
        ("scan_dir_files", OSError),
        ("calc_aligned_files_size", OSError),
        ("_get_part_checksum", OSError),
        ("_get_part_checksum", ValueError),
    ],
)
def test_detached_match_handles_validation_errors(detached_candidate, operation, error):
    """
    Unreadable files cannot authorize reuse. Other validation errors must
    propagate instead of being hidden as ordinary filesystem misses.
    """
    part_path, part = detached_candidate
    expected_error: AbstractContextManager = nullcontext()
    if error is ValueError:
        expected_error = pytest.raises(ValueError)
    with (
        patch(f"ch_backup.clickhouse.control.{operation}", side_effect=error()),
        expected_error,
    ):
        assert not ClickhouseCTL.detached_part_matches(part, str(part_path))


@pytest.mark.parametrize(
    "field,value", [("files", ["missing.bin"]), ("size", 2 * BLOCKSIZE)]
)
def test_detached_match_rejects_names_or_aligned_size(detached_candidate, field, value):
    """
    Equal checksum-file contents alone do not prove that a detached directory
    contains the expected files. A different file list or stored size makes the
    candidate unsafe to reuse.
    """
    path, part = detached_candidate
    setattr(part.raw_metadata, field, value)
    assert not ClickhouseCTL.detached_part_matches(part, str(path))
