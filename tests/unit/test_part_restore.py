"""Publication, state persistence and fatal errors not covered by CLI scenarios."""

import copy
import os
import signal
import struct
import threading
import time
from concurrent.futures import BrokenExecutor
from hashlib import md5
from multiprocessing.process import BaseProcess
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from ch_backup import logging
from ch_backup.backup.layout import BackupLayout
from ch_backup.backup.metadata import BackupMetadata, DetachedPartMetadata, PartMetadata
from ch_backup.backup.metadata.table_metadata import TableMetadata
from ch_backup.backup.restore_context import PartState, RestoreContext
from ch_backup.clickhouse.models import Database
from ch_backup.config import DEFAULT_CONFIG
from ch_backup.exceptions import ClickhouseBackupError, StorageError, TerminatingSignal
from ch_backup.logic.part_restore import LocalPartRestorer, check_local_part
from ch_backup.logic.table import TableBackup
from ch_backup.storage.async_pipeline.base_pipeline import exec_pool


def _files() -> dict[str, bytes]:
    # A real format-3 manifest with the ClickHouse hash of count.txt = b"10".
    checksum = struct.pack("<QQ", 0x95FC7BAFFF28F6B1, 0x924D3940D9DC0F2E)
    return {
        "count.txt": b"10",
        "checksums.txt": b"checksums format version: 3\n\x01\x09count.txt\x02"
        + checksum
        + b"\x00",
    }


@pytest.fixture(name="restore")
def fixture_restore(tmp_path):
    config = copy.deepcopy(DEFAULT_CONFIG)
    config["backup"].update(
        path_root="backups", restore_context_path=str(tmp_path / "state.json")
    )
    config["multiprocessing"]["workers"] = 0
    layout = BackupLayout(config)  # type: ignore[arg-type]
    storage = MagicMock()
    layout._storage_loader = storage  # pylint: disable=protected-access
    layout._get_escaped_if_exists = lambda _fn, *_args: "backup/data"  # type: ignore[method-assign]  # pylint: disable=protected-access
    backup = BackupMetadata("backup", "2", "25.8", "%Y-%m-%d")
    backup.add_database(Database("db", "Atomic", None, None, None))
    table = TableMetadata("db", "table", "MergeTree", None)
    backup.add_table(table)
    part = PartMetadata(
        "db",
        "table",
        "all_1_1_0",
        md5(_files()["checksums.txt"]).hexdigest(),
        1024,
        list(_files()),
        False,
    )
    table.add_part(part)
    context = MagicMock()
    context.config_root = config
    context.backup_meta = backup
    context.backup_layout = layout
    context.restore_context = RestoreContext(config["backup"])
    context.ch_ctl.get_detached_part_path.side_effect = lambda _table, _disk, name: str(
        tmp_path / "detached" / name
    )
    context.ch_ctl.attach_part.side_effect = lambda _table, name: os.rename(
        tmp_path / "detached" / name, tmp_path / name
    )

    def download_file(*, local_path, callback, **_kwargs):
        assert not (tmp_path / "detached" / part.name).exists()
        Path(local_path).write_bytes(_files()[Path(local_path).name])
        callback()

    storage.download_file.side_effect = download_file
    return context, table, part, storage, tmp_path


def _run(restore, keep_going=False):
    # pylint: disable=protected-access
    context, table, *_ = restore
    TableBackup._restore_data(context, [table], MagicMock(), False, keep_going)


@pytest.mark.parametrize(
    "contents", ["aligned", "legacy", "missing", "extra", "checksum", "size"]
)
def test_local_part_metadata_validation(tmp_path, contents):
    files = _files()
    part = PartMetadata(
        "db",
        "table",
        "all_1_1_0",
        md5(files["checksums.txt"]).hexdigest(),
        sum(map(len, files.values())) if contents == "legacy" else 1024,
        list(files),
        True,
    )
    if contents == "missing":
        del files["count.txt"]
    elif contents == "extra":
        files["extra.txt"] = b"x"
    elif contents == "checksum":
        files["checksums.txt"] = b"wrong checksum"
    elif contents == "size":
        part.raw_metadata.size = 123
    for name, data in files.items():
        (tmp_path / name).write_bytes(data)
    if contents in {"aligned", "legacy"}:
        check_local_part(part, str(tmp_path), existing=True)
    else:
        with pytest.raises(ClickhouseBackupError, match="does not match backup"):
            check_local_part(part, str(tmp_path))


def test_downloaded_part_missing_on_disk_is_downloaded_again(restore):
    context, _, part, _, root = restore
    context.restore_context.change_part_state(PartState.DOWNLOADED, part)
    _run(restore)
    assert context.restore_context.part_restored(part)
    assert (root / part.name / "count.txt").read_bytes() == b"10"


def test_local_part_rejects_symlinked_projection_directory(tmp_path):
    root = tmp_path / "part"
    root.mkdir()
    projection = tmp_path / "projection"
    projection.mkdir()
    (projection / "count.txt").write_bytes(b"10")
    (root / "p.proj").symlink_to(projection, target_is_directory=True)
    (root / "checksums.txt").write_bytes(b"checksums")
    part = PartMetadata(
        "db",
        "table",
        "all_1_1_0",
        md5(b"checksums").hexdigest(),
        1024,
        ["p.proj/count.txt", "checksums.txt"],
        True,
    )
    with pytest.raises(ClickhouseBackupError, match="symlink"):
        check_local_part(part, str(root))


@pytest.mark.parametrize("filename", ["../outside", "/outside"])
def test_legacy_download_rejects_paths_outside_staging(restore, filename):
    context, _, part, storage, root = restore
    part.raw_metadata.files = ["count.txt", filename]
    with pytest.raises(StorageError):
        context.backup_layout.download_data_part(
            context.backup_meta, part, str(root / "staging"), MagicMock()
        )
    storage.download_file.assert_not_called()
    assert not (root / "outside").exists()


@pytest.mark.parametrize("keep_going", [False, True])
def test_rename_failure_does_not_mark_downloaded(restore, keep_going):
    context, _, part, _, root = restore
    with patch(
        "ch_backup.logic.part_restore.os.rename", side_effect=OSError("rename failed")
    ):
        if keep_going:
            _run(restore, True)
        else:
            with pytest.raises(OSError, match="rename failed"):
                _run(restore)
    context.ch_ctl.attach_part.assert_not_called()
    assert not context.restore_context.part_downloaded(part)
    assert not list((root / "detached").iterdir())


def test_state_failure_after_publication_is_fatal_and_recoverable(restore):
    context, _, part, _, root = restore
    context.config_root["backup"]["restore_context_sync_threshold_ops"] = 1
    context.restore_context = RestoreContext(context.config_root["backup"])
    error = OSError("cannot save state")
    with patch.object(context.restore_context, "dump_state", side_effect=error):
        with pytest.raises(OSError) as raised:
            _run(restore, True)
    assert raised.value is error
    context.ch_ctl.attach_part.assert_not_called()
    assert (root / "detached" / part.name).is_dir()
    context.restore_context = RestoreContext(context.config_root["backup"])
    _run(restore)
    assert context.restore_context.part_restored(part)


def test_cleanup_failure_preserves_original_error(restore):
    with (
        patch(
            "ch_backup.logic.part_restore.promote_part",
            side_effect=ValueError("original"),
        ),
        patch(
            "ch_backup.logic.part_restore.shutil.rmtree",
            side_effect=PermissionError("cleanup"),
        ),
        pytest.raises(ValueError, match="original"),
    ):
        _run(restore)


def test_broken_executor_is_fatal_with_keep_going(restore):
    _, _, _, storage, _ = restore
    storage.download_file.side_effect = BrokenExecutor("broken")
    with pytest.raises(Exception, match="Failed to download part"):
        _run(restore, True)
    storage.wait.assert_not_called()
    storage.abort.assert_called_once_with()


@pytest.mark.parametrize("error", [TerminatingSignal(), BrokenExecutor("broken")])
def test_fatal_drain_error_propagates_after_submission_failure(restore, error):
    _, _, _, storage, _ = restore
    storage.download_file.side_effect = OSError("cannot submit")
    storage.wait.side_effect = error
    expected = (
        TerminatingSignal if isinstance(error, TerminatingSignal) else StorageError
    )
    with pytest.raises(expected) as raised:
        _run(restore)
    assert raised.value is error or raised.value.__cause__ is error


def test_abort_failure_does_not_replace_termination(restore):
    _, _, _, storage, _ = restore
    termination = TerminatingSignal()
    storage.download_file.side_effect = termination
    storage.abort.side_effect = BrokenExecutor("broken")
    with pytest.raises(TerminatingSignal) as raised:
        _run(restore)
    assert raised.value is termination
    storage.wait.assert_not_called()


def _unfinished_download(path: str, release: Path) -> None:
    Path(path).write_bytes(b"partial")
    while not release.exists():
        time.sleep(0.01)


@pytest.mark.skipif(os.name != "posix", reason="Uses SIGTERM")
@pytest.mark.parametrize("detached", [False, True])
@pytest.mark.parametrize("phase", ["submission", "wait"])
def test_sigterm_stops_part_workers_and_keeps_staging(
    tmp_path, monkeypatch, detached, phase
):
    # pylint: disable=protected-access,too-many-locals,too-many-statements
    monkeypatch.setattr(logging.logger._core, "handlers", {})  # type: ignore[attr-defined]
    monkeypatch.setattr(exec_pool, "TIMEOUT_WORKER_TERMINATING_SEC", 1)
    config = copy.deepcopy(DEFAULT_CONFIG)
    config["backup"]["path_root"] = "backups"
    layout = BackupLayout(config)  # type: ignore[arg-type]
    monkeypatch.setattr(layout, "_get_escaped_if_exists", lambda *_args: "data")
    pool = layout._storage_loader._ploader._exec_pool
    assert pool is not None
    processes: list[BaseProcess] = []
    completed: list[PartMetadata] = []
    failed = MagicMock()
    release = tmp_path / "release"
    # Release the worker even with the old, broken drain path, so regression
    # failures cannot hang pytest. Successful cancellation never needs this file.
    release_timer = threading.Timer(10, release.touch)
    destination = tmp_path / "detached" / "all_1_1_0"
    staging = []

    def download(*, local_path, **_kwargs):
        staging.append(Path(local_path))
        ready = Path(local_path) / "count.txt"
        pool.submit("unfinished", _unfinished_download, str(ready), release)
        processes.extend(pool._pool._processes.values())
        release_timer.start()
        while not ready.exists():
            assert not release.exists(), "worker did not start"
            time.sleep(0.01)
        if phase == "submission":
            os.kill(os.getpid(), signal.SIGTERM)

    monkeypatch.setattr(layout._storage_loader, "download_files", download)
    if phase == "wait":
        monkeypatch.setattr(
            layout,
            "wait",
            lambda *_args, **_kwargs: os.kill(os.getpid(), signal.SIGTERM),
        )
    part = PartMetadata("db", "table", destination.name, "checksum", 1024, [], True)
    try:
        with pytest.raises(TerminatingSignal):
            backup = BackupMetadata("backup", "2", "25.8", "%Y-%m-%d")
            if detached:
                context = MagicMock()
                context.backup_meta = backup
                context.backup_layout = layout
                context.ch_ctl.get_detached_part_path.return_value = str(destination)
                table = TableMetadata("db", "table", "MergeTree", None)
                table.add_detached_part(
                    DetachedPartMetadata(
                        "db",
                        "table",
                        part.name,
                        "",
                        "checksum",
                        1024,
                        [],
                        True,
                        "default",
                    )
                )
                TableBackup._restore_detached_data(context, [table], False, True)
            else:
                with LocalPartRestorer(
                    layout,
                    backup,
                    keep_going=True,
                    on_success=completed.append,
                    on_error=failed,
                ) as restorer:
                    restorer.download(part, str(destination))
                    restorer.wait()
        assert not release.exists(), "termination waited for the download"
        assert all(not process.is_alive() for process in processes)
        assert (staging[0] / "count.txt").read_bytes() == b"partial"
        assert not destination.exists()
        assert not completed
        failed.assert_not_called()
    finally:
        release_timer.cancel()
        release.touch()
        pool.shutdown(graceful=False)
