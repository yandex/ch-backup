"""Whole-part completion and atomic publication with real asynchronous failures."""

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
from tarfile import GNU_FORMAT, TarInfo
from unittest.mock import MagicMock, patch

import pytest

from ch_backup import logging
from ch_backup.backup.layout import BackupLayout
from ch_backup.backup.metadata import BackupMetadata, PartMetadata
from ch_backup.backup.metadata.table_metadata import TableMetadata
from ch_backup.backup.restore_context import PartState, RestoreContext
from ch_backup.clickhouse.models import Database
from ch_backup.config import DEFAULT_CONFIG
from ch_backup.exceptions import StorageError, TerminatingSignal
from ch_backup.logic.part_restore import LocalPartRestorer
from ch_backup.logic.table import TableBackup
from ch_backup.storage.async_pipeline.base_pipeline import exec_pool
from ch_backup.storage.async_pipeline.base_pipeline.exec_pool import ThreadExecPool
from ch_backup.storage.async_pipeline.stages.filesystem.write_files_stage import (
    UnpackTarballStage,
    WriteFilesStage,
)


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
        True,
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

    with ThreadExecPool(2) as pool:
        storage.wait.side_effect = pool.wait_all

        def write(path, files):
            assert not (tmp_path / "detached" / part.name).exists()
            for name, data in files.items():
                target = Path(path) / name
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(data)

        def download_files(*, local_path, callback, on_error, **_kwargs):
            pool.submit(
                local_path,
                write,
                local_path,
                _files(),
                callback=callback,
                on_error=on_error,
            )

        def download_file(*, local_path, callback, on_error, **_kwargs):
            pool.submit(
                local_path,
                Path(local_path).write_bytes,
                _files()[Path(local_path).name],
                callback=callback,
                on_error=on_error,
            )

        storage.download_files.side_effect = download_files
        storage.download_file.side_effect = download_file
        yield context, table, part, storage, pool, tmp_path


def _run(restore, keep_going=False):
    # pylint: disable=protected-access
    context, table, *_ = restore
    TableBackup._restore_data(context, [table], MagicMock(), False, keep_going)


@pytest.mark.parametrize("tarball", [True, False])
@pytest.mark.parametrize("legacy_size", [False, True])
def test_publish_before_state_and_attach(restore, tarball, legacy_size):
    context, _, part, _, _, root = restore
    part.raw_metadata.tarball = tarball
    if legacy_size:
        part.raw_metadata.size = sum(map(len, _files().values()))
    restore[1].raw_metadata["parts"][part.name].update(
        tarball=part.tarball, bytes=part.size
    )
    original = context.restore_context.change_part_state

    def change(state, current):
        if state == PartState.DOWNLOADED:
            assert (root / "detached" / part.name / "count.txt").read_bytes() == b"10"
            context.ch_ctl.attach_part.assert_not_called()
        original(state, current)

    with patch.object(context.restore_context, "change_part_state", side_effect=change):
        _run(restore)
    assert context.restore_context.part_restored(part)
    assert (root / part.name / "count.txt").read_bytes() == b"10"
    assert not list((root / "detached").iterdir())


@pytest.mark.parametrize("keep_going", [False, True])
@pytest.mark.parametrize("tarball", [False, True])
def test_failed_download_is_private_and_retry_starts_clean(
    restore, keep_going, tarball
):
    context, _, part, storage, pool, root = restore
    part.raw_metadata.tarball = tarball
    restore[1].raw_metadata["parts"][part.name]["tarball"] = part.tarball
    method = storage.download_files if tarball else storage.download_file
    success = method.side_effect
    failed_paths = []
    failure_reported = threading.Event()

    def write_then_fail(path):
        path = Path(path)
        failed_paths.append(path)
        if tarball:
            (path / "count.txt").write_bytes(b"1")
        else:
            path.write_bytes(b"1")
        raise OSError("interrupted download")

    def download(*, local_path, callback, on_error, **_kwargs):
        if not tarball and Path(local_path).name == "checksums.txt":

            def write_after_failure():
                assert failure_reported.wait(5)
                Path(local_path).write_bytes(_files()["checksums.txt"])

            return pool.submit(
                local_path, write_after_failure, callback=callback, on_error=on_error
            )

        def report(error):
            on_error(error)
            failure_reported.set()

        pool.submit(
            local_path,
            write_then_fail,
            local_path,
            callback=callback,
            on_error=report,
        )

    method.side_effect = download
    if keep_going:
        _run(restore, True)
    else:
        with pytest.raises(OSError, match="interrupted download"):
            _run(restore)
    assert context.restore_context.has_failed_parts()
    assert not context.restore_context.part_downloaded(part)
    context.ch_ctl.attach_part.assert_not_called()
    assert not list((root / "detached").iterdir())

    method.side_effect = success
    context.restore_context = RestoreContext(context.config_root["backup"])
    _run(restore)
    assert context.restore_context.part_restored(part)
    assert not context.restore_context.has_failed_parts()
    assert all(not path.exists() for path in failed_paths)


@pytest.mark.parametrize("damage", ["missing", "extra", "checksum", "size"])
def test_download_metadata_mismatch_is_not_published(restore, damage):
    context, _, part, _, _, root = restore
    files = _files()
    if damage == "missing":
        del files["count.txt"]
    elif damage == "extra":
        files["extra.txt"] = b"x"
    elif damage == "size":
        restore[1].raw_metadata["parts"][part.name]["bytes"] = 123
    else:
        files["checksums.txt"] = b"wrong checksum"
    with patch(__name__ + "._files", return_value=files):
        _run(restore, True)
    context.ch_ctl.attach_part.assert_not_called()
    assert context.restore_context.has_failed_parts()
    assert not context.restore_context.part_downloaded(part)
    assert not list((root / "detached").iterdir())


@pytest.mark.parametrize("state", [PartState.NOT_DOWNLOADED, PartState.DOWNLOADED])
@pytest.mark.parametrize(
    "contents", ["matching", "legacy", "corrupt", "missing", "extra", "file"]
)
def test_existing_destination_is_verified_and_preserved(restore, state, contents):
    context, _, part, storage, _, root = restore
    if contents == "legacy":
        restore[1].raw_metadata["parts"][part.name]["bytes"] = sum(
            map(len, _files().values())
        )
    target = root / "detached" / part.name
    if contents != "missing":
        target.mkdir(parents=True)
        for name, data in _files().items():
            (target / name).write_bytes(data)
        if contents == "corrupt":
            (target / "count.txt").write_bytes(b"11")
        if contents == "extra":
            (target / "extra").write_bytes(b"extra")
        if contents == "file":
            for path in target.iterdir():
                path.unlink()
            target.rmdir()
            target.write_bytes(b"preserved")
    context.restore_context.change_part_state(state, part)
    _run(restore, True)
    if contents in {"matching", "legacy", "missing"}:
        assert context.restore_context.part_restored(part)
        assert (root / part.name / "count.txt").read_bytes() == b"10"
    else:
        assert target.exists()
        if contents == "corrupt":
            assert (target / "count.txt").read_bytes() == b"11"
        if contents == "file":
            assert target.read_bytes() == b"preserved"
        context.ch_ctl.attach_part.assert_not_called()
        assert context.restore_context.has_failed_parts()
        assert not context.restore_context.part_downloaded(part)
    if contents != "missing":
        storage.download_files.assert_not_called()


def test_per_file_submission_error_does_not_confirm_part(restore):
    context, _, part, storage, _, root = restore
    part.raw_metadata.tarball = False
    restore[1].raw_metadata["parts"][part.name]["tarball"] = part.tarball
    success = storage.download_file.side_effect

    def download(**kwargs):
        if kwargs["local_path"].endswith("checksums.txt"):
            raise OSError("cannot submit")
        return success(**kwargs)

    storage.download_file.side_effect = download
    _run(restore, True)
    context.ch_ctl.attach_part.assert_not_called()
    assert context.restore_context.has_failed_parts()
    assert not list((root / "detached").iterdir())


@pytest.mark.parametrize("keep_going", [False, True])
def test_rename_failure_does_not_mark_downloaded(restore, keep_going):
    context, _, part, _, _, root = restore
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
    context, _, part, _, _, root = restore
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
    _, _, _, storage, _, _ = restore
    storage.download_files.side_effect = BrokenExecutor("broken")
    with pytest.raises(Exception, match="Failed to download part"):
        _run(restore, True)
    storage.wait.assert_not_called()
    storage.abort.assert_called_once_with()


@pytest.mark.parametrize("error", [TerminatingSignal(), BrokenExecutor("broken")])
def test_fatal_drain_error_propagates_after_submission_failure(restore, error):
    _, _, _, storage, _, _ = restore
    storage.download_files.side_effect = OSError("cannot submit")
    storage.wait.side_effect = error
    expected = (
        TerminatingSignal if isinstance(error, TerminatingSignal) else StorageError
    )
    with pytest.raises(expected) as raised:
        _run(restore)
    assert raised.value is error or raised.value.__cause__ is error


def test_abort_failure_does_not_replace_termination(restore):
    _, _, _, storage, _, _ = restore
    termination = TerminatingSignal()
    storage.download_files.side_effect = termination
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
def test_sigterm_during_submission_stops_workers_and_keeps_staging(
    tmp_path, monkeypatch
):
    # pylint: disable=protected-access
    monkeypatch.setattr(logging.logger._core, "handlers", {})  # type: ignore[attr-defined]
    monkeypatch.setattr(exec_pool, "TIMEOUT_WORKER_TERMINATING_SEC", 0.1)
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
        os.kill(os.getpid(), signal.SIGTERM)

    monkeypatch.setattr(layout._storage_loader, "download_files", download)
    part = PartMetadata("db", "table", destination.name, "checksum", 1024, [], True)
    try:
        with pytest.raises(TerminatingSignal):
            with LocalPartRestorer(
                layout,
                BackupMetadata("backup", "2", "25.8", "%Y-%m-%d"),
                keep_going=True,
                on_success=completed.append,
                on_error=failed,
            ) as restorer:
                restorer.download(part, str(destination))
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


@pytest.mark.parametrize("cut", [1, 511, 512, 562, 612, 1023])
@pytest.mark.parametrize("disk", [False, True])
def test_tar_rejects_truncated_header_payload_or_padding(tmp_path, cut, disk):
    header = TarInfo("data.bin")
    header.size = 100
    stream = header.tobuf(format=GNU_FORMAT) + b"x" * 100 + b"\0" * 412
    stage = (
        WriteFilesStage({}, tmp_path, 4096) if disk else UnpackTarballStage({}, 4096)
    )
    stage(stream[:cut], 0)
    with pytest.raises(ValueError, match="Truncated TAR"):
        list(stage.on_done() or [])


@pytest.mark.parametrize("name", ["data.bin", "длинное_имя/" * 20 + "file"])
def test_tar_accepts_complete_files_without_end_blocks(tmp_path, name):
    header = TarInfo(name)
    header.size = 100
    stream = header.tobuf(format=GNU_FORMAT) + b"x" * 100 + b"\0" * 412
    stage = WriteFilesStage({}, tmp_path, 4096)
    for pos in range(0, len(stream), 71):
        stage(stream[pos : pos + 71], pos)
    stage.on_done()
    assert (tmp_path / name).read_bytes() == b"x" * 100


def test_tar_rejects_incomplete_long_name(tmp_path):
    header = TarInfo("long_name" * 30)
    stage = WriteFilesStage({}, tmp_path, 4096)
    stage(header.tobuf(format=GNU_FORMAT)[:530], 0)
    with pytest.raises(ValueError, match="Truncated TAR"):
        stage.on_done()
