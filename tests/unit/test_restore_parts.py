"""Only fully downloaded parts may be attached, including after a retry."""

# pylint: disable=protected-access

import copy
import json
from concurrent.futures import BrokenExecutor
from pathlib import Path
from threading import Event
from unittest.mock import Mock

import pytest

from ch_backup import logging
from ch_backup.backup.metadata import BackupMetadata, PartMetadata, TableMetadata
from ch_backup.backup.restore_context import PartState, RestoreContext
from ch_backup.backup_context import BackupContext
from ch_backup.clickhouse.models import Table
from ch_backup.config import DEFAULT_CONFIG
from ch_backup.exceptions import StorageError, TerminatingSignal
from ch_backup.logic.table import TableBackup
from ch_backup.storage.async_pipeline import pipeline_executor
from ch_backup.storage.async_pipeline.base_pipeline.exec_pool import ThreadExecPool

BAD_PART = "all_1_1_0"
GOOD_PART = "all_2_2_0"


def _download_fixture(config, remote_path, local_path, *_args):
    """Write partial data before failing; this callable also runs in processes."""
    archive = remote_path.endswith(".tar")
    target = local_path / "data.bin" if archive else local_path
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(b"downloaded data")
    if config.get("fail_download", True) and BAD_PART in remote_path:
        if archive or remote_path.endswith("fail.bin"):
            raise OSError("injected download failure")


@pytest.fixture(name="restore_case")
def fixture_restore_case(tmp_path, monkeypatch):
    monkeypatch.setattr(logging.logger._core, "handlers", {})  # type: ignore[attr-defined]
    monkeypatch.setattr(pipeline_executor, "download_file_pipeline", _download_fixture)
    monkeypatch.setattr(pipeline_executor, "download_files_pipeline", _download_fixture)
    contexts = []

    def build(workers, tarball):
        config = copy.deepcopy(DEFAULT_CONFIG)
        config["multiprocessing"]["workers"] = workers
        config["backup"]["restore_context_path"] = str(tmp_path / "restore.json")
        config["backup"]["path_root"] = "ch_backup"
        context = BackupContext(config)  # type: ignore[arg-type]
        context.backup_meta = BackupMetadata("backup", "1", "23.3", "%Y-%m-%d")
        context.ch_ctl = Mock()
        table = Table("db", "tbl", "MergeTree", [], [], "", "", None)
        context.ch_ctl.get_table.return_value = table
        context.ch_ctl.get_detached_part_path.side_effect = (
            lambda _table, _disk, name: str(tmp_path / name)
        )
        context.backup_layout._storage_loader._engine = Mock()
        metadata = TableMetadata("db", "tbl", "MergeTree", None)
        for name in (BAD_PART, GOOD_PART):
            metadata.add_part(
                PartMetadata("db", "tbl", name, "", 1, ["fail.bin", "ok.bin"], tarball)
            )
        contexts.append(context)
        return context, metadata

    yield build
    for context in contexts:
        pool = context.backup_layout._storage_loader._ploader._exec_pool
        if pool:
            pool.shutdown()


def _state(context):
    return json.loads(
        Path(context.config["restore_context_path"]).read_text(encoding="utf-8")
    )


@pytest.mark.parametrize("workers", [0, 2])
@pytest.mark.parametrize("tarball", [True, False])
@pytest.mark.parametrize("keep_going", [True, False])
def test_failed_download_is_never_attached(restore_case, workers, tarball, keep_going):
    context, metadata = restore_case(workers, tarball)
    error: BaseException | None = None
    try:
        TableBackup._restore_data(context, [metadata], Mock(), False, keep_going)
    except Exception as caught:
        error = caught

    state = _state(context)
    assert state["databases"]["db"]["tbl"][BAD_PART] == "invalid"
    assert state["failed"]["db"]["tbl"]["failed_parts"] == {
        BAD_PART: "OSError('injected download failure')"
    }
    attached = [call.args[1] for call in context.ch_ctl.attach_part.call_args_list]
    assert attached == ([GOOD_PART] if keep_going else [])
    if keep_going:
        assert error is None
        assert state["databases"]["db"]["tbl"][GOOD_PART] == "restored"
    else:
        assert error is not None
        while isinstance(error, StorageError) and error.__cause__ is not None:
            error = error.__cause__
        assert isinstance(error, OSError)
        assert str(error) == "injected download failure"


@pytest.mark.parametrize("tarball", [True, False])
def test_retry_reloads_invalid_and_skips_restored_parts(restore_case, tarball):
    context, metadata = restore_case(2, tarball)
    TableBackup._restore_data(context, [metadata], Mock(), False, True)
    context.backup_layout._storage_loader._ploader._config["fail_download"] = False
    context.restore_context = RestoreContext(context.config)
    context.ch_ctl.attach_part.reset_mock()

    TableBackup._restore_data(context, [metadata], Mock(), False, False)

    assert [call.args[1] for call in context.ch_ctl.attach_part.call_args_list] == [
        BAD_PART
    ]
    assert _state(context)["databases"]["db"]["tbl"] == {
        BAD_PART: "restored",
        GOOD_PART: "restored",
    }
    assert not context.restore_context.has_failed_parts()


def test_downloaded_part_is_attached_without_download(restore_case, monkeypatch):
    context, metadata = restore_case(0, True)
    parts = list(metadata.get_parts())
    context.restore_context.change_part_state(PartState.DOWNLOADED, parts[0])
    context.restore_context.change_part_state(PartState.RESTORED, parts[1])
    download = Mock(side_effect=AssertionError("unexpected download"))
    monkeypatch.setattr(context.backup_layout, "download_data_part", download)
    TableBackup._restore_data(context, [metadata], Mock(), False, False)
    assert [call.args[1] for call in context.ch_ctl.attach_part.call_args_list] == [
        parts[0].name
    ]
    assert all(context.restore_context.part_restored(part) for part in parts)


def test_partial_submission_never_completes_part(restore_case, monkeypatch, tmp_path):
    context, metadata = restore_case(2, False)
    part = next(p for p in metadata.get_parts() if p.name == GOOD_PART)
    layout = context.backup_layout
    pool = layout._storage_loader._ploader._exec_pool
    submit = pool.submit
    error = OSError("injected submit failure")

    def submit_first(job_id, *args, **kwargs):
        if "ok.bin" in job_id:
            raise error
        return submit(job_id, *args, **kwargs)

    monkeypatch.setattr(pool, "submit", submit_first)
    completed = Mock()
    failed = Mock()
    with pytest.raises(OSError, match="injected submit failure"):
        layout.download_data_part(
            context.backup_meta, part, str(tmp_path / "partial"), completed, failed
        )
    layout.wait(keep_going=True)
    failed.assert_called_once_with(part, error)
    completed.assert_not_called()


@pytest.mark.parametrize(
    "failure", [BrokenExecutor("broken pool"), TerminatingSignal("stop")]
)
def test_fatal_submission_error_is_not_skipped(restore_case, monkeypatch, failure):
    context, metadata = restore_case(2, True)
    pool = context.backup_layout._storage_loader._ploader._exec_pool
    monkeypatch.setattr(pool, "submit", Mock(side_effect=failure))
    with pytest.raises(type(failure), match=str(failure)):
        TableBackup._restore_data(context, [metadata], Mock(), False, True)
    context.ch_ctl.attach_part.assert_not_called()


def test_preparation_error_is_recorded_before_any_download(restore_case, monkeypatch):
    context, metadata = restore_case(0, True)
    monkeypatch.setattr(
        context.backup_layout._storage_loader,
        "path_exists",
        Mock(side_effect=OSError("cannot list backup")),
    )
    TableBackup._restore_data(context, [metadata], Mock(), False, True)
    state = _state(context)
    assert state["databases"]["db"]["tbl"] == {
        BAD_PART: "invalid",
        GOOD_PART: "invalid",
    }
    assert state["failed"]["db"]["tbl"]["failed_parts"] == {
        name: "OSError('cannot list backup')" for name in (BAD_PART, GOOD_PART)
    }
    context.ch_ctl.attach_part.assert_not_called()


@pytest.mark.parametrize("late_failure", [False, True])
def test_late_file_completion_preserves_first_error(
    restore_case, monkeypatch, tmp_path, late_failure
):
    context, metadata = restore_case(0, False)
    part = next(p for p in metadata.get_parts() if p.name == BAD_PART)
    layout = context.backup_layout
    failed = Event()
    errors = []
    completed = Mock()

    def download(_config, remote_path, *_args):
        if remote_path.endswith("fail.bin"):
            raise OSError("first download error")
        assert failed.wait(10), "the first error was not collected"
        if late_failure:
            raise ValueError("later download error")

    def on_error(failed_part, error):
        assert failed_part is part
        errors.append(error)
        failed.set()

    monkeypatch.setattr(pipeline_executor, "download_file_pipeline", download)
    with ThreadExecPool(2) as pool:
        monkeypatch.setattr(layout._storage_loader._ploader, "_exec_pool", pool)
        layout.download_data_part(
            context.backup_meta, part, str(tmp_path / "late"), completed, on_error
        )
        layout.wait(keep_going=True)
    assert len(errors) == 1
    assert isinstance(errors[0], OSError)
    assert str(errors[0]) == "first download error"
    completed.assert_not_called()
