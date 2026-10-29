"""Persist diagnostics without carrying failures into the next command's exit code."""

import json

import pytest

from ch_backup.backup.metadata import PartMetadata
from ch_backup.backup.restore_context import PartState, RestoreContext


@pytest.fixture(name="state_case")
def fixture_state_case(tmp_path):
    path = tmp_path / "restore.json"
    config = {
        "restore_context_path": str(path),
        "restore_context_sync_threshold_ops": 1,
    }
    part = PartMetadata("db", "failed_table", "all_1_1_0", "", 1, [], True)
    context = RestoreContext(config)
    context.add_failed_part(part, OSError("original download failure"))
    context.change_part_state(PartState.INVALID, part)
    return config, path, part


def test_other_table_preserves_previous_failure(state_case):
    config, path, failed = state_case
    previous_errors = json.loads(path.read_text())["failed"]
    context = RestoreContext(config)
    other = PartMetadata("other_db", "other_table", "all_1_1_0", "", 1, [], True)
    context.change_part_state(PartState.RESTORED, other)
    state = json.loads(path.read_text())
    assert state["failed"] == previous_errors
    assert state["databases"][failed.database][failed.table][failed.name] == "invalid"
    assert not context.has_failed_parts()


@pytest.mark.parametrize("chown_error", [False, True])
def test_new_error_loads_previous_diagnostics_first(state_case, chown_error):
    config, path, part = state_case
    context = RestoreContext(config)
    if chown_error:
        context.add_failed_chown(part.database, part.table, "/detached")
    else:
        other = PartMetadata(part.database, part.table, "all_2_2_0", "", 1, [], True)
        context.add_failed_part(other, OSError("new download failure"))
    errors = json.loads(path.read_text())["failed"][part.database][part.table]
    assert errors["failed_parts"][part.name] == "OSError('original download failure')"
    if chown_error:
        assert errors["failed_paths"] == ["/detached"]
    else:
        assert errors["failed_parts"][other.name] == "OSError('new download failure')"
    assert context.has_failed_parts()


def test_successful_retry_clears_only_recovered_part_error(state_case):
    config, path, part = state_case
    context = RestoreContext(config)
    context.change_part_state(PartState.DOWNLOADED, part)
    assert json.loads(path.read_text())["failed"][part.database][part.table][
        "failed_parts"
    ]
    context.change_part_state(PartState.RESTORED, part)
    assert json.loads(path.read_text())["failed"] == {}
    assert not context.has_failed_parts()


def test_recovery_keeps_other_diagnostics_and_current_exit_status(state_case):
    config, path, part = state_case
    context = RestoreContext(config)
    context.add_failed_chown(part.database, part.table, "/detached")
    context.change_part_state(PartState.RESTORED, part)
    assert json.loads(path.read_text())["failed"][part.database][part.table] == {
        "failed_paths": ["/detached"],
        "failed_parts": {},
    }
    assert context.has_failed_parts()


def test_legacy_state_without_diagnostics(tmp_path):
    path = tmp_path / "legacy.json"
    path.write_text(
        json.dumps({"databases": {"db": {"tbl": {"all_1_1_0": "downloaded"}}}})
    )
    context = RestoreContext(
        {"restore_context_path": str(path), "restore_context_sync_threshold_ops": 1}
    )
    part = PartMetadata("db", "tbl", "all_1_1_0", "", 1, [], True)
    assert context.part_downloaded(part)
    context.change_part_state(PartState.RESTORED, part)
    assert json.loads(path.read_text())["failed"] == {}
    assert not context.has_failed_parts()
