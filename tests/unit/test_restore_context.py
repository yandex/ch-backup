"""Persist old diagnostics independently of the current command's exit status."""

import json

from ch_backup.backup.metadata import PartMetadata
from ch_backup.backup.restore_context import PartState, RestoreContext


def test_other_table_preserves_errors_until_successful_retry(tmp_path):
    path = tmp_path / "restore.json"
    config = {
        "restore_context_path": str(path),
        "restore_context_sync_threshold_ops": 1,
    }
    failed = PartMetadata("db", "failed", "all_1_1_0", "", 1, [], True)
    other = PartMetadata("db", "other", "all_1_1_0", "", 1, [], True)
    context = RestoreContext(config)
    context.add_failed_part(failed, OSError("original failure"))
    context.change_part_state(PartState.INVALID, failed)
    previous_errors = json.loads(path.read_text())["failed"]

    context = RestoreContext(config)
    context.change_part_state(PartState.RESTORED, other)
    state = json.loads(path.read_text())
    assert state["failed"] == previous_errors
    assert state["databases"]["db"]["failed"][failed.name] == "invalid"
    assert not context.has_failed_parts()

    context = RestoreContext(config)
    context.add_failed_chown(other.database, other.table, "/detached")
    context.change_part_state(PartState.DOWNLOADED, failed)
    assert (
        json.loads(path.read_text())["failed"]["db"]["failed"]
        == previous_errors["db"]["failed"]
    )
    context.change_part_state(PartState.RESTORED, failed)
    assert json.loads(path.read_text())["failed"] == {
        "db": {"other": {"failed_paths": ["/detached"], "failed_parts": {}}}
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
