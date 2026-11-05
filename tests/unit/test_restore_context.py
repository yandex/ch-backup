"""Persist old diagnostics independently of the current command's exit status."""

import json

import pytest

from ch_backup.backup.metadata import PartMetadata
from ch_backup.backup.restore_context import PartState, RestoreContext


@pytest.fixture(name="renamed_part_state")
def fixture_renamed_part_state(tmp_path):
    path = tmp_path / "restore.json"
    config = {
        "restore_context_path": str(path),
        "restore_context_sync_threshold_ops": 1,
    }
    part = PartMetadata(
        "db",
        "table",
        "all_1_1_0_7",
        "c",
        1,
        [],
        True,
        link="owner",
        link_part_name="all_1_1_0",
    )
    return path, config, part


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


@pytest.mark.parametrize(
    "field,value,restored",
    [
        ("database", "other", False),
        ("table", "other", False),
        ("name", "all_1_1_0", False),
        ("name", "all_1_1_0_8", False),
        ("link", "another_owner", True),
        ("link_part_name", "another_part", True),
    ],
)
def test_restore_progress_follows_requested_address(
    renamed_part_state, field, value, restored
):
    """
    Restore progress belongs to the requested table and part, not its current
    storage owner. Otherwise owner changes repeat completed work, or another
    requested part inherits completion and is skipped.
    """
    _, config, part = renamed_part_state
    context = RestoreContext(config)
    context.change_part_state(PartState.RESTORED, part)
    if field in ("link", "link_part_name"):
        setattr(part.raw_metadata, field, value)
    else:
        setattr(part, field, value)
    assert RestoreContext(config).part_restored(part) is restored
    assert not context.part_downloaded(
        PartMetadata("db", "table", "unknown", "c", 1, [], True)
    )


def test_renamed_failure_and_unread_state_survive_dump(renamed_part_state):
    """
    After restart, restore needs both progress and failure diagnostics under
    the requested name. An initial dump must preserve unread saved state, and
    successful retry must clear the old failure.
    """
    path, config, part = renamed_part_state
    config["restore_context_sync_threshold_ops"] = 2
    context = RestoreContext(config)
    context.change_part_state(PartState.DOWNLOADED, part)
    assert not path.exists()
    context.add_failed_part(part, ValueError("attach failed"))
    saved = json.loads(path.read_text())
    assert saved["databases"] == {"db": {"table": {"all_1_1_0_7": "downloaded"}}}
    assert saved["failed"]["db"]["table"]["failed_parts"] == {
        "all_1_1_0_7": "ValueError('attach failed')"
    }
    RestoreContext(config).dump_state()
    assert json.loads(path.read_text()) == saved
    loaded = RestoreContext(config)
    assert loaded.part_downloaded(part)
    loaded.change_part_state(PartState.RESTORED, part)
    loaded.dump_state()
    assert RestoreContext(config).part_restored(part)
    assert json.loads(path.read_text())["failed"] == {}
