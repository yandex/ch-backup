"""Operator output must distinguish planned work, confirmed work, and uncertain publication."""

# pylint: disable=protected-access

import copy
import json
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from click import Group
from click.testing import CliRunner

from ch_backup import cli, logging
from ch_backup.backup.cloud_storage_decryption import CloudStorageDecryption
from ch_backup.backup.copy_context import CopyContext
from ch_backup.backup.copy_report import CopyReport, print_copy_report
from ch_backup.backup.deduplication import (
    collect_dedup_references_for_batch_backup_deletion,
)
from ch_backup.backup.metadata import BackupState
from ch_backup.ch_backup import ClickhouseBackup
from tests.unit.test_cloud_generations import _add_same_source, _generation_context


def test_copy_report_distinguishes_dedup_from_resume(tmp_path, capsys):
    ctx = _generation_context()
    _add_same_source(ctx, "child")
    ctx.copy(["owner"])
    report = CopyReport()
    ctx.copy(["child"], report=report)
    path = tmp_path / "report.json"
    print_copy_report(report, str(path))
    result = json.loads(path.read_text())["backups"][0]
    assert result["cloud_parts_reused"] == 1
    assert result["cloud_parts_to_copy"] == 0
    assert result["objects_skipped"] == 0
    assert (
        result["cloud_generation"]
        == ctx.target_layout.backups["child"].cloud_generation
    )
    assert result["generation_action"] == "allocated"
    assert result["generation_publication"] == "published"
    assert "cloud part plan: 1 reused, 0 to copy" in capsys.readouterr().out
    assert "secret_access_key" not in path.read_text()


def test_failed_publication_is_not_reported_as_preserved(messages):
    ctx = _generation_context()
    ctx.copy(["owner"])
    ctx.target_config["copy"]["resume"] = False
    upload = ctx.target_layout.upload_backup_metadata

    def lose_publication_ack(backup):
        upload(backup)
        if backup.name == "owner" and backup.state == BackupState.CREATED:
            raise RuntimeError("lost publication response")

    report = CopyReport()
    messages.clear()
    with patch.object(
        ctx.target_layout, "upload_backup_metadata", side_effect=lose_publication_ack
    ):
        with pytest.raises(RuntimeError, match="lost publication response"):
            ctx.copy(["owner"], report=report)
    assert report.backups[0].generation_publication == "unconfirmed"
    assert not any(
        "previous generation preserved" in record["message"] for record in messages
    )


def test_failed_journal_write_does_not_claim_publication_was_undone():
    ctx = _generation_context()
    ctx.copy(["owner"])
    ctx.target_config["copy"]["resume"] = False
    save = CopyContext.save_generation_attempt

    def fail_after_publication(context, attempt):
        if attempt.get("metadata", {}).get("meta", {}).get("state") == "created":
            raise RuntimeError("journal failed")
        save(context, attempt)

    report = CopyReport()
    with patch.object(CopyContext, "save_generation_attempt", fail_after_publication):
        with pytest.raises(RuntimeError, match="journal failed"):
            ctx.copy(["owner"], report=report)
    assert report.backups[0].generation_publication == "published"
    assert (
        ctx.target_layout.backups["owner"].cloud_generation
        == report.backups[0].cloud_generation
    )


def _inspection_manager(ctx):
    manager = ClickhouseBackup.__new__(ClickhouseBackup)
    layout = MagicMock(wraps=ctx.target_layout)
    layout.get_backups.side_effect = lambda **_: list(
        ctx.target_layout.backups.values()
    )
    manager.__dict__["_context"] = SimpleNamespace(backup_layout=layout)
    return manager, layout


def test_inspection_explains_old_generation_and_pending_references():
    ctx = _generation_context()
    _add_same_source(ctx, "child")
    ctx.copy(["owner", "child"])
    previous = ctx.target_layout.backups["owner"].cloud_generation
    ctx.target_config["copy"]["resume"] = False
    ctx.copy(["owner"])
    manager, _ = _inspection_manager(ctx)
    result = manager.inspect_cloud_owners("owner")
    owners = {owner["name"]: owner for owner in result["owners"]}
    assert result["references_complete"]
    assert owners[previous]["referenced_by"] == ["child"]
    assert owners[previous]["current"] is False
    assert owners[result["current_generation"]]["referenced_by"] == ["owner"]
    pending = copy.deepcopy(ctx.target_layout.backups["owner"])
    pending.name = "pending"
    pending.state = BackupState.CREATING
    ctx.target_layout.backups[pending.name] = pending
    pending.get_table("db1", "t1").raw_metadata["parts"].clear()
    result = manager.inspect_cloud_owners("pending")
    assert result["current_generation"] is None
    assert result["pending_generation"] == pending.cloud_generation
    assert result["owners"][0]["pending_copies"] == ["pending"]
    result = manager.inspect_cloud_owners(previous)
    assert result["owners"][0]["name"] == previous
    assert result["owners"][0]["referenced_by"] == ["child"]


def test_inspection_marks_unreadable_references_as_incomplete():
    """The CLI must expose an incomplete scan, including for internal owner names."""
    ctx = _generation_context()
    ctx.copy(["owner"])
    manager, layout = _inspection_manager(ctx)

    def read_backups(*, unreadable, **_):
        unreadable["unreadable-child"] = "cannot read metadata"
        return list(ctx.target_layout.backups.values())

    layout.get_backups.side_effect = read_backups
    group = Group()
    group.add_command(cli.show_command)
    generation = ctx.target_layout.backups["owner"].cloud_generation
    command = CliRunner().invoke(
        group, ["show", generation, "--cloud-owners"], obj={"backup": manager}
    )
    assert command.exit_code == 2
    result = json.loads(command.output)
    assert not result["references_complete"]
    assert "unreadable-child" in result["metadata_errors"]


def test_inspection_reports_missing_owner_instead_of_empty_dependencies():
    ctx = _generation_context()
    ctx.copy(["owner"])
    missing = ctx.target_layout.backups["owner"].cloud_generation
    del ctx.target_layout.backups[missing]
    manager, _ = _inspection_manager(ctx)
    result = manager.inspect_cloud_owners("owner")
    assert result["references_complete"] is False
    assert missing in result["metadata_errors"]
    assert result["owners"][0]["referenced_by"] == ["owner"]


def test_inspection_accepts_reclaimed_empty_generation_of_all_linked_child():
    ctx = _generation_context()
    _add_same_source(ctx, "child")
    ctx.copy(["owner", "child"])
    child = ctx.target_layout.backups["child"]
    empty_generation = child.cloud_generation
    assert not ctx.target_layout.backups[empty_generation].get_parts()
    del ctx.target_layout.backups[empty_generation]
    manager, _ = _inspection_manager(ctx)

    result = manager.inspect_cloud_owners("child")

    assert result["references_complete"]
    assert [owner["name"] for owner in result["owners"]] == [
        ctx.target_layout.backups["owner"].cloud_generation
    ]


def test_retention_log_identifies_the_dependent_backup(messages):
    ctx = _generation_context()
    _add_same_source(ctx, "child")
    ctx.copy(["owner", "child"])
    generation = ctx.target_layout.backups["owner"].cloud_generation
    child = ctx.target_layout.backups["child"]
    layout = MagicMock()
    layout.reload_backup.return_value = child
    messages.clear()
    collect_dedup_references_for_batch_backup_deletion(layout, [child], {generation})
    assert any(
        generation in record["message"]
        and "dependent=child" in record["message"]
        and "parts=1" in record["message"]
        for record in messages
    )


def test_staging_cleanup_error_is_visible_and_identifies_location(messages):
    storage = copy.deepcopy(_generation_context().config["storage"])
    engine = MagicMock()
    engine.list_dir.return_value = ["staging/object"]
    engine.delete_files.side_effect = RuntimeError("delete failed")
    with patch(
        "ch_backup.backup.cloud_storage_decryption.get_storage_engine",
        return_value=engine,
    ):
        with pytest.raises(RuntimeError, match="delete failed"):
            CloudStorageDecryption._cleanup_prefix(storage, "staging")
    errors = [
        record["message"] for record in messages if record["level"].name == "ERROR"
    ]
    assert any(
        "staging" in error and "plaintext may remain" in error for error in errors
    )


def test_exception_logger_emits_error(messages):
    try:
        raise RuntimeError("cleanup failed")
    except RuntimeError:
        logging.exception("Secondary cleanup failure")
    assert messages[-1]["level"].name == "ERROR"
    assert messages[-1]["exception"] is not None
