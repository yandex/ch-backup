"""Operator output must distinguish planned work, confirmed work, and uncertain publication."""

# pylint: disable=protected-access

import copy
import json
from unittest.mock import MagicMock, patch

import pytest

from ch_backup import logging
from ch_backup.backup.cloud_storage_decryption import CloudStorageDecryption
from ch_backup.backup.copy_report import BackupCopyResult, CopyReport, print_copy_report
from ch_backup.backup.deduplication import (
    collect_dedup_references_for_batch_backup_deletion,
)
from ch_backup.config import DEFAULT_CONFIG
from tests.unit.test_backup_copy import _backup, _part


def test_copy_report_distinguishes_dedup_from_resume(tmp_path, capsys):
    report = CopyReport(
        backups=[BackupCopyResult("child", status="copied", cloud_parts_reused=1)]
    )
    path = tmp_path / "report.json"
    print_copy_report(report, str(path))
    result = json.loads(path.read_text())["backups"][0]
    assert result["cloud_parts_reused"] == 1
    assert result["cloud_parts_to_copy"] == 0
    assert result["objects_skipped"] == 0
    assert "cloud part plan: 1 reused, 0 to copy" in capsys.readouterr().out
    assert "secret_access_key" not in path.read_text()


def _cloud_backup(name, owner=None):
    return _backup(
        name,
        parts={"all_1_1_0": {**_part(link=owner), "disk_name": "s3"}},
        cloud_storage_disks=["s3"],
    )


def test_retention_log_identifies_the_dependent_backup(messages):
    child = _cloud_backup("child", "owner")
    layout = MagicMock()
    layout.reload_backup.return_value = child
    messages.clear()
    collect_dedup_references_for_batch_backup_deletion(layout, [child], {"owner"})
    assert any(
        "owner" in record["message"]
        and "dependent=child" in record["message"]
        and "parts=1" in record["message"]
        for record in messages
    )


def test_staging_cleanup_error_is_visible_and_identifies_location(messages):
    storage = copy.deepcopy(DEFAULT_CONFIG["storage"])
    engine = MagicMock()
    storage["bulk_delete_chunk_size"] = 1
    engine.list_dir.return_value = ["staging/first", "staging/second"]
    engine.delete_files.side_effect = [None, RuntimeError("delete failed")]
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
        "staging" in error
        and "plaintext may remain" in error
        and "confirmed=1" in error
        for error in errors
    )


def test_exception_logger_emits_error(messages):
    try:
        raise RuntimeError("cleanup failed")
    except RuntimeError:
        logging.exception("Secondary cleanup failure")
    assert messages[-1]["level"].name == "ERROR"
    assert messages[-1]["exception"] is not None
