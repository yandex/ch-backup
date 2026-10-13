"""Copy reports preserve all errors without retaining them all in memory."""

# pylint: disable=protected-access

import json
import tracemalloc
from pathlib import Path

import pytest

from ch_backup.backup.copy_report import (
    BackupCopyResult,
    CopyErrors,
    CopyReport,
    _write_report,
)


def test_error_log_supports_repeated_reads_and_appends() -> None:
    errors = CopyErrors()
    assert not errors
    with pytest.raises(IndexError):
        _ = errors[0]
    errors.append({"message": "first"})
    reader = iter(errors)
    assert next(reader) == errors[0]
    errors.append({"message": "second"})
    assert errors[-1] == {"message": "second"}
    assert errors == [{"message": "first"}, {"message": "second"}]
    assert list(errors) == list(errors)


def test_large_report_spills_errors_and_streams_json(tmp_path: Path) -> None:
    result = BackupCopyResult("large")
    report = CopyReport(backups=[result])
    path = tmp_path / "report.json"
    tracemalloc.start()
    try:
        for index in range(10000):
            result.add_error(
                RuntimeError("x" * 256),
                "copy",
                source=f"source/{index}",
                object_failed=True,
            )
        with path.open("w", encoding="utf-8") as stream:
            _write_report(report, stream)
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    assert peak < 2 * 1024 * 1024
    saved = json.loads(path.read_text())["backups"][0]
    assert saved["objects_failed"] == 10000
    assert len(saved["errors"]) == 10000
    assert saved["errors"][0]["source"] == "source/0"
    assert saved["errors"][-1]["source"] == "source/9999"
    assert len(result.errors) == 10000
