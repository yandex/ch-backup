"""
Unit tests for the journal of copied objects.
"""

from typing import Any
from unittest.mock import patch

from ch_backup.backup.copy_context import CopyContext
from ch_backup.exceptions import StorageError
from ch_backup.storage.object_copy import CopyEntry
from tests.unit.test_backup_copy import _backup

STORAGE = {
    "type": "s3",
    "credentials": {"endpoint_url": "http://s3.test:9000", "bucket": "copy"},
}


class MemoryEngine:
    """
    Storage engine keeping objects in memory.
    """

    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}
        self.upload_error: Exception | None = None

    def list_dir_with_sizes(self, remote_path: str) -> dict[str, int]:
        prefix = remote_path.strip("/") + "/"
        return {k: len(v) for k, v in self.objects.items() if k.startswith(prefix)}

    def upload_data(self, data: bytes, remote_path: str) -> str:
        if self.upload_error:
            raise self.upload_error
        self.objects[remote_path] = data
        return remote_path

    def download_data(self, remote_path: str) -> bytes:
        return self.objects[remote_path]


def _entry(name: str, size: int = 1) -> CopyEntry:
    return CopyEntry(STORAGE, STORAGE, f"src/{name}", f"dst/{name}", size)


def _context(engine: MemoryEngine, **kwargs: Any) -> CopyContext:
    return CopyContext(engine, "/backups/b1", _backup("b1"), **kwargs)  # type: ignore[arg-type]


def test_records_are_read_by_next_run() -> None:
    engine = MemoryEngine()
    context = _context(engine)
    context.record(_entry("a"))
    context.flush(force=True)

    resumed = _context(engine)
    resumed.load()

    assert resumed.has_records()
    assert resumed.is_recorded(_entry("a"))
    assert not resumed.is_recorded(_entry("a", size=2))
    assert not resumed.is_recorded(_entry("b"))
    assert all(path.startswith("backups/b1/copy_context/") for path in engine.objects)


def test_runs_write_separate_files() -> None:
    engine = MemoryEngine()
    for name in ("a", "b"):
        context = _context(engine)
        context.record(_entry(name))
        context.flush(force=True)

    resumed = _context(engine)
    resumed.load()

    assert len(engine.objects) == 2
    assert resumed.is_recorded(_entry("a")) and resumed.is_recorded(_entry("b"))


def test_records_of_another_backup_are_ignored() -> None:
    engine = MemoryEngine()
    context = _context(engine)
    context.record(_entry("a"))
    context.flush(force=True)

    other = CopyContext(
        engine,  # type: ignore[arg-type]
        "/backups/b1",
        _backup("b1", start_time="2026-01-02 00:00:00 +0000"),
    )
    other.load()

    assert not other.has_records()


def test_flush_waits_for_size_or_interval() -> None:
    engine = MemoryEngine()
    with patch("ch_backup.backup.copy_context.time.monotonic", return_value=0):
        context = _context(engine, flush_size=2, flush_interval=60)
    with patch("ch_backup.backup.copy_context.time.monotonic", return_value=59):
        context.record(_entry("a"))
        context.flush()
        assert not engine.objects
        context.record(_entry("b"))
        context.flush()
        assert len(engine.objects) == 1
    with patch("ch_backup.backup.copy_context.time.monotonic", return_value=120):
        context.record(_entry("c"))
        context.flush()
        assert len(engine.objects) == 2


def test_failed_flush_keeps_records() -> None:
    engine = MemoryEngine()
    context = _context(engine)
    context.record(_entry("a"))
    engine.upload_error = StorageError("SlowDown")
    context.flush(force=True)
    engine.upload_error = None
    context.flush(force=True)

    resumed = _context(engine)
    resumed.load()

    assert resumed.is_recorded(_entry("a"))


def test_failed_flush_waits_for_size_or_interval() -> None:
    engine = MemoryEngine()
    with patch("ch_backup.backup.copy_context.time.monotonic", return_value=0):
        context = _context(engine, flush_size=2, flush_interval=60)
        context.record(_entry("a"))
        context.record(_entry("b"))
        engine.upload_error = StorageError("SlowDown")
        context.flush()
        engine.upload_error = None
        context.record(_entry("c"))
        context.flush()
        assert not engine.objects
        context.record(_entry("d"))
        context.flush()
        assert len(engine.objects) == 1


def test_unreadable_file_is_skipped() -> None:
    engine = MemoryEngine()
    context = _context(engine)
    context.record(_entry("a"))
    context.flush(force=True)
    context.record(_entry("b"))
    context.flush(force=True)
    engine.objects[sorted(engine.objects)[0]] = b"not gzip"

    resumed = _context(engine)
    resumed.load()

    assert not resumed.is_recorded(_entry("a"))
    assert resumed.is_recorded(_entry("b"))
