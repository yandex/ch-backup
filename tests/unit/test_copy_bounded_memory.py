"""Check bounded copy structures and preservation of backup safety rules."""

# pylint: disable=protected-access

import copy
import gc
import gzip
import json
import subprocess
import sys
import tarfile
import threading
from collections import Counter
from pathlib import Path
from unittest.mock import patch

import pytest

from ch_backup.backup.copier import _add_cloud_storage_copy_entry
from ch_backup.backup.copy_context import _archive_record
from ch_backup.backup.copy_index import CopyEntries, CopyIndex, EntryIndex
from ch_backup.compression.gzip import GZIPCompression
from ch_backup.config import DEFAULT_CONFIG
from ch_backup.exceptions import ClickhouseBackupError, StorageError
from ch_backup.storage.async_pipeline.base_pipeline.exec_pool import ThreadExecPool
from ch_backup.storage.async_pipeline.pipeline_builder import PipelineBuilder
from ch_backup.storage.async_pipeline.pipelines import run_and_collect_all
from ch_backup.storage.metadata_archive import TemporaryMetadataArchive, spool_metadata
from ch_backup.storage.object_copy import CopyEntry
from tests.unit.test_backup_copy import _backup, _part
from tests.unit.test_copy_context import STORAGE, MemoryEngine, _context, _entry


@pytest.mark.parametrize("split", [1, 37, 1000000])
def test_decompression_limits_each_queued_chunk(split: int) -> None:
    """Expansion must remain bounded before reaching the rechunking stage."""
    data = b"a" * (3 * 1024 * 1024 + 31)
    compressed = gzip.compress(data)
    config = copy.deepcopy(DEFAULT_CONFIG)
    config["filesystem"]["chunk_size"] = 4096
    builder = PipelineBuilder(config).build_iterable_stage(
        compressed[i : i + split] for i in range(0, len(compressed), split)
    )
    builder.build_decompress_stage()
    chunks = run_and_collect_all(builder.pipeline())
    assert b"".join(chunks) == data
    assert max(map(len, chunks)) <= 4096


def test_decompression_keeps_full_flush_stream_tail() -> None:
    """Keep the existing gzip format, including a stream finished by Z_FULL_FLUSH."""
    compression = GZIPCompression()
    data = b"metadata" * 17000
    encoded = compression.compress(data) + compression.flush_compress()
    chunks = list(compression.decompress_chunks(encoded, 1024))
    chunks.extend(compression.decompress_chunks(b"", 1024))
    assert b"".join(chunks) + compression.flush_decompress() == data
    assert max(map(len, chunks)) <= 1024
    with pytest.raises(ValueError):
        list(GZIPCompression().decompress_chunks(encoded, 0))


def test_metadata_spool_preserves_long_names_empty_files_and_digest() -> None:
    """Preserve tar members and the old resume digest without retaining file contents."""
    files = [("part/empty", b""), ("part/" + "long" * 50, b"data" * 300000)]
    archive = spool_metadata(iter(files))
    assert archive.count_files() == 2
    assert _read_archive(archive) == files
    assert _archive_record(STORAGE, "archive", archive, 123) == _archive_record(
        STORAGE, "archive", files, 123
    )
    path = Path(archive.path())
    assert path.stat().st_mode & 0o077 == 0
    del archive
    gc.collect()
    assert not path.exists()


def test_downloaded_metadata_archive_is_reiterable(tmp_path: Path) -> None:
    """Read a downloaded tar without a cached member list."""
    original = spool_metadata([("part/data", b"payload")])
    path = tmp_path / "downloaded.tar"
    path.write_bytes(Path(original.path()).read_bytes())
    downloaded = TemporaryMetadataArchive(str(path))
    assert downloaded.count_files() == 1
    assert (
        _read_archive(downloaded)
        == _read_archive(downloaded)
        == [("part/data", b"payload")]
    )


def _read_archive(archive: TemporaryMetadataArchive) -> list[tuple[str, bytes]]:
    """Collect explicit archive batches for small fixture assertions."""
    result = []
    continuation = None
    while True:
        batch, continuation = archive.read_batch(continuation)
        result.extend(batch)
        if continuation is None:
            return result


def _read_plan(entries: CopyEntries, limit: int = 128) -> list[CopyEntry]:
    """Collect explicit bounded reads for assertions on small test plans."""
    result = []
    continuation = None
    while True:
        batch, continuation = entries.read_batch(continuation, limit)
        assert len(batch) <= limit
        result.extend(batch)
        if continuation is None:
            return result


def test_entry_index_rejects_cross_part_conflicts_and_keeps_secrets_in_memory() -> None:
    """Reject conflicting destinations and preserve credentials only in memory."""
    storage: dict = copy.deepcopy(STORAGE)
    storage["credentials"]["secret_access_key"] = "DO_NOT_PERSIST_SOURCE"
    target: dict = copy.deepcopy(STORAGE)
    target["credentials"]["access_key_id"] = "DO_NOT_PERSIST_TARGET"
    index = EntryIndex()
    first = CopyEntry(storage, target, "source/a", "target/shared", 1, 1)
    _add_cloud_storage_copy_entry(index, first)
    _add_cloud_storage_copy_entry(index, first)
    assert len(index) == 1
    for conflicting in (
        first._replace(src_path="source/b"),
        first._replace(size=2),
        first._replace(
            src_storage={
                "type": "s3",
                "credentials": {
                    "bucket": "other",
                    "endpoint_url": "http://s3.test:9000",
                },
            }
        ),
    ):
        with pytest.raises(ClickhouseBackupError):
            _add_cloud_storage_copy_entry(index, conflicting)
    decoded = index.read_batch(after=0, limit=1)[0][2]
    assert decoded == first
    assert decoded.src_storage is storage
    assert decoded.dst_storage is target
    blobs = index._index.read_batch(after=0, limit=128)
    for _, _, blob in blobs:
        assert b"DO_NOT_PERSIST" not in blob
    database = index._index._db
    database.commit()
    path = Path(database.execute("PRAGMA database_list").fetchone()[2])
    assert b"DO_NOT_PERSIST" not in path.read_bytes()
    del index
    gc.collect()
    assert not path.exists()


def test_plan_and_listing_read_in_order_and_roll_back_incomplete_import() -> None:
    """Keep existing confirmations when a later journal chunk is corrupt."""
    entries = CopyEntries(_entry(str(i)) for i in range(300))
    queries: list[str] = []
    entries._index._index._db.set_trace_callback(queries.append)
    assert len(entries) == 300
    assert [entry.src_path for entry in _read_plan(entries)] == [
        f"src/{i}" for i in range(300)
    ]
    assert len(queries) == 3
    records = CopyIndex()
    records.put("old", b"confirmed")
    with pytest.raises(ValueError):
        with records.importing():
            records.put("old", b"changed")
            for i in range(70000):
                records.put(str(i), b"")
            raise ValueError("truncated gzip")
    assert records.get("old") == b"confirmed"
    assert records.read_batch(after=0, limit=128) == [(1, "old", b"confirmed")]
    assert len(records) == 1
    with records.importing():
        records.put("next", b"")
    assert records.contains("next")


def test_entry_insert_only_reads_duplicate_keys() -> None:
    """Avoid lookups for new destinations and preserve the first duplicate."""
    index = EntryIndex()
    queries: list[str] = []
    index._index._db.set_trace_callback(queries.append)
    first = _entry("first")
    assert index.get_or_insert("first", first) is first
    assert index.get_or_insert("last", _entry("last")) == _entry("last")
    assert not any(query.startswith("SELECT") for query in queries)
    assert index.get_or_insert("first", first._replace(size=42)) == first
    assert sum(query.startswith("SELECT") for query in queries) == 1
    assert len(index) == 2
    assert [entry for _, _, entry in index.read_batch(0, 128)] == [
        first,
        _entry("last"),
    ]


@pytest.mark.parametrize("deduplicate", [False, True])
def test_index_batches_commits(deduplicate: bool) -> None:
    """Commit larger groups on both write paths and keep a bounded page cache."""
    index = CopyIndex()
    statements: Counter[str] = Counter()

    def count_statement(statement: str) -> None:
        """Count operations without retaining their keys and values."""
        statements[statement.split()[0]] += 1

    index._db.set_trace_callback(count_statement)
    for number in range(65537):
        if deduplicate:
            assert index.put_if_absent(str(number), b"entry") is None
        else:
            index.put(str(number), b"entry")
    assert statements["COMMIT"] == 1
    assert len(index) == 65537
    assert index.get("65536") == b"entry"
    assert index._db.execute("PRAGMA cache_size").fetchone()[0] == -65536
    assert index._db.execute("PRAGMA mmap_size").fetchone()[0] == 0
    assert index._db.execute("PRAGMA synchronous").fetchone()[0] == 0
    assert index._db.execute("PRAGMA journal_mode").fetchone()[0] == "delete"
    assert index._db.execute("PRAGMA cache_spill").fetchone()[0] > 0


@pytest.mark.parametrize("keep_every", [0, 1, 2, 299])
@pytest.mark.parametrize("size", [0, 300])
def test_filtered_plan_preserves_source_and_order(keep_every: int, size: int) -> None:
    """Filter multiple segments without rewriting entries or the full manifest."""
    original = [_entry(str(i)) for i in range(size)]
    source = CopyEntries(original[:150])
    extra = EntryIndex()
    for entry in original[150:]:
        extra.put(entry.dst_path, entry)
    source.extend_index(extra)
    with patch.object(
        EntryIndex, "_encode", side_effect=AssertionError("rewrote plan")
    ):
        selected = source.filtered(
            lambda entry: bool(keep_every)
            and int(entry.src_path.split("/")[-1]) % keep_every == 0
        )
    expected = original[::keep_every] if keep_every else []
    assert len(selected) == len(expected)
    assert _read_plan(selected) == _read_plan(selected, 1) == expected
    assert _read_plan(source) == original
    source.append(_entry("appended later"))
    assert _read_plan(selected) == expected
    del source
    gc.collect()
    for entry in _read_plan(selected):
        assert entry.src_storage is STORAGE
    selected.append(_entry("new"))
    assert _read_plan(selected) == expected + [_entry("new")]
    assert _read_plan(selected.filtered(lambda entry: entry.src_path == "src/new")) == [
        _entry("new")
    ]


def test_filtered_plan_advances_empty_batches_across_excluded_ranges() -> None:
    """An empty batch must retain progress until a later included entry."""
    source = CopyEntries(_entry(str(i)) for i in range(500))
    selected = source.filtered(lambda entry: entry.src_path == "src/499")
    continuation = None
    positions = []
    result = []
    while True:
        batch, continuation = selected.read_batch(continuation, limit=17)
        result.extend(batch)
        if continuation is None:
            break
        positions.append(continuation)
    assert len(positions) == len(set(positions))
    assert len(positions) > 20
    assert result == [_entry("499")]
    assert len(selected) == 1


@pytest.mark.parametrize("selection", ["alternating", "excluded_prefix", "none"])
def test_filtered_plan_reads_remain_bounded(selection: str) -> None:
    """Read sparse plans with batch queries rather than one query per entry."""
    size, limit = 4096, 64
    original = [_entry(str(i)) for i in range(size)]
    source = CopyEntries(original)

    def keep(entry: CopyEntry) -> bool:
        """Choose alternating entries, a short suffix, or no entries."""
        number = int(entry.src_path.removeprefix("src/"))
        return (
            number % 2 == 0
            if selection == "alternating"
            else (number >= size - 3 if selection == "excluded_prefix" else False)
        )

    queries: Counter[str] = Counter()
    read_batch = CopyIndex.read_batch

    def read(index: CopyIndex, after: int, limit: int) -> list[tuple[int, str, bytes]]:
        """Verify every read uses the caller's bound."""
        assert 0 < limit <= 64
        queries["reads"] += 1
        return read_batch(index, after, limit)

    with patch.object(
        EntryIndex, "_encode", side_effect=AssertionError("rewrote entries")
    ):
        selected = source.filtered(keep)
        with patch.object(CopyIndex, "read_batch", autospec=True, side_effect=read):
            assert _read_plan(selected, limit) == [
                entry for entry in original if keep(entry)
            ]
    assert queries["reads"] <= 3 * (size // limit) + 4
    assert len(selected) == sum(keep(entry) for entry in original)


def test_fully_confirmed_plan_does_not_scan_source_again() -> None:
    """Store one range and avoid rereading a fully confirmed plan."""
    source = CopyEntries(_entry(str(i)) for i in range(300))
    with patch.object(
        CopyIndex, "put", autospec=True, side_effect=CopyIndex.put
    ) as store:
        selected = source.filtered(lambda _item: False)
    assert store.call_count == 1
    queries: list[str] = []
    source._index._index._db.set_trace_callback(queries.append)
    assert not selected
    assert not _read_plan(selected)
    assert not queries


def test_failed_filter_leaves_original_plan_unchanged() -> None:
    """Preserve the full plan if confirmation checking fails partway through."""
    original = [_entry(str(i)) for i in range(300)]
    source = CopyEntries(original)

    def keep(entry: CopyEntry) -> bool:
        """Fail after excluding one batch of entries."""
        if entry.src_path == "src/150":
            raise StorageError("confirmation failed")
        return entry.src_path == "src/128"

    with pytest.raises(StorageError):
        source.filtered(keep)
    assert len(source) == 300
    assert _read_plan(source) == original


@pytest.mark.parametrize("size", [0, 1, 128, 129, 300])
def test_index_reads_materialized_bounded_batches(size: int) -> None:
    """Each explicit read issues one query and releases the lock before return."""
    index = CopyIndex()
    expected = [(str(i), str(i).encode()) for i in range(size)]
    for key, blob in expected:
        index.put(key, blob)
    queries: list[str] = []
    index._db.set_trace_callback(queries.append)
    actual = []
    position = 0
    while batch := index.read_batch(after=position, limit=128):
        assert len(batch) <= 128
        assert not index._lock.locked()
        for position, key, blob in batch:
            actual.append((key, blob))
    assert actual == expected
    assert len(queries) == (size + 127) // 128 + 1
    for limit in [0, -1]:
        with pytest.raises(ValueError):
            index.read_batch(after=0, limit=limit)


def test_index_updates_preserve_order_and_empty_blobs() -> None:
    """Keep insertion positions and distinguish absent keys from empty blobs."""
    index = CopyIndex()
    index.put("first//../\0", b"first")
    index.put("last", b"last")
    index.put("first//../\0", b"updated")
    assert index.put_if_absent("empty", b"") is None
    assert index.put_if_absent("empty", b"changed") == b""
    assert index.contains("empty")
    assert index.get("missing") is None
    assert not index.contains("missing")
    assert len(index) == 3
    assert [(key, blob) for _, key, blob in index.read_batch(0, 128)] == [
        ("first//../\0", b"updated"),
        ("last", b"last"),
        ("empty", b""),
    ]
    index.clear()
    assert not index
    assert not index.read_batch(0, 128)


def test_entry_updates_preserve_position_and_storage_references() -> None:
    """Decode replacement entries in insertion order with original storages."""
    index = EntryIndex()
    first, last = _entry("first"), _entry("last")
    index.put("first", first)
    index.put("last", last)
    updated = first._replace(size=42)
    index.put("first", updated)
    batch = index.read_batch(0, 128)
    assert [(key, entry) for _, key, entry in batch] == [
        ("first", updated),
        ("last", last),
    ]
    for _, _, entry in batch:
        assert entry.src_storage is STORAGE
        assert entry.dst_storage is STORAGE


def test_failed_journal_uploads_keep_bounded_retry_chunks() -> None:
    """An unavailable target must not grow an in-memory list or retry one giant gzip."""
    engine = MemoryEngine()
    context = _context(engine, flush_size=3)
    engine.upload_error = StorageError("unavailable")
    for i in range(40):
        context.record(_entry(str(i)))
        context.flush()
    assert len(context._buffer) == 40
    engine.upload_error = None
    context.flush(force=True)
    assert not context._buffer
    for data in engine.objects.values():
        assert len(gzip.decompress(data).splitlines()) <= 3
    resumed = _context(engine)
    resumed.load()
    assert all(resumed.is_recorded(_entry(str(i))) for i in range(40))


def test_corrupt_journal_does_not_confirm_its_readable_prefix() -> None:
    """Skip all records from a gzip whose trailer fails validation."""
    engine = MemoryEngine()
    context = _context(engine, flush_size=100000)
    for i in range(70000):
        context.record(_entry(str(i)))
    context.flush(force=True)
    path = next(iter(engine.objects))
    engine.objects[path] = engine.objects[path][:-8]
    resumed = _context(engine)
    resumed.load()
    assert not resumed.has_records()


def test_streamed_backup_json_preserves_legacy_links_without_mutating_source() -> None:
    """Publish the same metadata without copying the complete databases tree."""
    backup = _backup("b1", parts={"all_1_1_0": _part(link="older")})
    before = copy.deepcopy(backup._databases)
    expected = json.loads(backup.dump_json())
    with patch.object(
        backup, "_databases_with_legacy_part_links", side_effect=AssertionError
    ):
        actual = json.loads("".join(backup.iter_json()))
    assert actual == expected
    assert backup._databases == before


@pytest.mark.parametrize("tail", [b"abc", b"abc\0", b"ab"])
def test_streamed_tar_keeps_legacy_final_padding_rule(
    tail: bytes, tmp_path: Path
) -> None:
    """Accept missing final padding but reject partial padding and incomplete content."""
    header = tarfile.TarInfo("part/data")
    header.size = 3
    path = tmp_path / "metadata.tar"
    path.write_bytes(header.tobuf() + tail)
    archive = TemporaryMetadataArchive(str(path))
    if tail == b"abc":
        assert _read_archive(archive) == [("part/data", b"abc")]
    else:
        with pytest.raises(ValueError, match="Truncated TAR"):
            _read_archive(archive)


@pytest.mark.parametrize("suffix", [b"", b"\0" * 8, b"trailing"])
def test_decompression_stops_at_gzip_eof(suffix: bytes) -> None:
    """Terminate with the same output as the legacy decoder even with trailing bytes."""
    script = (
        "import gzip; from ch_backup.compression.gzip import GZIPCompression; "
        "data = b'x' * (2 * 1024 * 1024); "
        f"encoded = gzip.compress(data) + {suffix!r}; "
        "assert b''.join(GZIPCompression().decompress_chunks(encoded, 1024 * 1024)) "
        "== GZIPCompression().decompress(encoded) == data"
    )
    subprocess.run([sys.executable, "-c", script], check=True, timeout=10)


def test_copy_completion_callbacks_run_in_collecting_thread() -> None:
    """Worker completion must not run journal callbacks concurrently in pool threads."""
    owner = threading.get_ident()
    callbacks: list[int] = []
    with ThreadExecPool(2) as pool:
        for index in range(10):
            pool.submit(
                str(index),
                str,
                index,
                callback=lambda: callbacks.append(threading.get_ident()),
            )
        pool.wait_all()
    assert callbacks == [owner] * 10
