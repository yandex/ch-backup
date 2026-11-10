"""Check bounded copy structures and preservation of backup safety rules."""

# pylint: disable=protected-access

import copy
import gc
import gzip
import json
import subprocess
import sys
import tarfile
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
from ch_backup.storage.async_pipeline.pipeline_builder import PipelineBuilder
from ch_backup.storage.async_pipeline.pipelines import run_and_collect_all
from ch_backup.storage.metadata_archive import MetadataArchive, spool_metadata
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
    assert len(archive) == 2
    assert list(archive) == files
    assert _archive_record(STORAGE, "archive", archive, 123) == _archive_record(
        STORAGE, "archive", files, 123
    )
    path = Path(archive.path())
    assert path.stat().st_mode & 0o077 == 0
    del archive
    gc.collect()
    assert not path.exists()


def test_downloaded_metadata_archive_is_reiterable() -> None:
    """Read a downloaded tar without a cached member list."""
    original = spool_metadata([("part/data", b"payload")])
    downloaded = MetadataArchive(downloaded=True)
    Path(downloaded.path()).write_bytes(Path(original.path()).read_bytes())
    assert len(downloaded) == 1
    assert list(downloaded) == list(downloaded) == [("part/data", b"payload")]


def test_entry_index_rejects_cross_part_conflicts_and_keeps_secrets_in_memory() -> None:
    """A global disk index must detect conflicting sources and avoid persisting credentials."""
    storage: dict = copy.deepcopy(STORAGE)
    storage["credentials"]["secret_access_key"] = "DO_NOT_PERSIST"
    index: EntryIndex[str] = EntryIndex()
    first = CopyEntry(storage, storage, "source/a", "target/shared", 1, 1)
    _add_cloud_storage_copy_entry(index, first)
    _add_cloud_storage_copy_entry(index, first)
    assert len(index) == 1
    with pytest.raises(ClickhouseBackupError):
        _add_cloud_storage_copy_entry(index, first._replace(src_path="source/b"))
    with pytest.raises(ClickhouseBackupError):
        _add_cloud_storage_copy_entry(index, first._replace(size=2))
    other_storage = copy.deepcopy(storage)
    other_storage["credentials"]["bucket"] = "other"
    with pytest.raises(ClickhouseBackupError):
        _add_cloud_storage_copy_entry(index, first._replace(src_storage=other_storage))
    assert index[first.dst_path] == first
    index._db.commit()
    path = Path(index._db.execute("PRAGMA database_list").fetchone()[2])
    assert b"DO_NOT_PERSIST" not in path.read_bytes()
    assert index._db.execute("PRAGMA cache_size").fetchone()[0] == -65536
    assert index._db.execute("PRAGMA mmap_size").fetchone()[0] == 0
    assert index._db.execute("PRAGMA synchronous").fetchone()[0] == 0
    assert index._db.execute("PRAGMA journal_mode").fetchone()[0] == "delete"
    assert index._db.execute("PRAGMA cache_spill").fetchone()[0] > 0
    del index
    gc.collect()
    assert not path.exists()


def test_plan_and_listing_iterate_in_order_and_roll_back_incomplete_import() -> None:
    """Keep existing confirmations when a later journal chunk is corrupt."""
    entries = CopyEntries(_entry(str(i)) for i in range(300))
    queries: list[str] = []
    entries._index._db.set_trace_callback(queries.append)
    assert len(entries) == 300
    assert [entry.src_path for entry in entries] == [f"src/{i}" for i in range(300)]
    assert len(queries) == 4
    records: CopyIndex[bytes, bool] = CopyIndex()
    records[b"old"] = True
    with pytest.raises(ValueError):
        with records.importing():
            records[b"old"] = False
            for i in range(70000):
                records[str(i).encode()] = True
            raise ValueError("truncated gzip")
    assert records[b"old"] is True
    assert list(records) == [b"old"]
    assert len(records) == 1


def test_entry_insert_only_reads_duplicate_keys() -> None:
    """Avoid lookups for new destinations while preserving the first entry on a duplicate."""
    index: EntryIndex[str] = EntryIndex()
    queries: list[str] = []
    index._db.set_trace_callback(queries.append)
    first = _entry("first")
    assert index.get_or_insert("first", first) is first
    assert index.get_or_insert("last", _entry("last")) == _entry("last")
    assert not any(query.startswith("SELECT") for query in queries)
    assert index.get_or_insert("first", first._replace(size=42)) == first
    assert sum(query.startswith("SELECT") for query in queries) == 1
    assert len(index) == 2
    assert list(index.values()) == [first, _entry("last")]


@pytest.mark.parametrize("deduplicate", [False, True])
def test_index_batches_commits(deduplicate: bool) -> None:
    """Commit larger groups on both write paths while keeping the page cache bounded."""
    index: EntryIndex[int] = EntryIndex()
    statements: Counter[str] = Counter()

    def count_statement(statement: str) -> None:
        """Count operations without retaining their keys and values."""
        statements[statement.split()[0]] += 1

    index._db.set_trace_callback(count_statement)
    for number in range(65537):
        if deduplicate:
            index.get_or_insert(number, _entry(str(number)))
        else:
            index[number] = _entry(str(number))
    assert statements["COMMIT"] == 1
    assert len(index) == 65537
    assert index[65536] == _entry("65536")
    assert index._db.execute("PRAGMA cache_size").fetchone()[0] == -65536
    assert index._db.execute("PRAGMA mmap_size").fetchone()[0] == 0
    assert index._db.execute("PRAGMA synchronous").fetchone()[0] == 0
    assert index._db.execute("PRAGMA journal_mode").fetchone()[0] == "delete"
    assert index._db.execute("PRAGMA cache_spill").fetchone()[0] > 0


@pytest.mark.parametrize("keep_every", [0, 1, 2, 299])
@pytest.mark.parametrize("size", [0, 300])
def test_filtered_plan_preserves_source_and_order(keep_every: int, size: int) -> None:
    """Filter multiple segments without rewriting entries or losing the full manifest."""
    original = [_entry(str(i)) for i in range(size)]
    source = CopyEntries(original[:150])
    extra: EntryIndex[str] = EntryIndex()
    for entry in original[150:]:
        extra[entry.dst_path] = entry
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
    assert list(selected) == list(selected) == expected
    assert list(source) == original
    source.append(_entry("appended later"))
    assert list(selected) == expected
    del source
    gc.collect()
    for entry in selected:
        assert entry.src_storage is STORAGE
    selected.append(_entry("new"))
    assert list(selected) == expected + [_entry("new")]
    assert list(selected.filtered(lambda entry: entry.src_path == "src/new")) == [
        _entry("new")
    ]


def test_fully_confirmed_plan_does_not_scan_source_again() -> None:
    """Store one range and skip reading the plan when all objects are confirmed."""
    source = CopyEntries(_entry(str(i)) for i in range(300))
    with patch.object(
        CopyIndex, "__setitem__", autospec=True, side_effect=CopyIndex.__setitem__
    ) as store:
        selected = source.filtered(lambda _item: False)
    assert store.call_count == 1
    queries: list[str] = []
    source._index._db.set_trace_callback(queries.append)
    assert not selected
    assert not list(selected)
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
    assert list(source) == original


@pytest.mark.parametrize("view_name", ["values", "items"])
@pytest.mark.parametrize("size", [0, 1, 128, 129, 300])
def test_index_views_scan_in_batches(view_name: str, size: int) -> None:
    """Read each batch once without issuing a lookup for every record."""
    index: CopyIndex[tuple, int] = CopyIndex()
    expected = {(("s3", "bucket"), f"part/{i}"): i for i in range(size)}
    index.update(expected)
    queries: list[str] = []
    index._db.set_trace_callback(queries.append)
    view = getattr(index, view_name)()
    iterator = iter(view)
    assert not queries
    if size:
        assert next(iterator) == next(iter(getattr(expected, view_name)()))
        assert len(queries) == 1
    list(iterator)
    assert len(queries) == (size + 127) // 128 + 1
    assert list(view) == list(getattr(expected, view_name)())
    assert len(view) == size


@pytest.mark.parametrize("view_name", ["values", "items"])
def test_entry_index_views_remain_live(view_name: str) -> None:
    """Reuse views after updates while preserving storage identity and order."""
    index: EntryIndex[str] = EntryIndex()
    first = _entry("first")
    last = _entry("last")
    index["first"] = first
    view = getattr(index, view_name)()
    updated = first._replace(size=42)
    index["last"] = last
    index["first"] = updated
    expected = {"first": updated, "last": last}
    assert list(view) == list(getattr(expected, view_name)())
    assert len(view) == 2
    for entry in index.values():
        assert entry.src_storage is first.src_storage
        assert entry.dst_storage is first.dst_storage
    del index["first"]
    assert len(view) == 1
    assert next(iter(getattr({"last": last}, view_name)())) in view
    index.clear()
    assert not view
    assert not list(view)


def test_index_updates_serialize_once() -> None:
    """Serialize a replacement once and keep its position and row count."""
    index: CopyIndex[str, int] = CopyIndex()
    index.update({"first": 1, "last": 2})
    with patch.object(index, "_encode", wraps=index._encode) as encode:
        index["first"] = 3
    encode.assert_called_once_with(3)
    assert len(index) == 2
    assert list(index.items()) == [("first", 3), ("last", 2)]


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
def test_streamed_tar_keeps_legacy_final_padding_rule(tail: bytes) -> None:
    """Accept missing final padding but reject partial padding and incomplete content."""
    header = tarfile.TarInfo("part/data")
    header.size = 3
    archive = MetadataArchive(downloaded=True)
    Path(archive.path()).write_bytes(header.tobuf() + tail)
    if tail == b"abc":
        assert list(archive) == [("part/data", b"abc")]
    else:
        with pytest.raises(ValueError, match="Truncated TAR"):
            list(archive)


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
