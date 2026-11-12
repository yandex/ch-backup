"""Private disk indexes for copy plans, listings and journal confirmations."""

import json
import sqlite3
import threading
import weakref
from collections.abc import Callable, Iterable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from tempfile import NamedTemporaryFile
from typing import IO

from ch_backup.storage.object_copy import CopyEntry

_COMMIT_BATCH_SIZE = 65536


def _close_index(connection: sqlite3.Connection, file: IO[bytes]) -> None:
    """Close the database before deleting its private file."""
    connection.close()
    file.close()


class CopyIndex:
    """Store string addresses and blobs on disk with a bounded page cache."""

    def __init__(self) -> None:
        """Create a disposable index with rollback but no power-loss durability."""
        # pylint: disable-next=consider-using-with
        file = NamedTemporaryFile()
        self._db = sqlite3.connect(file.name, check_same_thread=False)
        self._lock = threading.Lock()
        self._count = 0
        self._writes = 0
        self._importing = False
        self._db.execute("PRAGMA synchronous=OFF")
        self._db.execute("PRAGMA cache_size=-65536")
        self._db.execute("PRAGMA temp_store=FILE")
        self._db.execute("PRAGMA mmap_size=0")
        self._db.execute(
            "CREATE TABLE records (key TEXT PRIMARY KEY, value BLOB NOT NULL)"
        )
        self._cleanup = weakref.finalize(self, _close_index, self._db, file)

    def get(self, key: str) -> bytes | None:
        """Read a blob, returning None when its address is absent."""
        with self._lock:
            row = self._db.execute(
                "SELECT value FROM records WHERE key=?", (key,)
            ).fetchone()
        return None if row is None else row[0]

    def contains(self, key: str) -> bool:
        """Check whether an address exists."""
        return self.get(key) is not None

    def put(self, key: str, value: bytes) -> None:
        """Store a blob without changing its insertion position on updates."""
        with self._lock:
            cursor = self._db.execute(
                "INSERT OR IGNORE INTO records VALUES (?, ?)", (key, value)
            )
            self._count += cursor.rowcount
            if not cursor.rowcount:
                self._db.execute("UPDATE records SET value=? WHERE key=?", (value, key))
            self._written()

    def put_if_absent(self, key: str, value: bytes) -> bytes | None:
        """Insert a blob or read the existing blob under the same lock."""
        with self._lock:
            cursor = self._db.execute(
                "INSERT OR IGNORE INTO records VALUES (?, ?)", (key, value)
            )
            if cursor.rowcount:
                self._count += 1
                self._written()
                return None
            return self._db.execute(
                "SELECT value FROM records WHERE key=?", (key,)
            ).fetchone()[0]

    def _written(self) -> None:
        """Commit groups of writes without accumulating records in Python."""
        self._writes += 1
        if self._writes >= _COMMIT_BATCH_SIZE and not self._importing:
            self._db.commit()
            self._writes = 0

    def read_batch(self, after: int, limit: int) -> list[tuple[int, str, bytes]]:
        """Materialize one bounded batch in insertion order, then release the lock."""
        if limit <= 0:
            raise ValueError("Copy index batch limit must be positive")
        with self._lock:
            cursor = self._db.execute(
                "SELECT rowid, key, value FROM records WHERE rowid>? ORDER BY rowid LIMIT ?",
                (after, limit),
            )
            rows = cursor.fetchall()
            cursor.close()
        return rows

    def __len__(self) -> int:
        """Return the maintained row count."""
        with self._lock:
            return self._count

    def clear(self) -> None:
        """Forget all records after verification."""
        with self._lock:
            self._db.execute("DELETE FROM records")
            self._db.commit()
            self._count = self._writes = 0

    @contextmanager
    def importing(self):
        """Discard a whole damaged journal chunk, including its valid prefix."""
        with self._lock:
            self._db.commit()
            count = self._count
            self._importing = True
        try:
            yield
            with self._lock:
                self._db.commit()
        except BaseException:
            with self._lock:
                self._db.rollback()
                self._count = count
            raise
        finally:
            with self._lock:
                self._importing = False
                self._writes = 0


class EntryIndex:
    """Store copy entries while keeping storage configurations only in memory."""

    def __init__(self) -> None:
        """Share each storage configuration across all of its entries."""
        self._index = CopyIndex()
        self._lock = threading.Lock()
        self._storages: list[dict] = []
        self._storage_ids: dict[int, int] = {}

    def _storage(self, storage: dict) -> int:
        """Assign a process-local identifier without persisting secrets."""
        key = id(storage)
        if key not in self._storage_ids:
            self._storage_ids[key] = len(self._storages)
            self._storages.append(storage)
        return self._storage_ids[key]

    def _encode(self, entry: CopyEntry) -> bytes:
        """Keep only storage identifiers, paths and entry fields on disk."""
        return json.dumps(
            (
                self._storage(entry.src_storage),
                self._storage(entry.dst_storage),
                *entry[2:],
            )
        ).encode()

    def _decode(self, blob: bytes) -> CopyEntry:
        """Attach the original in-memory configurations to one entry."""
        source, target, *fields = json.loads(blob)
        return CopyEntry(self._storages[source], self._storages[target], *fields)

    def put(self, key: str, entry: CopyEntry) -> None:
        """Store an entry without changing its insertion position on updates."""
        with self._lock:
            self._index.put(key, self._encode(entry))

    def get_or_insert(self, key: str, entry: CopyEntry) -> CopyEntry:
        """Decode only duplicates and return new entries without another lookup."""
        with self._lock:
            existing = self._index.put_if_absent(key, self._encode(entry))
            return entry if existing is None else self._decode(existing)

    def read_batch(self, after: int, limit: int) -> list[tuple[int, str, CopyEntry]]:
        """Read and decode one bounded batch of entries."""
        return [
            (position, key, self._decode(blob))
            for position, key, blob in self._index.read_batch(after, limit)
        ]

    def __len__(self) -> int:
        """Return the number of stored entries."""
        return len(self._index)


@dataclass(frozen=True)
class CopyEntriesCursor:
    """Locate the next read within an ordered plan's stored segments."""

    segment: int = 0
    after: "int | _FilteredCursor" = 0


@dataclass(frozen=True)
class _FilteredCursor:
    """Resume source and exclusion scans after every examined entry."""

    source: CopyEntriesCursor | None = None
    position: int = 0
    range_after: int = 0
    start: int = 0
    end: int = 0


class CopyEntries:
    """Keep an ordered plan on disk and expose only one batch at a time."""

    def __init__(self, entries: Iterable[CopyEntry] = ()) -> None:
        """Append initial entries without collecting the iterable."""
        self._index = EntryIndex()
        self._segments: list[EntryIndex | _FilteredEntries] = [self._index]
        self.extend(entries)

    def append(self, entry: CopyEntry) -> None:
        """Append one plan entry."""
        if self._segments[-1] is not self._index:
            self._index = EntryIndex()
            self._segments.append(self._index)
        self._index.put(str(len(self._index)), entry)

    def extend(self, entries: Iterable[CopyEntry]) -> None:
        """Append in-memory entries in order."""
        for entry in entries:
            self.append(entry)

    def extend_index(self, entries: "EntryIndex | _FilteredEntries") -> None:
        """Reuse stored entries without serializing them again."""
        self._segments.append(entries)

    def filtered(self, keep: Callable[[CopyEntry], bool]) -> "CopyEntries":
        """Index excluded ranges without rewriting or mutating the source plan."""
        excluded = CopyIndex()
        start = None
        removed = position = 0
        continuation = None
        while True:
            batch, continuation = self.read_batch(continuation, limit=128)
            for entry in batch:
                if not keep(entry):
                    removed += 1
                    if start is None:
                        start = position
                elif start is not None:
                    excluded.put(str(start), str(position).encode("ascii"))
                    start = None
                position += 1
            if continuation is None:
                break
        if start is not None:
            excluded.put(str(start), str(position).encode("ascii"))
        result = CopyEntries()
        result.extend_index(_FilteredEntries(self, excluded, removed))
        return result

    def __len__(self) -> int:
        """Return the number of planned objects."""
        return sum(len(segment) for segment in self._segments)

    def read_batch(
        self, continuation: CopyEntriesCursor | None = None, limit: int = 128
    ) -> tuple[list[CopyEntry], CopyEntriesCursor | None]:
        """Read a bounded segment batch; only a None cursor marks completion."""
        if limit <= 0:
            raise ValueError("Copy plan batch limit must be positive")
        cursor = continuation or CopyEntriesCursor()
        if cursor.segment >= len(self._segments):
            return [], None
        segment = self._segments[cursor.segment]
        if isinstance(segment, EntryIndex):
            assert isinstance(cursor.after, int)
            rows = segment.read_batch(cursor.after, limit)
            batch = [entry for _, _, entry in rows]
            after: int | _FilteredCursor | None = (
                rows[-1][0] if len(rows) == limit else None
            )
        else:
            batch, after = segment.read_batch(
                cursor.after if isinstance(cursor.after, _FilteredCursor) else None,
                limit,
            )
        if after is not None:
            return batch, CopyEntriesCursor(cursor.segment, after)
        following = cursor.segment + 1
        return batch, (
            CopyEntriesCursor(following) if following < len(self._segments) else None
        )


class _FilteredEntries:
    """Read the original plan alongside sorted excluded ranges."""

    def __init__(self, source: CopyEntries, excluded: CopyIndex, removed: int) -> None:
        """Keep the source alive and freeze the number of filtered entries."""
        self._source = source
        self._excluded = excluded
        self._size = len(source)
        self._count = self._size - removed

    def __len__(self) -> int:
        """Return the remaining count without scanning either index."""
        return self._count

    def read_batch(
        self, continuation: _FilteredCursor | None, limit: int
    ) -> tuple[list[CopyEntry], _FilteredCursor | None]:
        """Advance through bounded source records, including excluded entries."""
        cursor = continuation or _FilteredCursor()
        if not self._count or cursor.position >= self._size:
            return [], None
        entries, source = self._source.read_batch(
            cursor.source, min(limit, self._size - cursor.position)
        )
        position, range_after, start, end = (
            cursor.position,
            cursor.range_after,
            cursor.start,
            cursor.end,
        )
        ranges: Iterator[tuple[int, str, bytes]] = iter(())
        batch = []
        for entry in entries:
            while position >= end:
                row = next(ranges, None)
                if row is None:
                    ranges = iter(self._excluded.read_batch(range_after, limit))
                    row = next(ranges, None)
                if row is None:
                    start, end = self._size, self._size + 1
                else:
                    range_after, first, last = row
                    start, end = int(first), int(last)
            if position < start:
                batch.append(entry)
            position += 1
        return batch, (
            _FilteredCursor(source, position, range_after, start, end)
            if source is not None and position < self._size
            else None
        )
