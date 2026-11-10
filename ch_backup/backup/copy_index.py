"""Private disk indexes for copy plans, listings and journal confirmations."""

import json
import sqlite3
import threading
import weakref
from collections.abc import (
    Callable,
    ItemsView,
    Iterable,
    Iterator,
    MutableMapping,
    ValuesView,
)
from contextlib import contextmanager
from itertools import islice
from tempfile import NamedTemporaryFile
from typing import IO, Any, Generic, TypeVar

from ch_backup.storage.object_copy import CopyEntry

K = TypeVar("K")
V = TypeVar("V")

_COMMIT_BATCH_SIZE = 65536


def _close_index(connection: sqlite3.Connection, file: IO[bytes]) -> None:
    """Close the database before deleting its private file."""
    connection.close()
    file.close()


def _pack_key(key: Any) -> bytes:
    """Encode digest bytes and structured object keys without executable formats."""
    return b"b" + key if isinstance(key, bytes) else b"j" + json.dumps(key).encode()


def _unpack_key(data: bytes) -> Any:
    """Recreate hashable keys written to the private index."""
    return data[1:] if data[:1] == b"b" else _tuple_key(json.loads(data[1:]))


def _tuple_key(value):
    """Restore nested tuple identities from JSON arrays."""
    return (
        tuple(_tuple_key(item) for item in value) if isinstance(value, list) else value
    )


class CopyIndex(MutableMapping[K, V], Generic[K, V]):
    """Keep arbitrary internal records on disk with a bounded page cache."""

    def __init__(self) -> None:
        """Create a disposable index; rollback is required, power-loss durability is not."""
        # pylint: disable-next=consider-using-with
        file = NamedTemporaryFile()
        self._db = sqlite3.connect(file.name, check_same_thread=False)
        self._lock = threading.RLock()
        self._count = 0
        self._writes = 0
        self._importing = False
        self._db.execute("PRAGMA synchronous=OFF")
        self._db.execute("PRAGMA cache_size=-65536")
        self._db.execute("PRAGMA temp_store=FILE")
        self._db.execute("PRAGMA mmap_size=0")
        self._db.execute(
            "CREATE TABLE records (key BLOB PRIMARY KEY, value BLOB NOT NULL)"
        )
        self._cleanup = weakref.finalize(self, _close_index, self._db, file)

    def _encode(self, value: V) -> bytes:
        """Serialize an internal value into the private scratch database."""
        return json.dumps(value).encode()

    def _decode(self, value: bytes) -> V:
        """Read only records written by this process to its own index."""
        return json.loads(value)

    def __getitem__(self, key: K) -> V:
        """Look up a record without caching all keys."""
        with self._lock:
            row = self._db.execute(
                "SELECT value FROM records WHERE key=?",
                (_pack_key(key),),
            ).fetchone()
        if row is None:
            raise KeyError(key)
        return self._decode(row[0])

    def __setitem__(self, key: K, value: V) -> None:
        """Store a record and commit bounded groups of writes."""
        encoded = _pack_key(key)
        with self._lock:
            data = self._encode(value)
            cursor = self._db.execute(
                "INSERT OR IGNORE INTO records VALUES (?, ?)",
                (encoded, data),
            )
            self._count += cursor.rowcount
            if not cursor.rowcount:
                self._db.execute(
                    "UPDATE records SET value=? WHERE key=?",
                    (data, encoded),
                )
            self._written()

    def _written(self) -> None:
        """Commit groups of writes without accumulating records in Python."""
        self._writes += 1
        if self._writes >= _COMMIT_BATCH_SIZE and not self._importing:
            self._db.commit()
            self._writes = 0

    def __delitem__(self, key: K) -> None:
        """Remove one record."""
        with self._lock:
            cursor = self._db.execute(
                "DELETE FROM records WHERE key=?", (_pack_key(key),)
            )
            if not cursor.rowcount:
                raise KeyError(key)
            self._count -= 1

    def __iter__(self) -> Iterator[K]:
        """Read keys in insertion order."""
        for key, _ in self._rows():
            yield _unpack_key(key)

    def values(self) -> ValuesView[V]:
        """Read values in batches without looking up each key again."""
        return _CopyValuesView(self)

    def items(self) -> ItemsView[K, V]:
        """Read key-value pairs directly from each batch."""
        return _CopyItemsView(self)

    def _rows(self) -> Iterator[tuple[bytes, bytes]]:
        """Release the connection lock between bounded result batches."""
        position = 0
        while True:
            with self._lock:
                cursor = self._db.execute(
                    "SELECT rowid, key, value FROM records WHERE rowid>? ORDER BY rowid LIMIT 128",
                    (position,),
                )
                rows = cursor.fetchall()
                cursor.close()
            if not rows:
                return
            for position, key, value in rows:
                yield key, value

    def __len__(self) -> int:
        """Return the maintained row count without querying from sampler threads."""
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
        self._db.commit()
        count = self._count
        self._importing = True
        try:
            yield
            self._db.commit()
        except BaseException:
            self._db.rollback()
            self._count = count
            raise
        finally:
            self._importing = False
            self._writes = 0


class _CopyValuesView(ValuesView[V]):
    """Keep the mapping view behavior while scanning stored values directly."""

    _mapping: CopyIndex[Any, V]

    def __iter__(self) -> Iterator[V]:
        """Decode one value at a time from bounded batches."""
        # pylint: disable=protected-access
        for _, value in self._mapping._rows():
            yield self._mapping._decode(value)


class _CopyItemsView(ItemsView[K, V]):
    """Keep the mapping view behavior while scanning stored records directly."""

    _mapping: CopyIndex[K, V]

    def __iter__(self) -> Iterator[tuple[K, V]]:
        """Decode key-value pairs in insertion order."""
        # pylint: disable=protected-access
        for key, value in self._mapping._rows():
            yield _unpack_key(key), self._mapping._decode(value)


class EntryIndex(CopyIndex[K, CopyEntry]):
    """Store paths and sizes while keeping storage credentials only in memory."""

    def __init__(self) -> None:
        """Share each storage configuration across all of its entries."""
        super().__init__()
        self._storages: list[dict] = []
        self._storage_ids: dict[int, int] = {}

    def _storage(self, storage: dict) -> int:
        """Assign a process-local identifier without persisting secrets."""
        key = id(storage)
        if key not in self._storage_ids:
            self._storage_ids[key] = len(self._storages)
            self._storages.append(storage)
        return self._storage_ids[key]

    def _encode(self, value: CopyEntry) -> bytes:
        """Keep only identifiers, object keys and size on disk."""
        return json.dumps(
            (
                self._storage(value.src_storage),
                self._storage(value.dst_storage),
                *value[2:],
            ),
        ).encode()

    def _decode(self, value: bytes) -> CopyEntry:
        """Attach the original in-memory configurations to one entry."""
        source, target, *fields = json.loads(value)
        return CopyEntry(self._storages[source], self._storages[target], *fields)

    def get_or_insert(self, key: K, entry: CopyEntry) -> CopyEntry:
        """Read the existing entry only when its key is already present."""
        with self._lock:
            cursor = self._db.execute(
                "INSERT OR IGNORE INTO records VALUES (?, ?)",
                (_pack_key(key), self._encode(entry)),
            )
            if cursor.rowcount:
                self._count += 1
                self._written()
                return entry
            return self[key]


class CopyEntries:
    """Keep an ordered plan on disk and expose only one batch at a time."""

    def __init__(self, entries: Iterable[CopyEntry] = ()) -> None:
        """Append initial entries without collecting the iterable."""
        self._index: EntryIndex[int] = EntryIndex()
        self._segments: list[EntryIndex | _FilteredEntries] = [self._index]
        self.extend(entries)

    def append(self, entry: CopyEntry) -> None:
        """Append one plan entry."""
        if self._segments[-1] is not self._index:
            self._index = EntryIndex()
            self._segments.append(self._index)
        self._index[len(self._index)] = entry

    def extend(self, entries: Iterable[CopyEntry]) -> None:
        """Append entries in order."""
        for entry in entries:
            self.append(entry)

    def extend_index(self, entries: "EntryIndex | _FilteredEntries") -> None:
        """Reuse stored entries without serializing them again."""
        self._segments.append(entries)

    def filtered(self, keep: Callable[[CopyEntry], bool]) -> "CopyEntries":
        """Index excluded ranges without rewriting or mutating the source plan."""
        excluded: CopyIndex[int, int] = CopyIndex()
        start = None
        removed = 0
        for position, entry in enumerate(self):
            if not keep(entry):
                removed += 1
                if start is None:
                    start = position
            elif start is not None:
                excluded[start] = position
                start = None
        if start is not None:
            excluded[start] = len(self)
        result = CopyEntries()
        result.extend_index(_FilteredEntries(self, excluded, removed))
        return result

    def __len__(self) -> int:
        """Return the number of planned objects."""
        return sum(len(segment) for segment in self._segments)

    def __iter__(self) -> Iterator[CopyEntry]:
        """Read bounded batches from the plan."""
        for segment in self._segments:
            yield from segment.values()


class _FilteredEntries:
    """Read the original plan alongside sorted excluded ranges."""

    def __init__(
        self, source: CopyEntries, excluded: CopyIndex[int, int], removed: int
    ) -> None:
        """Keep the source alive and freeze the number of entries being filtered."""
        self._source = source
        self._excluded = excluded
        self._size = len(source)
        self._count = self._size - removed

    def __len__(self) -> int:
        """Return the remaining count without scanning either index."""
        return self._count

    def values(self) -> Iterator[CopyEntry]:
        """Merge two bounded streams without querying the index for each object."""
        if not self:
            return
        source = islice(self._source, self._size)
        if not self._excluded:
            yield from source
            return
        ranges = iter(self._excluded.items())
        start, end = next(ranges, (self._size, self._size))
        for position, entry in enumerate(source):
            if position >= end:
                start, end = next(ranges, (self._size, self._size))
            if position < start:
                yield entry
