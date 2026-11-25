"""Temporary string-to-blob indexes with bounded memory and rollback."""

import sqlite3
import threading
import weakref
from contextlib import contextmanager
from tempfile import NamedTemporaryFile
from typing import IO

_COMMIT_BATCH_SIZE = 65536


def _close_index(connection: sqlite3.Connection, file: IO[bytes]) -> None:
    """Close the database before deleting its private file."""
    connection.close()
    file.close()


class TemporaryDiskIndex:
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
            raise ValueError("Disk index batch limit must be positive")
        with self._lock:
            cursor = self._db.execute(
                "SELECT rowid, key, value FROM records WHERE rowid>? ORDER BY rowid LIMIT ?",
                (after, limit),
            )
            rows = cursor.fetchall()
            cursor.close()
        return rows

    def read_sorted_batch(
        self, after: str | None, limit: int
    ) -> list[tuple[str, bytes]]:
        """Read a bounded batch in key order without sorting records in Python."""
        if limit <= 0:
            raise ValueError("Disk index batch limit must be positive")
        with self._lock:
            if after is None:
                cursor = self._db.execute(
                    "SELECT key, value FROM records ORDER BY key LIMIT ?", (limit,)
                )
            else:
                cursor = self._db.execute(
                    "SELECT key, value FROM records WHERE key>? ORDER BY key LIMIT ?",
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
        """Roll back all imported records if the import fails."""
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
