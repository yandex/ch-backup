"""Bound RAM and retry chunks while keeping pending confirmations on disk."""

import os
from gzip import GzipFile
from tempfile import TemporaryFile
from typing import BinaryIO

MAX_RECORD_BYTES = 1024 * 1024
MAX_CHUNK_BYTES = 16 * 1024 * 1024


class PendingJournalRecords:
    """Keep pending journal records in a temporary file.

    Only the collecting thread accesses this object. Records remain pending
    until their upload succeeds.
    """

    def __init__(self) -> None:
        """Create an empty records file with no prepared upload batch."""
        self._records_file = TemporaryFile()
        self._pending_start = 0
        self._pending_count = 0
        self._prepared_batch: tuple[int, int] | None = None

    def append(self, record: str) -> None:
        """Persist one pending confirmation without retaining it in RAM."""
        data = record.encode() + b"\n"
        if len(data) > MAX_RECORD_BYTES:
            raise ValueError("Copy journal record exceeds its size limit")
        self._records_file.seek(0, os.SEEK_END)
        self._records_file.write(data)
        self._pending_count += 1

    def __len__(self) -> int:
        """Return the number of unacknowledged records."""
        return self._pending_count

    def write_pending_batch(
        self, output: BinaryIO | GzipFile, max_records: int
    ) -> None:
        """Prepare a bounded batch without acknowledging its records."""
        self._prepared_batch = None
        self._records_file.seek(self._pending_start)
        count = size = 0
        while count < max(1, max_records) and size < MAX_CHUNK_BYTES:
            line = self._records_file.readline(MAX_RECORD_BYTES + 1)
            if not line:
                break
            if len(line) > MAX_RECORD_BYTES:
                raise ValueError("Copy journal record exceeds its size limit")
            output.write(line)
            count += 1
            size += len(line)
        if count:
            self._prepared_batch = count, self._records_file.tell()

    def acknowledge_uploaded(self) -> None:
        """Consume one prepared batch after its upload succeeds."""
        if self._prepared_batch is None:
            raise RuntimeError("No prepared journal batch to acknowledge")
        count, position = self._prepared_batch
        self._prepared_batch = None
        self._pending_count -= count
        self._pending_start = position
        if not self._pending_count:
            self._records_file.seek(0)
            self._records_file.truncate()
            self._pending_start = 0
