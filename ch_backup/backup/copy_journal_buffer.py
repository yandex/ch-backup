"""Bound RAM and retry chunks while keeping pending confirmations on disk."""

import os
from gzip import GzipFile
from tempfile import TemporaryFile
from typing import BinaryIO

MAX_RECORD_BYTES = 1024 * 1024
MAX_CHUNK_BYTES = 16 * 1024 * 1024


class JournalBuffer:
    """Append confirmations to one private spool and acknowledge uploaded prefixes."""

    def __init__(self) -> None:
        """Create an empty spool with independent read and write positions."""
        self._file = TemporaryFile()
        self._position = 0
        self._count = 0

    def append(self, record: str) -> None:
        """Persist one pending confirmation without retaining it in RAM."""
        data = record.encode() + b"\n"
        if len(data) > MAX_RECORD_BYTES:
            raise ValueError("Copy journal record exceeds its size limit")
        self._file.seek(0, os.SEEK_END)
        self._file.write(data)
        self._count += 1

    def __len__(self) -> int:
        """Return the number of unacknowledged records."""
        return self._count

    def write_chunk(self, output: BinaryIO | GzipFile, records: int) -> tuple[int, int]:
        """Copy a bounded prefix into the compressed upload file."""
        self._file.seek(self._position)
        count = size = 0
        while count < max(1, records) and size < MAX_CHUNK_BYTES:
            line = self._file.readline(MAX_RECORD_BYTES + 1)
            if not line:
                break
            if len(line) > MAX_RECORD_BYTES:
                raise ValueError("Copy journal record exceeds its size limit")
            output.write(line)
            count += 1
            size += len(line)
        return count, self._file.tell()

    def discard(self, count: int, position: int) -> None:
        """Release only the prefix whose upload succeeded."""
        self._count -= count
        self._position = position
        if not self._count:
            self._file.seek(0)
            self._file.truncate()
            self._position = 0
