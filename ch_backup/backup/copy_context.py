"""
Journal of objects confirmed copied into a target backup.
"""

import gzip
import hashlib
import json
import os
import time
import uuid

from ch_backup import logging
from ch_backup.backup.metadata import BackupMetadata
from ch_backup.storage.engine.base import PipeLineCompatibleStorageEngine
from ch_backup.storage.object_copy import CopyEntry, storage_id

COPY_CONTEXT_DIR = "copy_context"


class CopyContext:
    """
    Journal of objects confirmed copied into one target backup.

    Records are kept in the target storage as gzip files of JSON lines under
    "<backup path>/copy_context/<fingerprint>/". Every file is written once under a
    unique name, so an interrupted run never damages records of earlier runs.
    The fingerprint identifies the source backup; a record identifies source and
    target locations and size of the object.
    """

    # pylint: disable=too-many-arguments,too-many-positional-arguments
    def __init__(
        self,
        engine: PipeLineCompatibleStorageEngine,
        backup_path: str,
        backup: BackupMetadata,
        flush_size: int = 10000,
        flush_interval: float = 60,
    ) -> None:
        identity = json.dumps([backup.name, backup.start_time_str, backup.hostname])
        fingerprint = hashlib.sha256(identity.encode()).hexdigest()[:16]
        self._engine = engine
        self._path = os.path.join(backup_path, COPY_CONTEXT_DIR, fingerprint).lstrip(
            "/"
        )
        self._run_id = f"{time.strftime('%Y%m%dT%H%M%S')}-{uuid.uuid4().hex[:8]}"
        self._index = 0
        self._flush_size = flush_size
        self._flush_interval = flush_interval
        self._flushed_at = time.monotonic()
        self._flushed_size = 0
        self._buffer: list[str] = []
        self._recorded: set[bytes] = set()

    def has_records(self) -> bool:
        """Check that records of previous runs were read."""
        return bool(self._recorded)

    def load(self) -> None:
        """Read records of previous runs, skipping files that cannot be read."""
        try:
            paths = sorted(self._engine.list_dir_with_sizes(self._path))
        except Exception as e:
            logging.warning("Failed to list copy context {}: {}", self._path, e)
            return
        for path in paths:
            try:
                lines = gzip.decompress(self._engine.download_data(path)).splitlines()
            except Exception as e:
                logging.warning("Skipping unreadable copy context file {}: {}", path, e)
                continue
            self._recorded.update(_digest(line) for line in lines if line)
        logging.info(
            "Read {} records of copy context {}", len(self._recorded), self._path
        )

    def is_recorded(self, entry: CopyEntry) -> bool:
        """Check that copying of the object was confirmed by a previous run."""
        return _digest(_record(entry)) in self._recorded

    def forget(self) -> None:
        """Drop records of previous runs once the plan is checked against them."""
        self._recorded.clear()

    def record(self, entry: CopyEntry) -> None:
        """Remember the confirmed copy until the next flush."""
        self._buffer.append(_record(entry).decode())

    def flush(self, force: bool = False) -> None:
        """
        Save buffered records once enough of them or enough time has accumulated.

        A failed write keeps the records for the next attempt, made after the same interval
        or number of new records: losing them only means copying these objects again.
        """
        if not self._buffer or (
            not force
            and len(self._buffer) - self._flushed_size < self._flush_size
            and time.monotonic() - self._flushed_at < self._flush_interval
        ):
            return
        self._index += 1
        path = os.path.join(self._path, f"{self._run_id}-{self._index:08d}.jsonl.gz")
        data = gzip.compress(("\n".join(self._buffer) + "\n").encode())
        self._flushed_at = time.monotonic()
        self._flushed_size = len(self._buffer)
        try:
            self._engine.upload_data(data, path)
        except Exception as e:
            logging.warning("Failed to save copy context {}: {}", path, e)
            return
        self._buffer.clear()
        self._flushed_size = 0


def _record(entry: CopyEntry) -> bytes:
    """Serialize source and target locations and size of the object."""
    return json.dumps(
        [
            storage_id(entry.src_storage),
            entry.src_path,
            storage_id(entry.dst_storage),
            entry.dst_path,
            entry.size,
        ]
    ).encode()


def _digest(record: bytes) -> bytes:
    """Compact the record to bound memory used by large journals."""
    return hashlib.blake2b(record, digest_size=16).digest()
