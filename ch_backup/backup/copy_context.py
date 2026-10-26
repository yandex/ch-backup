"""
Journal of objects confirmed copied into a target backup.
"""

import gzip
import hashlib
import json
import os
import time
import uuid
from tempfile import NamedTemporaryFile
from types import TracebackType

from ch_backup import logging
from ch_backup.backup.metadata import BackupMetadata, CopyScope
from ch_backup.storage.engine.base import PipeLineCompatibleStorageEngine
from ch_backup.storage.object_copy import CopyEntry, storage_id

COPY_CONTEXT_DIR = "copy_context"
FAILED_OBJECTS_DIR = "failed"
MANIFEST_DIR = "manifest"
SOURCE_DIR = "source"


class FailedObjects:
    """
    Objects that failed to copy, kept compressed in a temporary file instead of memory.
    """

    def __init__(self) -> None:
        # pylint: disable-next=consider-using-with
        self._file = NamedTemporaryFile()
        self._gzip = gzip.GzipFile(fileobj=self._file, mode="wb")
        self._count = 0

    def __enter__(self) -> "FailedObjects":
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self._gzip.close()
        self._file.close()

    def __len__(self) -> int:
        return self._count

    def append(self, entry: CopyEntry, error: str) -> None:
        """Save the object that failed to copy with the error."""
        line = {
            "src": entry.src_path,
            "dst": entry.dst_path,
            "size": entry.size,
            "error": error,
        }
        self._gzip.write((json.dumps(line) + "\n").encode())
        self._count += 1

    def path(self) -> str:
        """Finish compression and return the path of the file."""
        self._gzip.close()
        self._file.flush()
        return self._file.name


class CopyContext:
    """
    Journal of objects confirmed copied into one target backup.

    Records are kept in the target storage as gzip files of JSON lines under
    "<backup path>/copy_context/<fingerprint>/". Every file is written once under a
    unique name, so an interrupted run never damages records of earlier runs.
    The fingerprint identifies the source backup; a record identifies source and
    target locations and size of the object. The journal is only read from files directly
    under the fingerprint directory. Objects that failed to copy are listed in
    "failed/<scope>.jsonl.gz", objects of a scope in "manifest/<scope>.jsonl.gz", and
    "source/" holds source metadata of the backup laid out as in its directory.
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
        self._engine = engine
        self._path = os.path.join(
            backup_path, COPY_CONTEXT_DIR, _fingerprint(backup)
        ).lstrip("/")
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
            if os.path.dirname(path) != self._path:
                continue
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

    def is_archive_recorded(
        self, storage: dict, path: str, files: list[tuple[str, bytes]], size: int | None
    ) -> bool:
        """Check that upload of the rewritten metadata tarball of this size was confirmed by a previous run."""
        return _digest(_archive_record(storage, path, files, size)) in self._recorded

    def forget(self) -> None:
        """Drop records of previous runs once the plan is checked against them."""
        self._recorded.clear()

    def record(self, entry: CopyEntry) -> None:
        """Remember the confirmed copy until the next flush."""
        self._buffer.append(_record(entry).decode())

    def record_archive(
        self, storage: dict, path: str, files: list[tuple[str, bytes]], size: int
    ) -> None:
        """Remember the confirmed upload of the rewritten metadata tarball until the next flush."""
        self._buffer.append(_archive_record(storage, path, files, size).decode())

    def save_failures(self, scope: CopyScope, failures: FailedObjects) -> None:
        """Replace the list of objects of the scope that failed to copy, removing it if there are none."""
        path = os.path.join(self._path, FAILED_OBJECTS_DIR, f"{scope.value}.jsonl.gz")
        try:
            if len(failures):
                self._engine.upload_file(failures.path(), path)
            else:
                self._engine.delete_file(path)
        except Exception as e:
            logging.warning("Failed to save list of failed objects {}: {}", path, e)

    def save_manifest(self, scope: CopyScope, entries: list[CopyEntry]) -> None:
        """Save locations and sizes of all objects of the scope, without credentials."""
        lines = [
            json.dumps(
                [
                    list(storage_id(entry.src_storage)),
                    entry.src_path,
                    list(storage_id(entry.dst_storage)),
                    entry.dst_path,
                    entry.size,
                ]
            )
            for entry in entries
        ]
        self._engine.upload_data(
            gzip.compress(("\n".join(lines) + "\n").encode()),
            os.path.join(self._path, MANIFEST_DIR, f"{scope.value}.jsonl.gz"),
        )

    def load_manifest(self, scope: CopyScope) -> list[CopyEntry]:
        """Return objects of the scope saved by a previous run, with storages identified only."""
        data = self._engine.download_data(
            os.path.join(self._path, MANIFEST_DIR, f"{scope.value}.jsonl.gz")
        )
        entries = []
        for line in gzip.decompress(data).splitlines():
            if line:
                src, src_path, dst, dst_path, size = json.loads(line)
                entries.append(
                    CopyEntry(_storage(src), _storage(dst), src_path, dst_path, size)
                )
        return entries

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


def source_dir(backup: BackupMetadata) -> str:
    """Return the directory with source metadata of the backup, relative to the backup directory."""
    return os.path.join(COPY_CONTEXT_DIR, _fingerprint(backup), SOURCE_DIR)


def _fingerprint(backup: BackupMetadata) -> str:
    """Identify the source backup."""
    identity = json.dumps([backup.name, backup.start_time_str, backup.hostname])
    return hashlib.sha256(identity.encode()).hexdigest()[:16]


def _storage(identity: list) -> dict:
    """Describe the storage by its identifier, as returned by storage_id."""
    storage_type, endpoint_url, bucket = identity
    return {
        "type": storage_type,
        "credentials": {"endpoint_url": endpoint_url, "bucket": bucket},
    }


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


def _archive_record(
    storage: dict, path: str, files: list[tuple[str, bytes]], size: int | None
) -> bytes:
    """Serialize target location, size and content of the rewritten metadata tarball."""
    content = hashlib.blake2b(digest_size=16)
    for name, data in files:
        content.update(json.dumps([name, len(data)]).encode())
        content.update(data)
    return json.dumps([storage_id(storage), path, size, content.hexdigest()]).encode()


def _digest(record: bytes) -> bytes:
    """Compact the record to bound memory used by large journals."""
    return hashlib.blake2b(record, digest_size=16).digest()
