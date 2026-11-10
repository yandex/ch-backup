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
from typing import Iterable

from ch_backup import logging
from ch_backup.backup.copy_index import CopyEntries, CopyIndex
from ch_backup.backup.copy_journal_buffer import MAX_RECORD_BYTES, JournalBuffer
from ch_backup.backup.metadata import BackupMetadata, CopyScope
from ch_backup.storage.engine.base import PipeLineCompatibleStorageEngine
from ch_backup.storage.metadata_archive import MetadataArchive, MetadataFiles
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
        """Create an empty compressed temporary file."""
        # pylint: disable-next=consider-using-with
        self._file = NamedTemporaryFile()
        self._gzip = gzip.GzipFile(fileobj=self._file, mode="wb")
        self._count = 0

    def __enter__(self) -> "FailedObjects":
        """Return the list itself."""
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Remove the temporary file."""
        self._gzip.close()
        self._file.close()

    def __len__(self) -> int:
        """Return the number of objects that failed to copy."""
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


# pylint: disable=too-many-instance-attributes
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
        data_encryption: dict | None = None,
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
        self._buffer = JournalBuffer()
        self._recorded: CopyIndex[bytes, bool] = CopyIndex()
        self._data_encryption = data_encryption

    def has_records(self) -> bool:
        """Check that records of previous runs were read."""
        return bool(self._recorded)

    def load(self) -> None:
        """Read records of previous runs, skipping files that cannot be read."""
        paths: CopyIndex[str, int] = CopyIndex()
        try:
            for path, size in self._engine.iter_dir_with_sizes(self._path):
                if os.path.dirname(path) == self._path:
                    paths[path] = size
        except Exception as e:
            logging.warning("Failed to list copy context {}: {}", self._path, e)
            return
        for path in paths:
            if os.path.dirname(path) != self._path:
                continue
            try:
                with NamedTemporaryFile() as source:
                    self._engine.download_file(path, source.name)
                    with (
                        gzip.open(source.name, "rb") as stream,
                        self._recorded.importing(),
                    ):
                        for line in _journal_lines(stream):
                            self._recorded[_digest(line)] = True
            except Exception as e:
                logging.warning("Skipping unreadable copy context file {}: {}", path, e)
        logging.info(
            "Read {} records of copy context {}", len(self._recorded), self._path
        )

    def is_recorded(self, entry: CopyEntry) -> bool:
        """Check that copying of the object was confirmed by a previous run."""
        return _digest(_record(entry, self._data_encryption)) in self._recorded

    def is_archive_recorded(
        self, storage: dict, path: str, files: MetadataFiles, size: int | None
    ) -> bool:
        """Check that upload of the rewritten metadata tarball of this size was confirmed by a previous run."""
        return _digest(_archive_record(storage, path, files, size)) in self._recorded

    def forget(self) -> None:
        """Drop records of previous runs once the plan is checked against them."""
        self._recorded.clear()

    def record(self, entry: CopyEntry) -> None:
        """Remember the confirmed copy until the next flush."""
        self._buffer.append(_record(entry, self._data_encryption).decode())

    def record_archive(
        self, storage: dict, path: str, files: MetadataFiles, size: int
    ) -> None:
        """Remember the confirmed upload of the rewritten metadata tarball until the next flush."""
        self._buffer.append(_archive_record(storage, path, files, size).decode())

    def save_failures(self, scope: CopyScope, failures: FailedObjects) -> None:
        """Replace the list of objects of the scope that failed to copy, removing it if there are none."""
        path = os.path.join(self._path, FAILED_OBJECTS_DIR, f"{scope.value}.jsonl.gz")
        try:
            if failures:
                self._engine.upload_file(failures.path(), path)
            else:
                self._engine.delete_file(path)
        except Exception as e:
            logging.warning("Failed to save list of failed objects {}: {}", path, e)

    def save_manifest(self, scope: CopyScope, entries: Iterable[CopyEntry]) -> None:
        """Stream the object manifest without credentials into a compressed file."""
        with NamedTemporaryFile() as target:
            with gzip.open(target.name, "wb") as stream:
                for entry in entries:
                    stream.write(_record(entry) + b"\n")
            self._engine.upload_file(
                target.name,
                os.path.join(self._path, MANIFEST_DIR, f"{scope.value}.jsonl.gz"),
            )

    def load_manifest(self, scope: CopyScope) -> CopyEntries:
        """Import the saved manifest without keeping all locations in memory."""
        entries = CopyEntries()
        storages: dict[tuple, dict] = {}
        with NamedTemporaryFile() as source:
            self._engine.download_file(
                os.path.join(self._path, MANIFEST_DIR, f"{scope.value}.jsonl.gz"),
                source.name,
            )
            with gzip.open(source.name, "rb") as stream:
                for line in _journal_lines(stream):
                    src, src_path, dst, dst_path, size = json.loads(line)
                    src, dst = (
                        storages.setdefault(tuple(i), _storage(i)) for i in (src, dst)
                    )
                    entries.append(CopyEntry(src, dst, src_path, dst_path, size, size))
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
        while self._buffer:
            self._index += 1
            path = os.path.join(
                self._path, f"{self._run_id}-{self._index:08d}.jsonl.gz"
            )
            self._flushed_at = time.monotonic()
            self._flushed_size = len(self._buffer)
            with NamedTemporaryFile() as target:
                with gzip.open(target.name, "wb") as stream:
                    count, position = self._buffer.write_chunk(stream, self._flush_size)
                try:
                    self._engine.upload_file(target.name, path)
                except Exception as e:
                    logging.warning("Failed to save copy context {}: {}", path, e)
                    return
            self._buffer.discard(count, position)
            self._flushed_size = 0
            if not force:
                return


def _journal_lines(stream):
    """Read bounded journal records and reject oversized or damaged input."""
    while line := stream.readline(MAX_RECORD_BYTES + 1):
        if len(line) > MAX_RECORD_BYTES:
            raise ValueError("Copy journal record exceeds its size limit")
        line = line.rstrip(b"\r\n")
        if line:
            yield line


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


def _record(entry: CopyEntry, data_encryption: dict | None = None) -> bytes:
    """Serialize source and target locations and size of the object."""
    record = [
        storage_id(entry.src_storage),
        entry.src_path,
        storage_id(entry.dst_storage),
        entry.dst_path,
        entry.dst_size,
    ]
    if entry.encrypt and data_encryption is not None:
        record.append(data_encryption)
    return json.dumps(record, sort_keys=True).encode()


def _archive_record(
    storage: dict, path: str, files: MetadataFiles, size: int | None
) -> bytes:
    """Serialize target location, size and content of the rewritten metadata tarball."""
    if isinstance(files, MetadataArchive):
        return json.dumps(
            [storage_id(storage), path, size, files.content_digest()]
        ).encode()
    content = hashlib.blake2b(digest_size=16)
    for name, data in files:
        content.update(json.dumps([name, len(data)]).encode())
        content.update(data)
    return json.dumps([storage_id(storage), path, size, content.hexdigest()]).encode()


def _digest(record: bytes) -> bytes:
    """Compact the record to bound memory used by large journals."""
    return hashlib.blake2b(record, digest_size=16).digest()
