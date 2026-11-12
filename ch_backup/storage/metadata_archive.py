"""Keep disk metadata in private local tar files rather than Python lists."""

import hashlib
import json
import os
import tarfile
import weakref
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import IO, Iterable

from ch_backup.storage.async_pipeline.stages.filesystem.read_files_tarball_stage import (
    _make_tar_header,
)

METADATA_ARCHIVE_BATCH_BYTES = 1024 * 1024


def _remove_archive(file: IO[bytes], path: str) -> None:
    """Close an unfinished writer and remove its private archive."""
    try:
        file.close()
    finally:
        Path(path).unlink(missing_ok=True)


class MetadataArchiveWriter:
    """Write metadata incrementally and transfer ownership on completion."""

    def __init__(self) -> None:
        """Create a private file and incremental content digest."""
        # pylint: disable-next=consider-using-with
        self._file = NamedTemporaryFile(delete=False)
        self._cleanup = weakref.finalize(
            self, _remove_archive, self._file, self._file.name
        )
        self._count = 0
        self._digest = hashlib.blake2b(digest_size=16)

    def append(self, item: tuple[str, bytes]) -> None:
        """Append one metadata file, removing the archive if writing fails."""
        if self._file.closed:
            raise ValueError("Metadata archive writer is closed")
        try:
            name, data = item
            self._file.write(_make_tar_header(name, len(data)))
            self._file.write(data)
            self._file.write(b"\0" * (-len(data) % 512))
            self._digest.update(json.dumps([name, len(data)]).encode())
            self._digest.update(data)
            self._count += 1
        except BaseException:
            self._cleanup()
            raise

    def finish(self) -> "TemporaryMetadataArchive":
        """Close the writer and transfer its file to a completed archive."""
        if self._file.closed:
            raise ValueError("Metadata archive writer is closed")
        try:
            self._file.close()
            archive = TemporaryMetadataArchive(
                self._file.name, self._count, self._digest.hexdigest()
            )
        except BaseException:
            self._cleanup()
            raise
        self._cleanup.detach()
        return archive


class TemporaryMetadataArchive:
    """Own a completed temporary uncompressed tar archive.

    Keep this object alive while an asynchronous operation uses its file.
    Object cleanup removes the file.
    """

    def __init__(
        self, path: str, count: int | None = None, digest: str | None = None
    ) -> None:
        """Take ownership of a completed archive and optional writing results."""
        self._path = path
        self._cleanup = weakref.finalize(self, Path(path).unlink, missing_ok=True)
        self._count = count
        self._digest = digest

    def path(self) -> str:
        """Return the filename without performing I/O."""
        return self._path

    def read_batch(
        self, continuation: int | None = None
    ) -> tuple[list[tuple[str, bytes]], int | None]:
        """Read a bounded batch; None starts reading and marks completion.

        Continuations are byte offsets returned by an earlier call. Memory is
        bounded by 1 MiB of tar data plus one file crossing the batch boundary.
        Legacy archives may omit all padding after the final file.
        """
        offset = 0 if continuation is None else continuation
        with Path(self._path).open("rb") as stream:
            size = os.fstat(stream.fileno()).st_size
            if offset < 0 or offset > size or offset % 512:
                raise ValueError("Invalid metadata archive continuation")
            if offset == size:
                return [], None
            position = offset
            files = []
            try:
                stream.seek(offset)
                with tarfile.open(fileobj=stream, mode="r:") as archive:
                    member = archive.next()
                    while position < size:
                        if member is None:
                            raise ValueError("Truncated TAR stream")
                        data = _read_member(stream, member)
                        if data or member.isfile():
                            files.append((member.name, data))
                        position = stream.tell()
                        if position - offset >= METADATA_ARCHIVE_BATCH_BYTES:
                            break
                        if position < size:
                            member = tarfile.TarInfo.fromtarfile(archive)
            except (tarfile.HeaderError, tarfile.ReadError) as error:
                raise ValueError("Truncated TAR stream") from error
            return files, position if position < size else None

    def count_files(self) -> int:
        """Return the file count, scanning downloaded archives once."""
        if self._count is None:
            count = 0
            continuation = None
            while True:
                files, continuation = self.read_batch(continuation)
                count += len(files)
                if continuation is None:
                    break
            self._count = count
        return self._count

    def content_digest(self) -> str:
        """Return the journal digest in the original file order."""
        if self._digest is None:
            digest = hashlib.blake2b(digest_size=16)
            continuation = None
            while True:
                files, continuation = self.read_batch(continuation)
                for name, data in files:
                    digest.update(json.dumps([name, len(data)]).encode())
                    digest.update(data)
                if continuation is None:
                    break
            return digest.hexdigest()
        return self._digest


MetadataFiles = list[tuple[str, bytes]] | TemporaryMetadataArchive


def _read_member(stream: IO[bytes], member: tarfile.TarInfo) -> bytes:
    """Read one member, preserving the legacy final-padding allowance."""
    stream.seek(member.offset_data)
    data = stream.read(member.size)
    padding = stream.read(-member.size % 512)
    if len(data) != member.size or (
        padding and (len(padding) != -member.size % 512 or any(padding))
    ):
        raise ValueError("Truncated TAR stream")
    return data


def spool_metadata(files: Iterable[tuple[str, bytes]]) -> TemporaryMetadataArchive:
    """Write a metadata iterable and return its completed local archive."""
    writer = MetadataArchiveWriter()
    try:
        for item in files:
            writer.append(item)
        return writer.finish()
    except BaseException:
        writer._cleanup()  # pylint: disable=protected-access
        raise
