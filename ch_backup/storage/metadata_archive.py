"""Keep disk metadata in private local tar files rather than Python lists."""

import hashlib
import json
import os
import weakref
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import IO, Iterable, Iterator

from ch_backup.storage.async_pipeline.stages.filesystem.read_files_tarball_stage import (
    _make_tar_header,
)
from ch_backup.storage.async_pipeline.stages.filesystem.write_files_stage import (
    UnpackTarballStage,
)


def _remove_archive(file: IO[bytes], path: str) -> None:
    """Close an unfinished writer and remove its private archive."""
    file.close()
    os.unlink(path)


class MetadataArchive:
    """Own an uncompressed archive until all readers and uploads finish."""

    def __init__(self, downloaded: bool = False) -> None:
        """Create a private file and optional incremental content digest."""
        # pylint: disable-next=consider-using-with
        self._file = NamedTemporaryFile(delete=False)
        self._cleanup = weakref.finalize(
            self, _remove_archive, self._file, self._file.name
        )
        self._count: int | None = None if downloaded else 0
        self._digest = None if downloaded else hashlib.blake2b(digest_size=16)

    def path(self) -> str:
        """Seal the writer before readers use the immutable archive."""
        if not self._file.closed:
            self._file.close()
        return self._file.name

    def append(self, item: tuple[str, bytes]) -> None:
        """Append one metadata file and update its resume identity."""
        assert self._count is not None and self._digest is not None
        name, data = item
        self._file.write(_make_tar_header(name, len(data)))
        self._file.write(data)
        self._file.write(b"\0" * (-len(data) % 512))
        self._digest.update(json.dumps([name, len(data)]).encode())
        self._digest.update(data)
        self._count += 1

    def __iter__(self) -> Iterator[tuple[str, bytes]]:
        """Parse a bounded chunk at a time without caching tar headers."""
        chunk_size = 1024 * 1024
        reader = UnpackTarballStage({}, chunk_size + 512, include_empty=True)
        with Path(self.path()).open("rb") as stream:
            for index, chunk in enumerate(iter(lambda: stream.read(chunk_size), b"")):
                reader(chunk, index)
                yield from reader.take_files()
            yield from reader.on_done()

    def __len__(self) -> int:
        """Return the cached number of files, counting downloaded archives once."""
        if self._count is None:
            self._count = sum(1 for _ in self)
        return self._count

    def content_digest(self) -> str:
        """Return the journal digest in the original file order."""
        if self._digest is None:
            digest = hashlib.blake2b(digest_size=16)
            for name, data in self:
                digest.update(json.dumps([name, len(data)]).encode())
                digest.update(data)
            return digest.hexdigest()
        return self._digest.hexdigest()


MetadataFiles = list[tuple[str, bytes]] | MetadataArchive


def spool_metadata(files: Iterable[tuple[str, bytes]]) -> MetadataArchive:
    """Write a metadata iterable to a local archive."""
    archive = MetadataArchive()
    for item in files:
        archive.append(item)
    return archive
