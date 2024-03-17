"""
Reading files to TAR stream stage.
"""

import tarfile
from pathlib import Path
from typing import Iterator, List, Optional

from ch_backup.storage.async_pipeline.base_pipeline.handler import InputHandler
from ch_backup.storage.async_pipeline.stages.types import StageType
from ch_backup.util import read_by_chunks, scan_dir_files


class ReadFilesTarballScanStage(InputHandler):
    """
    Read and archive files to TAR stream.
    """

    stype = StageType.FILESYSTEM

    def __init__(
        self,
        config: dict,
        base_path: Path,
        exclude_file_names: Optional[list[str]] = None,
    ) -> None:
        self._chunk_size = config["chunk_size"]
        self._base_path = base_path
        self._exclude_file_names = exclude_file_names

    def __call__(self) -> Iterator[bytes]:
        """
        Read files and yield them as TAR stream.
        """
        for file_relative_path in scan_dir_files(
            self._base_path, self._exclude_file_names
        ):
            file_path = self._base_path / file_relative_path

            yield self.make_tar_header(str(file_relative_path), file_path)
            yield from self.read_file_content(file_path)

    def read_file_content(self, file_path: Path) -> Iterator[bytes]:
        """
        Produce file content by chunks.
        """
        with file_path.open(mode="rb") as file:
            for chunk in read_by_chunks(file, self._chunk_size):
                yield chunk

            # Fill padding for last file's TAR block
            remainder = file_path.stat().st_size % tarfile.BLOCKSIZE
            if remainder > 0:
                yield tarfile.NUL * (tarfile.BLOCKSIZE - remainder)

    @staticmethod
    def make_tar_header(name: str, file_path: Path) -> bytes:
        """
        Compose TAR header.
        """
        tarinfo = tarfile.TarInfo(name)
        stat = file_path.stat()
        tarinfo.mtime, tarinfo.size = int(stat.st_mtime), stat.st_size
        return tarinfo.tobuf(format=tarfile.GNU_FORMAT)


class ReadFilesTarballStage(InputHandler):
    """
    Read and archive files to TAR stream.
    """

    stype = StageType.FILESYSTEM

    def __init__(
        self, config: dict, base_path: Path, file_relative_paths: List[Path]
    ) -> None:
        self._chunk_size = config["chunk_size"]
        self._base_path = base_path
        self._file_relative_paths = file_relative_paths

    def __call__(self) -> Iterator[bytes]:
        """
        Read files and yield them as TAR stream.
        """
        for file_relative_path in self._file_relative_paths:
            file_path = self._base_path / file_relative_path

            yield self.make_tar_header(str(file_relative_path), file_path)
            yield from self.read_file_content(file_path)

    def read_file_content(self, file_path: Path) -> Iterator[bytes]:
        """
        Produce file content by chunks.
        """
        with file_path.open(mode="rb") as file:
            for chunk in read_by_chunks(file, self._chunk_size):
                yield chunk

            # Fill padding for last file's TAR block
            remainder = file_path.stat().st_size % tarfile.BLOCKSIZE
            if remainder > 0:
                yield tarfile.NUL * (tarfile.BLOCKSIZE - remainder)

    @staticmethod
    def make_tar_header(name: str, file_path: Path) -> bytes:
        """
        Compose TAR header.
        """
        tarinfo = tarfile.TarInfo(name)
        stat = file_path.stat()
        tarinfo.mtime, tarinfo.size = int(stat.st_mtime), stat.st_size
        return tarinfo.tobuf(format=tarfile.GNU_FORMAT)
