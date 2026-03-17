"""
Reading files to TAR stream stage.
"""

import tarfile
import time
from io import BytesIO
from pathlib import Path
from typing import Any, BinaryIO, Iterable, Iterator, List, Optional

from ch_backup.storage.async_pipeline.base_pipeline.handler import InputHandler
from ch_backup.storage.async_pipeline.stages.types import StageType
from ch_backup.util import read_by_chunks, scan_dir_files


def _make_tar_header_from_file(name: str, file_path: Path) -> bytes:
    """
    Compose TAR header for filesystem file.
    """
    tarinfo = tarfile.TarInfo(name)
    stat = file_path.stat()
    tarinfo.size = stat.st_size
    tarinfo.mtime = int(stat.st_mtime)
    return tarinfo.tobuf(format=tarfile.GNU_FORMAT)


def _make_tar_header(name: str, size: int) -> bytes:
    """
    Compose TAR header for in-memory data.
    """
    tarinfo = tarfile.TarInfo(name)
    tarinfo.size = size
    tarinfo.mtime = int(time.time())
    return tarinfo.tobuf(format=tarfile.GNU_FORMAT)


class ReadFilesTarballStageBase(InputHandler):
    """
    Base class for read files to tarball stage.
    """

    stype = StageType.FILESYSTEM

    def __init__(
        self,
        config: dict,
        base_path: Path,
        tar_base_dir: Optional[str] = None,
    ) -> None:
        self._chunk_size = config["chunk_size"]
        self._base_path = base_path
        self._file_source: Iterable[Any] = []
        self._tar_base_dir: Optional[str] = tar_base_dir

    def __call__(self) -> Iterator[bytes]:
        """
        Read files and yield them as TAR stream.
        """
        for file_relative_path in self._file_source:
            file_path = self._base_path / file_relative_path
            file_path_in_tar = (
                Path(self._tar_base_dir) / file_relative_path
                if self._tar_base_dir
                else file_relative_path
            )

            yield _make_tar_header_from_file(str(file_path_in_tar), file_path)
            yield from self._read_file_content(file_path)

    def _read_file_content(self, file_path: Path) -> Iterator[bytes]:
        """
        Produce file content by chunks from filesystem.
        """
        with file_path.open(mode="rb") as file:
            yield from self._read_content(file, file_path.stat().st_size)

    def _read_content(self, stream: BinaryIO, size: int) -> Iterator[bytes]:
        """
        Read content from stream by chunks and add TAR padding.
        """
        yield from read_by_chunks(stream, self._chunk_size)

        # Fill padding for last file's TAR block
        remainder = size % tarfile.BLOCKSIZE
        if remainder > 0:
            yield tarfile.NUL * (tarfile.BLOCKSIZE - remainder)


class ReadFilesTarballScanStage(ReadFilesTarballStageBase):
    """
    Read and archive files from a directory to TAR stream.
    Do not load all file names in memory.
    """

    def __init__(
        self,
        config: dict,
        base_path: Path,
        tar_base_dir: Optional[str] = None,
        exclude_file_names: Optional[List[str]] = None,
    ) -> None:
        super().__init__(config, base_path, tar_base_dir)
        self._file_source = scan_dir_files(self._base_path, exclude_file_names)


class ReadFilesTarballStage(ReadFilesTarballStageBase):
    """
    Read and archive files to TAR stream.
    """

    def __init__(
        self,
        config: dict,
        base_path: Path,
        file_relative_paths: List[Path],
        tar_base_dir: Optional[str] = None,
    ) -> None:
        super().__init__(config, base_path, tar_base_dir)
        self._file_source = file_relative_paths


class ReadDataTarballStage(ReadFilesTarballStageBase):
    """
    Read in-memory data and archive it to TAR stream.
    """

    def __init__(
        self,
        config: dict,
        file_names: List[str],
        data_list: List[bytes],
    ) -> None:
        # Call parent with dummy base_path since we don't use filesystem
        super().__init__(config, Path("."))
        self._file_names = file_names
        self._data_list = data_list

    def __call__(self) -> Iterator[bytes]:
        """
        Read data and yield them as TAR stream.
        """
        for file_name, data in zip(self._file_names, self._data_list):
            yield _make_tar_header(file_name, len(data))
            yield from self._read_content(BytesIO(data), len(data))
