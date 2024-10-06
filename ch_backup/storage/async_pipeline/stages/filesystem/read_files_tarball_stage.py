"""
Reading files to TAR stream stage.
"""

import tarfile
from pathlib import Path
from typing import Any, Iterable, Iterator, List, Optional

from ch_backup.storage.async_pipeline.base_pipeline.handler import InputHandler
from ch_backup.storage.async_pipeline.stages.types import StageType
from ch_backup.util import read_by_chunks, scan_dir_files


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

            yield self.make_tar_header(str(file_path_in_tar), file_path)
            yield from self.read_file_content(file_path)

    def read_file_content(self, file_path: Path) -> Iterator[bytes]:
        """
        Produce file content by chunks.
        """
        with file_path.open(mode="rb") as file:
            yield from read_by_chunks(file, self._chunk_size)

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
        tar_base_dir: Optional[Path] = None,
    ) -> None:
        super().__init__(config, base_path, tar_base_dir)
        self._file_source = file_relative_paths
