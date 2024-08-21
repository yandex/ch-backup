"""
Reading files to TAR stream stage.
"""

import tarfile
from pathlib import Path
from typing import Iterator, List, Tuple

from ch_backup.storage.async_pipeline.base_pipeline.handler import IterableHandler
from ch_backup.storage.async_pipeline.stages.backup.stage_communication import (
    PartPipelineInfo,
)
from ch_backup.storage.async_pipeline.stages.types import StageType
from ch_backup.util import read_by_chunks


class ReadFilesTarballPipelineStage(IterableHandler):
    """
    Base class for read files from pipeline to tarball stage.
    """

    stype = StageType.FILESYSTEM

    def __init__(
        self,
        config: dict,
    ) -> None:
        self._chunk_size = config["chunk_size"]

    def __call__(
        self, files_and_info: Tuple[List[str], PartPipelineInfo], index: int
    ) -> Iterator[Tuple[bytes, PartPipelineInfo]]:
        """
        Read files and yield them as TAR stream.
        """
        relative_paths, part_info = files_and_info
        base_path = part_info.part_path
        for file_relative_path in relative_paths:
            file_path = Path(base_path) / file_relative_path
            yield (self.make_tar_header(str(file_relative_path), file_path), part_info)
            for data in self.read_file_content(file_path):
                yield (data, part_info)

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
