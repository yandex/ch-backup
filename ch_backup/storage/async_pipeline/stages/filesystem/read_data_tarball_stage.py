"""
Reading in-memory data to TAR stream stage.
"""

import tarfile
import time
from io import BytesIO
from typing import Iterator, List

from ch_backup.storage.async_pipeline.base_pipeline.handler import InputHandler
from ch_backup.storage.async_pipeline.stages.types import StageType
from ch_backup.util import read_by_chunks


class ReadDataTarballStage(InputHandler):
    """
    Read in-memory data and archive it to TAR stream.
    """

    stype = StageType.FILESYSTEM

    def __init__(
        self,
        config: dict,
        file_names: List[str],
        data_list: List[bytes],
    ) -> None:
        self._chunk_size = config["chunk_size"]
        self._file_names = file_names
        self._data_list = data_list

    def __call__(self) -> Iterator[bytes]:
        """
        Read data and yield them as TAR stream.
        """
        for file_name, data in zip(self._file_names, self._data_list):
            yield self.make_tar_header(file_name, len(data))
            yield from self.read_data_content(data)

    def read_data_content(self, data: bytes) -> Iterator[bytes]:
        """
        Produce data content by chunks.
        """
        data_io = BytesIO(data)
        yield from read_by_chunks(data_io, self._chunk_size)

        # Fill padding for last file's TAR block
        remainder = len(data) % tarfile.BLOCKSIZE
        if remainder > 0:
            yield tarfile.NUL * (tarfile.BLOCKSIZE - remainder)

    @staticmethod
    def make_tar_header(name: str, size: int) -> bytes:
        """
        Compose TAR header for in-memory data.
        """
        tarinfo = tarfile.TarInfo(name)
        tarinfo.mtime = int(time.time())
        tarinfo.size = size
        return tarinfo.tobuf(format=tarfile.GNU_FORMAT)