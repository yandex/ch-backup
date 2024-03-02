"""
Decompressing stage.
"""

from typing import Optional

from ch_backup.compression.base import BaseCompression
from ch_backup.storage.async_pipeline.base_pipeline.bytes_fifo import BytesFIFO
from ch_backup.storage.async_pipeline.base_pipeline.handler import Handler
from ch_backup.storage.async_pipeline.stages.types import StageType


class DecompressStage(Handler):
    """
    Decompresses data.
    """

    stype = StageType.COMPRESSION

    def __init__(
        self, compressor: BaseCompression, chunk_size: int, buffer_size: int
    ) -> None:
        self._compressor = compressor
        self._buffer = BytesFIFO(buffer_size)
        self._chunk_size = chunk_size

    def __call__(self, data: bytes, index: int) -> Optional[bytes]:
        self._buffer.write(self._compressor.decompress(data))

        if len(self._buffer) >= self._chunk_size:
            return self._buffer.read(self._chunk_size)
        return None

    def on_done(self) -> Optional[bytes]:
        self._buffer.write(self._compressor.flush_decompress())
        if len(self._buffer) > 0:
            return self._buffer.read()
        return None
