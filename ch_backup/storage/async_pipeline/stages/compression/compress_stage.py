"""
Compressing stage.
"""

from typing import Optional

from ch_backup.compression.base import BaseCompression
from ch_backup.storage.async_pipeline.base_pipeline.bytes_fifo import BytesFIFO
from ch_backup.storage.async_pipeline.base_pipeline.handler import Handler
from ch_backup.storage.async_pipeline.stages.types import StageType


class CompressStage(Handler):
    """
    Compresses data and return it by chunks of specified size
    """

    stype = StageType.COMPRESSION

    def __init__(
        self, compressor: BaseCompression, chunk_size: int, buffer_size: int
    ) -> None:
        self._compressor = compressor
        self._buffer = BytesFIFO(buffer_size)
        self._chunk_size = chunk_size

    def __call__(self, data: bytes, index: int) -> Optional[bytes]:
        compressed_data = self._compressor.compress(data)
        if len(compressed_data) > 0:
            self._buffer.write(compressed_data)

        if len(self._buffer) >= self._chunk_size:
            return self._buffer.read(self._chunk_size)
        return None

    def on_done(self) -> Optional[bytes]:
        self._buffer.write(self._compressor.flush_compress())
        if len(self._buffer) > 0:
            return self._buffer.read()
        return None
