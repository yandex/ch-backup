"""
Decompressing stage.
"""

from typing import Iterator

from ch_backup.compression.base import BaseCompression
from ch_backup.storage.async_pipeline.base_pipeline.handler import IterableHandler
from ch_backup.storage.async_pipeline.stages.types import StageType


class DecompressStage(IterableHandler):
    """
    Decompresses data and return it by chunks of specified size
    """

    stype = StageType.COMPRESSION

    def __init__(self, compressor: BaseCompression, chunk_size: int) -> None:
        """Limit each item passed to the next queue."""
        self._compressor = compressor
        self._chunk_size = chunk_size

    def __call__(self, data: bytes, index: int) -> Iterator[bytes]:
        """Yield bounded decompressed chunks."""
        yield from self._compressor.decompress_chunks(data, self._chunk_size)

    def on_done(self) -> Iterator[bytes]:
        """Drain output before finalizing the decompressor."""
        yield from self._compressor.decompress_chunks(b"", self._chunk_size)
        decompressed_data = self._compressor.flush_decompress()
        if decompressed_data:
            yield decompressed_data
