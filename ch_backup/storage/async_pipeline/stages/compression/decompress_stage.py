"""
Decompressing stage.
"""

from typing import Optional

from ch_backup.compression.base import BaseCompression
from ch_backup.storage.async_pipeline.base_pipeline.handler import Handler
from ch_backup.storage.async_pipeline.stages.types import StageType


class DecompressStage(Handler):
    """
    Decompresses data and return it by chunks of specified size
    """

    stype = StageType.COMPRESSION

    def __init__(self, compressor: BaseCompression) -> None:
        self._compressor = compressor

    def __call__(self, data: bytes, index: int) -> Optional[bytes]:
        decompressed_data = self._compressor.decompress(data)
        if len(decompressed_data) > 0:
            return decompressed_data
        return None

    def on_done(self) -> Optional[bytes]:
        decompressed_data = self._compressor.flush_decompress()
        if len(decompressed_data) > 0:
            return decompressed_data
        return None
