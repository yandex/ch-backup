"""
Compressing stage.
"""

from typing import Optional

from ch_backup.compression.base import BaseCompression
from ch_backup.storage.async_pipeline.base_pipeline.handler import Handler
from ch_backup.storage.async_pipeline.stages.types import StageType


class CompressStage(Handler):
    """
    Compresses data and return it by chunks of specified size
    """

    stype = StageType.COMPRESSION

    def __init__(self, compressor: BaseCompression) -> None:
        self._compressor = compressor

    def __call__(self, data: bytes, index: int) -> Optional[bytes]:
        compressed_data = self._compressor.compress(data)
        if len(compressed_data) > 0:
            return compressed_data
        return None

    def on_done(self) -> Optional[bytes]:
        compressed_data = self._compressor.flush_compress()
        if len(compressed_data) > 0:
            return compressed_data
        return None
