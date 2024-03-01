"""
Compressing stage.
"""

from ch_backup.compression.base import BaseCompression
from ch_backup.storage.async_pipeline.base_pipeline.handler import Handler
from ch_backup.storage.async_pipeline.stages.types import StageType


class CompressStage(Handler):
    """
    Compresses data.
    """

    stype = StageType.COMPRESSION

    def __init__(self, compressor: BaseCompression) -> None:
        self._compressor = compressor

    def __call__(self, data: bytes, index: int) -> bytes:
        res = self._compressor.compress(data) + self._compressor.flush_compress()
        return res

    def on_done(self) -> bytes:
        return self._compressor.flush_compress()
