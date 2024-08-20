"""
Compressing stage.
"""

from typing import Iterable, Optional, Tuple

from ch_backup.compression.base import BaseCompression
from ch_backup.storage.async_pipeline.base_pipeline.handler import Handler, IterableHandler
from ch_backup.storage.async_pipeline.stages.backup.stage_communication import PartPipelineInfo
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


class CompressPartStage(IterableHandler):
    """
    Compresses data and return it by chunks of specified size
    """

    stype = StageType.COMPRESSION

    def __init__(self, compressor: BaseCompression) -> None:
        self._compressor = compressor
        self._last_part_info: Optional[PartPipelineInfo] = None

    def __call__(self, value: Tuple[bytes, PartPipelineInfo], index: int) -> Iterable[Tuple[bytes, PartPipelineInfo]]:
        data, part_info = value
        if self._last_part_info is not None and part_info.part_metadata.name != self._last_part_info.part_metadata.name:
            compressed_data = self._compressor.flush_compress()
            if len(compressed_data) > 0:
                yield (compressed_data, self._last_part_info)
    
        self._last_part_info = part_info
        compressed_data = self._compressor.compress(data)
        if len(compressed_data) > 0:
            yield (compressed_data, self._last_part_info)

    def on_done(self) -> Iterable[Tuple[bytes, PartPipelineInfo]]:
        compressed_data = self._compressor.flush_compress()
        if len(compressed_data) > 0:
            yield (compressed_data, self._last_part_info)
