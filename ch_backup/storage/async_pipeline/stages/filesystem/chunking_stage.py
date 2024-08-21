"""
Chunking stage.
"""

from typing import Iterator, Optional, Tuple

from ch_backup.storage.async_pipeline.base_pipeline.bytes_fifo import BytesFIFO
from ch_backup.storage.async_pipeline.base_pipeline.handler import Handler
from ch_backup.storage.async_pipeline.stages.backup.stage_communication import (
    PartPipelineInfo,
)
from ch_backup.storage.async_pipeline.stages.types import StageType


class ChunkingStage(Handler):
    """
    Re-chunk incoming bytes stream to chunks of specified size.
    """

    stype = StageType.FILESYSTEM

    def __init__(self, chunk_size: int, buffer_size: int) -> None:
        if chunk_size > buffer_size:
            raise ValueError(
                f"The chunk size {chunk_size} can't be greater than the buffer size {buffer_size}"
            )

        self._chunk_size = chunk_size
        self._buffer = BytesFIFO(buffer_size)

    def __call__(self, value: bytes, index: int) -> Iterator[bytes]:
        while len(value) > 0:
            written = self._buffer.write(value)
            value = value[written:]

            while len(self._buffer) >= self._chunk_size:
                data = self._buffer.read(self._chunk_size)
                yield data

    def on_done(self) -> Iterator[bytes]:
        assert len(self._buffer) < self._chunk_size

        data = self._buffer.read()
        if len(data) > 0:
            yield data


class ChunkingPartStage(Handler):
    """
    Re-chunk incoming bytes stream to chunks of specified size.
    """

    stype = StageType.FILESYSTEM

    def __init__(self, chunk_size: int, buffer_size: int) -> None:
        if chunk_size > buffer_size:
            raise ValueError(
                f"The chunk size {chunk_size} can't be greater than the buffer size {buffer_size}"
            )

        self._chunk_size = chunk_size
        self._buffer = BytesFIFO(buffer_size)
        self._last_part_info: Optional[PartPipelineInfo] = None

    def __call__(
        self, value: Tuple[bytes, PartPipelineInfo], index: int
    ) -> Iterator[Tuple[bytes, PartPipelineInfo]]:
        data, part_info = value
        # New part is passed, yield all previous part data
        if (
            self._last_part_info is not None
            and part_info.part_metadata.name != self._last_part_info.part_metadata.name
        ):
            while len(self._buffer) >= self._chunk_size:
                result_data = self._buffer.read(self._chunk_size)
                yield (result_data, self._last_part_info)

        self._last_part_info = part_info
        while len(data) > 0:
            written = self._buffer.write(data)
            data = data[written:]

            while len(self._buffer) >= self._chunk_size:
                result_data = self._buffer.read(self._chunk_size)
                yield (result_data, part_info)

    def on_done(self) -> Iterator[Tuple[bytes, PartPipelineInfo]]:
        assert len(self._buffer) < self._chunk_size

        data = self._buffer.read()
        if len(data) > 0:
            yield (data, self._last_part_info)
