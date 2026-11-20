"""
Chunking stage.
"""

from typing import Iterator

from ch_backup.storage.async_pipeline.base_pipeline.bytes_fifo import BytesFIFO
from ch_backup.storage.async_pipeline.base_pipeline.handler import Handler
from ch_backup.storage.async_pipeline.stages.types import StageType


class ChunkingStage(Handler):
    """
    Re-chunk incoming bytes stream to chunks of specified size.
    """

    stype = StageType.FILESYSTEM

    def __init__(
        self, chunk_size: int, buffer_size: int, *, direct_chunks: bool = False
    ) -> None:
        """Keep a partial chunk, or use the legacy FIFO for comparison."""
        if chunk_size <= 0:
            raise ValueError("The chunk size must be positive")
        if chunk_size > buffer_size:
            raise ValueError(
                f"The chunk size {chunk_size} can't be greater than the buffer size {buffer_size}"
            )

        self._chunk_size = chunk_size
        self._direct_chunks = direct_chunks
        self._buffer = BytesFIFO(chunk_size if direct_chunks else buffer_size)

    def __call__(self, value: bytes, index: int) -> Iterator[bytes]:
        """Emit complete chunks in order and retain only unfinished bytes."""
        if self._direct_chunks:
            offset = 0
            if not self._buffer.empty():
                offset = min(self._chunk_size - len(self._buffer), len(value))
                self._buffer.write(value[:offset])
                if self._buffer.full():
                    yield self._buffer.read()
            while offset + self._chunk_size <= len(value):
                yield value[offset : offset + self._chunk_size]
                offset += self._chunk_size
            self._buffer.write(value[offset:])
            return

        while len(value) > 0:
            written = self._buffer.write(value)
            value = value[written:]

            while len(self._buffer) >= self._chunk_size:
                data = self._buffer.read(self._chunk_size)
                yield data

    def on_done(self) -> Iterator[bytes]:
        """Emit the final short chunk without padding."""
        assert len(self._buffer) < self._chunk_size

        data = self._buffer.read()
        if len(data) > 0:
            yield data
