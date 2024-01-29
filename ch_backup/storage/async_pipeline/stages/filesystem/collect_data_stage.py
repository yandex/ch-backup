"""
Collecting data stage.
"""

from io import BytesIO

from ch_backup.storage.async_pipeline.base_pipeline.handler import Handler
from ch_backup.storage.async_pipeline.stages.types import StageType


class CollectDataStage(Handler):
    """
    Collect all incoming data blocks and pass it as a single block.
    """

    stype = StageType.FILESYSTEM

    def __init__(self) -> None:
        self._buffer = BytesIO()

    def __call__(self, data: bytes, index: int) -> None:
        self._buffer.write(data)

    def on_done(self) -> bytes:
        return self._buffer.getvalue()
