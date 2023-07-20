"""
Write file stage.
"""
from pathlib import Path
from typing import BinaryIO, Optional

from ch_backup.storage.async_pipeline.base_pipeline.handler import Handler
from ch_backup.storage.async_pipeline.stages.types import StageType


class WriteFileStage(Handler):
    """
    Write consumed from pipeline data to file.
    """

    stype = StageType.FILESYSTEM

    def __init__(self, file_path: Path) -> None:
        self._file_path = file_path
        self._fobj: Optional[BinaryIO] = None

    def on_start(self) -> None:
        self._fobj = self._file_path.open('bw', buffering=0)  # Switch off buffering

    def __call__(self, data: bytes, index: int) -> None:
        assert self._fobj
        self._fobj.write(data)

    def on_done(self) -> None:
        if self._fobj:
            self._fobj.close()
