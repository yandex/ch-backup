"""
Write file stage.
"""

from pathlib import Path
from typing import BinaryIO, Optional, Union

from ch_backup.storage.async_pipeline.base_pipeline.handler import Handler
from ch_backup.storage.async_pipeline.stages.types import StageType


class WriteFileStage(Handler):
    """
    Write consumed from pipeline data to file.
    """

    stype = StageType.FILESYSTEM

    def __init__(self, file: Union[Path, BinaryIO]) -> None:
        self._fobj: Optional[BinaryIO] = None
        self._file = file

    def on_start(self) -> None:
        if isinstance(self._file, Path):
            # pylint: disable=consider-using-with
            self._fobj = self._file.open("bw", buffering=0)
        else:
            self._fobj = self._file

    def __call__(self, data: bytes, index: int) -> None:
        assert self._fobj
        self._fobj.write(data)

    def on_done(self) -> None:
        if self._fobj and isinstance(self._file, Path):
            self._fobj.close()
