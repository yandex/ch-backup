"""
Write file stage.
"""

from io import FileIO
from typing import Any

from ch_backup.storage.async_pipeline.base_pipeline.handler import Handler
from ch_backup.storage.async_pipeline.stages.types import StageType


class WriteFileStage(Handler):
    """
    Write consumed from pipeline data to file.
    """

    stype = StageType.FILESYSTEM

    def __init__(self, file: Any) -> None:
        self._fobj = None
        self._owns_file = not isinstance(file, FileIO)

        if self._owns_file:
            self._file_path = file
        else:
            self._fobj = file

    def on_start(self) -> None:
        if self._owns_file:
            # pylint: disable=consider-using-with
            self._fobj = self._file_path.open("bw", buffering=0)

    def __call__(self, data: bytes, index: int) -> None:
        assert self._fobj
        self._fobj.write(data)

    def on_done(self) -> None:
        if self._fobj and self._owns_file:
            self._fobj.close()
