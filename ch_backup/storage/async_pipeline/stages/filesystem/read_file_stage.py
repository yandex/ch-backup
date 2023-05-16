"""
Read file stage.
"""
from pathlib import Path
from typing import BinaryIO, Iterable, Optional

from ch_backup.storage.async_pipeline.base_pipeline.handler import InputHandler
from ch_backup.storage.async_pipeline.stages.types import StageType


class ReadFileStage(InputHandler):
    """
    Reads data from file unlimited.
    """

    stype = StageType.FILESYSTEM

    def __init__(self, config: dict, file_path: Path) -> None:
        self._chunk_size = config['chunk_size']
        self._file_path = file_path
        self._fobj: Optional[BinaryIO] = None

    def on_start(self) -> None:
        self._fobj = self._file_path.open(mode='rb')

    def __call__(self) -> Iterable[bytes]:
        assert self._fobj

        while True:
            chunk = self._fobj.read(self._chunk_size)
            if not chunk:
                break
            yield chunk

    def on_done(self) -> None:
        if self._fobj:
            self._fobj.close()
