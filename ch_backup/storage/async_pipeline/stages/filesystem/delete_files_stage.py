"""
Deleting files stage.
"""

from pathlib import Path
from typing import Any, List

from ch_backup.storage.async_pipeline.base_pipeline.handler import Handler
from ch_backup.storage.async_pipeline.stages.types import StageType


class DeleteFilesStage(Handler):
    """
    Delete files from local filesystem.
    """

    stype = StageType.FILESYSTEM

    def __init__(self, config: dict, files: List[Path]) -> None:
        self._config = config
        self._files = files

    def __call__(self, _: Any, index: int) -> None:
        pass

    def on_done(self) -> None:
        for file in self._files:
            file.unlink()
