"""
Deleting files stage.
"""

from pathlib import Path
from typing import Any, Callable, List

from ch_backup.storage.async_pipeline.base_pipeline.handler import Handler
from ch_backup.storage.async_pipeline.stages.types import StageType
from ch_backup.util import scan_dir_files


class DeleteFilesStage(Handler):
    """
    Delete files from local filesystem.
    """

    stype = StageType.FILESYSTEM

    def __init__(self, config: dict, base_path: Path, file_filter: Callable) -> None:
        self._config = config
        self._base_path = base_path
        self._file_filter = file_filter

    def __call__(self, _: Any, index: int) -> None:
        pass

    def on_done(self) -> None:
        for filename in scan_dir_files(self._base_path, self._file_filter):
            filepath = self._base_path / filename
            filepath.unlink()


class DeleteFilesStageInMemory(Handler):
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
