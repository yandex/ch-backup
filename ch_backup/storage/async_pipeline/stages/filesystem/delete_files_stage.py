"""
Deleting files stage.
"""

from pathlib import Path
from typing import Any, List, Optional

from ch_backup.storage.async_pipeline.base_pipeline.handler import Handler
from ch_backup.storage.async_pipeline.stages.types import StageType
from ch_backup.util import scan_dir_files


class DeleteFilesScanStage(Handler):
    """
    Delete files from local filesystem.
    Do not load all file names in memory.
    """

    stype = StageType.FILESYSTEM

    def __init__(
        self,
        config: dict,
        base_path: Path,
        exclude_file_names: Optional[list[str]] = None,
    ) -> None:
        self._config = config
        self._base_path = base_path
        self._exclude_file_names = exclude_file_names

    def __call__(self, _: Any, index: int) -> None:
        pass

    def on_done(self) -> None:
        for filename in scan_dir_files(self._base_path, self._exclude_file_names):
            filepath = self._base_path / filename
            filepath.unlink()


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
