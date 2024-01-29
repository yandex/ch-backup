"""
Deleting objects from storage stage.
"""

from typing import Sequence

from ch_backup.storage.async_pipeline.base_pipeline.handler import InputHandler
from ch_backup.storage.async_pipeline.stages.types import StageType
from ch_backup.storage.engine import PipeLineCompatibleStorageEngine
from ch_backup.util import chunked, retry


class DeleteMultipleStorageStage(InputHandler):
    """
    Delete object in the storage in chunked manner.
    """

    stype = StageType.STORAGE

    def __init__(
        self,
        config: dict,
        remote_paths: Sequence[str],
        storage: PipeLineCompatibleStorageEngine,
    ) -> None:
        self._storage = storage
        self._bulk_delete_chunk_size = config["bulk_delete_chunk_size"]
        self._remote_paths = remote_paths

    def __call__(self) -> None:
        pass

    def on_done(self) -> None:
        for paths in chunked(self._remote_paths, self._bulk_delete_chunk_size):
            self._delete_files(paths)

    @retry()
    def _delete_files(self, paths: Sequence[str]) -> None:
        self._storage.delete_files(paths)
