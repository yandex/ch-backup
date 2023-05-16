"""
Downloading object from a storage stage.
"""
from typing import Iterable, Optional

from ch_backup.storage.async_pipeline.base_pipeline.handler import InputHandler
from ch_backup.storage.async_pipeline.stages.types import StageType
from ch_backup.storage.engine.base import PipeLineCompatibleStorageEngine


class DownloadStorageStage(InputHandler):
    """
    Download object from the storage as a set of parts.
    """

    stype = StageType.STORAGE

    def __init__(self, config: dict, loader: PipeLineCompatibleStorageEngine, remote_path: str) -> None:
        self._chunk_size = config['chunk_size']
        self._loader = loader
        self._remote_path = remote_path
        self._download_id: Optional[str] = None

    def on_start(self) -> None:
        self._download_id = self._loader.create_multipart_download(self._remote_path)

    def __call__(self) -> Iterable[bytes]:
        while True:
            data = self._loader.download_part(download_id=self._download_id, part_len=self._chunk_size)
            if not data:
                return
            yield data

    def on_done(self) -> None:
        self._loader.complete_multipart_download(download_id=self._download_id)
