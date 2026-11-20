"""
Downloading object from a storage stage.
"""

from typing import Generator

from ch_backup.storage.async_pipeline.base_pipeline.handler import InputHandler
from ch_backup.storage.async_pipeline.stages.types import StageType
from ch_backup.storage.engine.base import PipeLineCompatibleStorageEngine


class DownloadStorageStage(InputHandler):
    """
    Download object from the storage as a set of parts.
    """

    stype = StageType.STORAGE

    def __init__(
        self,
        config: dict,
        loader: PipeLineCompatibleStorageEngine,
        remote_path: str,
        *,
        range_size: int = 0,
    ) -> None:
        """Read chunks with an optional larger persistent HTTP range."""
        self._chunk_size = config["chunk_size"]
        self._range_size = range_size
        self._loader = loader
        self._remote_path = remote_path
        self._download_id: str | None = None

    def on_start(self) -> None:
        """Resolve the object identity before reading its first byte."""
        self._download_id = self._loader.create_multipart_download(self._remote_path)

    def __call__(self) -> Generator[bytes, None, None]:
        """Yield bounded chunks and close the response even on cancellation."""
        options = {"range_len": self._range_size} if self._range_size else {}
        try:
            while True:
                data = self._loader.download_part(
                    download_id=self._download_id, part_len=self._chunk_size, **options
                )
                if not data:
                    return
                yield data
        finally:
            self.on_done()

    def on_done(self) -> None:
        """Release the download once after exhaustion or an interrupted consumer."""
        if self._download_id is not None:
            self._loader.complete_multipart_download(download_id=self._download_id)
            self._download_id = None
