"""
Uploading to storage stage.
"""
from ch_backup.storage.async_pipeline.base_pipeline.handler import Handler
from ch_backup.storage.async_pipeline.stages.types import StageType
from ch_backup.storage.engine.base import PipeLineCompatibleStorageEngine


class StorageUploadingStage(Handler):
    """
    Upload every incoming data block as single object to the storage.
    """
    stype = StageType.STORAGE

    def __init__(self, config: dict, loader: PipeLineCompatibleStorageEngine, remote_path: str) -> None:
        self.config = config
        self._loader = loader
        self._remote_path = remote_path

    def __call__(self, data: bytes, index: int) -> None:
        self._loader.upload_data(data, self._remote_path)
