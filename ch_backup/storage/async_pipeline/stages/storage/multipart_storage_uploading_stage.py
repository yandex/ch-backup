"""
Multipart uploading to storage stage.
"""
from dataclasses import dataclass
from typing import Optional

from ch_backup.storage.async_pipeline.base_pipeline.handler import Handler
from ch_backup.storage.async_pipeline.stages.types import StageType
from ch_backup.storage.engine.base import PipeLineCompatibleStorageEngine


@dataclass
class UploadingPart:
    """
    Passed between uploading stages.
    """
    upload_id: str
    data: bytes


class StartMultipartUploadStage(Handler):
    """
    Initiate multipart storage uploading and mark all passing blocks in pipeline with upload_id.

    This stage must be started in a single worker.
    """

    stype = StageType.STORAGE

    def __init__(self, config: dict, loader: PipeLineCompatibleStorageEngine, remote_path: str) -> None:
        self._config = config
        self._loader = loader
        self._remote_path = remote_path
        self._upload_id: Optional[str] = None

    def on_start(self) -> None:
        self._upload_id = self._loader.create_multipart_upload(remote_path=self._remote_path)

    def __call__(self, data: bytes, index: int) -> UploadingPart:
        assert self._upload_id is not None

        return UploadingPart(upload_id=self._upload_id, data=data)  # type: ignore[call-arg]


class MultipartUploadStage(Handler):
    """
    Uploads all data blocks to the storage single object as a set of parts.

    This stage can be started in parallel.
    """

    stype = StageType.STORAGE

    def __init__(self, config: dict, loader: PipeLineCompatibleStorageEngine, remote_path: str) -> None:
        self._config = config
        self._loader = loader
        self._remote_path = remote_path

    def __call__(self, part: UploadingPart, index: int) -> UploadingPart:
        part_num = index + 1  # Count from 1
        self._loader.upload_part(part.data, self._remote_path, part.upload_id, part_num)

        return part


class CompleteMultipartUploadStage(Handler):
    """
    Complete multipart uploading.

    This stage must be started in a single worker.
    """

    stype = StageType.STORAGE

    def __init__(self, config: dict, loader: PipeLineCompatibleStorageEngine, remote_path: str) -> None:
        self._config = config
        self._loader = loader
        self._remote_path = remote_path
        self._upload_id: Optional[str] = None

    def __call__(self, part: UploadingPart, index: int) -> None:
        if self._upload_id is None:
            self._upload_id = part.upload_id

    def on_done(self) -> None:
        if self._upload_id is not None:
            self._loader.complete_multipart_upload(remote_path=self._remote_path, upload_id=self._upload_id)
