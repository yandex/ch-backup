"""
Multipart uploading to storage stage.
"""

from dataclasses import dataclass

from ch_backup.storage.async_pipeline.base_pipeline.handler import Handler
from ch_backup.storage.async_pipeline.stages.types import StageType
from ch_backup.storage.engine.base import PipeLineCompatibleStorageEngine
from ch_backup.util import Slotted


@dataclass
class UploadingPart(Slotted):
    """
    Passed between uploading stages.

    We could use dataclass(slots=true) from functools when the supported version of python would be >= 3.10.
    """

    __slots__ = "data", "upload_id"
    data: bytes
    upload_id: str | None


class StartMultipartUploadStage(Handler):
    """
    Initiate multipart storage uploading and mark all passing blocks in pipeline with upload_id.

    This stage must be started in a single worker.
    """

    stype = StageType.STORAGE

    def __init__(
        self,
        config: dict,
        chunk_size: int,
        loader: PipeLineCompatibleStorageEngine,
        remote_path: str,
    ) -> None:
        self._config = config
        self._loader = loader
        self._remote_path = remote_path
        self._upload_id: str | None = None
        self._chunk_size = chunk_size

    def __call__(self, data: bytes, index: int) -> UploadingPart:
        assert len(data) <= self._chunk_size, "Previous chunking stage must ensure this"

        if self._upload_id is None and len(data) == self._chunk_size:
            # If we get the first full chunk we assume there is more data and use multipart upload
            self._upload_id = self._loader.create_multipart_upload(
                remote_path=self._remote_path
            )

        return UploadingPart(upload_id=self._upload_id, data=data)  # type: ignore[call-arg]


class StorageUploadingStage(Handler):
    """
    Uploads all data blocks to the storage single object as a set of parts.

    If upload_id is not set for the part than usual data uploading (not multipart)
    is performed.
    This stage can be started in parallel.
    """

    stype = StageType.STORAGE

    def __init__(
        self, config: dict, loader: PipeLineCompatibleStorageEngine, remote_path: str
    ) -> None:
        self._config = config
        self._loader = loader
        self._remote_path = remote_path

    def __call__(self, part: UploadingPart, index: int) -> UploadingPart:
        if part.upload_id:
            part_num = index + 1  # Loader expects counting from 1
            self._loader.upload_part(
                part.data, self._remote_path, part.upload_id, part_num
            )
        else:
            self._loader.upload_data(part.data, self._remote_path)

        return part


class CompleteStorageUploadStage(Handler):
    """
    Complete a multipart upload or persist a successfully completed empty stream.

    This stage must be started in a single worker.
    """

    stype = StageType.STORAGE

    def __init__(
        self, config: dict, loader: PipeLineCompatibleStorageEngine, remote_path: str
    ) -> None:
        self._config = config
        self._loader = loader
        self._remote_path = remote_path
        self._upload_id: str | None = None
        self._received_parts = False

    def __call__(self, part: UploadingPart, index: int) -> None:
        self._received_parts = True
        if self._upload_id is None and part.upload_id:
            self._upload_id = part.upload_id

    def on_done(self) -> None:
        if self._upload_id is not None:
            self._loader.complete_multipart_upload(
                remote_path=self._remote_path, upload_id=self._upload_id
            )
        elif not self._received_parts:
            # Empty streams produce no parts. Persist the empty object here so
            # replacement uploads cannot leave old contents behind. The pipeline
            # calls on_done only after successful upstream completion.
            self._loader.upload_data(b"", self._remote_path)
