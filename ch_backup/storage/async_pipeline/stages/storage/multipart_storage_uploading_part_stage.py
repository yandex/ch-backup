"""
Multipart uploading to storage stage.
"""

from dataclasses import dataclass
from typing import Optional

import ch_backup.storage.async_pipeline.stages.backup.stage_communication as stage_communication
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

    __slots__ = "data", "upload_id", "part_info"
    data: bytes
    upload_id: Optional[str]
    part_info: stage_communication.PartPipelineInfo


# TODO: if not working for more than 1 part - try IterableHandler
class StartMultipartUploadPartStage(Handler):
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
    ) -> None:
        self._config = config
        self._loader = loader
        self._chunk_size = chunk_size
        self._last_part_info: Optional[stage_communication.PartPipelineInfo] = None
        self._upload_id = None

    def __call__(
        self, value: tuple[bytes, stage_communication.PartPipelineInfo], index: int
    ) -> UploadingPart:
        data, part_info = value
        assert len(data) <= self._chunk_size, "Previous chunking stage must ensure this"

        if len(data) < self._chunk_size:
            # Part is smaller than chunk
            if self._is_new_part(part_info):
                self._last_part_info = part_info
                return UploadingPart(data, None, part_info)
            # Last chunk of part
            return UploadingPart(data, self._upload_id, part_info)

        # If we get the first full chunk we assume there is more data and use multipart upload
        if self._is_new_part(part_info):
            self._upload_id = self._loader.create_multipart_upload(
                remote_path=part_info.remote_path
            )
            self._last_part_info = part_info
            return UploadingPart(data, self._upload_id, part_info)  # type: ignore[call-arg]
        # Continue uploading current part
        return UploadingPart(data, self._upload_id, part_info)  # type: ignore[call-arg]

    def _is_new_part(self, part_info: stage_communication.PartPipelineInfo):
        return (
            self._last_part_info is None
            or self._last_part_info.part_metadata.name != part_info.part_metadata.name
        )


class StorageUploadingPartStage(Handler):
    """
    Uploads all data blocks to the storage single object as a set of parts.

    If upload_id is not set for the part than usual data uploading (not multipart)
    is performed.
    This stage can be started in parallel.
    """

    stype = StageType.STORAGE

    def __init__(self, config: dict, loader: PipeLineCompatibleStorageEngine) -> None:
        self._config = config
        self._loader = loader
        self._upload_id = None
        self._part_num = 1

    def __call__(self, part: UploadingPart, index: int) -> UploadingPart:
        if part.upload_id:
            # New part
            if self._upload_id != part.upload_id:
                self._upload_id = part.upload_id
                self._part_num = 1  # Loader expects counting from 1
            self._loader.upload_part(
                part.data, part.part_info.remote_path, part.upload_id, self._part_num
            )
            self._part_num += 1
        else:
            self._loader.upload_data(part.data, part.part_info.remote_path)

        return part


class CompleteMultipartUploadPartStage(Handler):
    """
    Complete multipart uploading if needed.

    This stage must be started in a single worker.
    """

    stype = StageType.STORAGE

    def __init__(
        self,
        config: dict,
        loader: PipeLineCompatibleStorageEngine,
    ) -> None:
        self._config = config
        self._loader = loader
        self._last_part_info: UploadingPart = None

    def __call__(self, part: UploadingPart, index: int) -> None:
        # Next part, but not first
        if self._last_part_info is not None and self._last_part_info.part_info.part_metadata.name != part.part_info.part_metadata.name:
            if self._last_part_info.upload_id is not None:
                self._loader.complete_multipart_upload(
                    remote_path=self._last_part_info.part_info.remote_path,
                    upload_id=self._last_part_info.upload_id,
                )
            stage_communication.part_metadata_queue.put(self._last_part_info.part_info)
        self._last_part_info = part

    def on_done(self) -> None:
        # Pass last part and signal that all parts are uploaded
        if self._last_part_info.upload_id is not None:
            self._loader.complete_multipart_upload(
                remote_path=self._last_part_info.part_info.remote_path,
                upload_id=self._last_part_info.upload_id,
            )
        self._last_part_info.part_info.all_parts_done = True
        stage_communication.part_metadata_queue.put(self._last_part_info.part_info)
