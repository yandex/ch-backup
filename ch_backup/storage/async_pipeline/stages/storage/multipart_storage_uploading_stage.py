"""
Multipart uploading to storage stage.
"""
import time
from dataclasses import dataclass
from typing import Callable, Optional

from ch_backup.storage.async_pipeline.base_pipeline.handler import Handler
from ch_backup.storage.async_pipeline.stages.types import StageType
from ch_backup.storage.engine.base import PipeLineCompatibleStorageEngine
from ch_backup.util import RateLimiter


@dataclass
class UploadingPart:
    """
    Passed between uploading stages.
    """

    data: bytes
    upload_id: Optional[str] = None


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
        self._upload_id: Optional[str] = None
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


class TrafficLimitingStage(Handler):
    """
    Traffic rate limiting stage.

    A bottleneck for controlling the number of uploading chunks to prevent excessive network loading.
    Based on tocken bucket algorithm.
    """

    stype = StageType.STORAGE

    def __init__(
        self,
        traffic_limit_per_sec: int,
        update_interval: float = 0.01,
        get_time_func: Callable = time.time,
        sleep_func: Callable = time.sleep,
    ) -> None:
        self._update_interval = update_interval
        self._rate_limiter = RateLimiter(
            limit_per_sec=traffic_limit_per_sec, get_time_func=get_time_func
        )
        self._sleep_func = sleep_func

    def __call__(self, part: UploadingPart, index: int) -> UploadingPart:
        while not self._rate_limiter.grant(len(part.data)):
            self._sleep_func(self._update_interval)

        return part


class CompleteMultipartUploadStage(Handler):
    """
    Complete multipart uploading if needed.

    This stage must be started in a single worker.
    """

    stype = StageType.STORAGE

    def __init__(
        self, config: dict, loader: PipeLineCompatibleStorageEngine, remote_path: str
    ) -> None:
        self._config = config
        self._loader = loader
        self._remote_path = remote_path
        self._upload_id: Optional[str] = None

    def __call__(self, part: UploadingPart, index: int) -> None:
        if self._upload_id is None and part.upload_id:
            self._upload_id = part.upload_id

    def on_done(self) -> None:
        if self._upload_id is not None:
            self._loader.complete_multipart_upload(
                remote_path=self._remote_path, upload_id=self._upload_id
            )
