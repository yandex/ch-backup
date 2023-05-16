"""
Multipart uploading to storage stage.
"""
from concurrent.futures import ThreadPoolExecutor
from typing import Optional

from ch_backup.storage.async_pipeline.base_pipeline.exec_pool import ExecPool
from ch_backup.storage.async_pipeline.base_pipeline.handler import Handler
from ch_backup.storage.async_pipeline.stages.types import StageType
from ch_backup.storage.engine.base import PipeLineCompatibleStorageEngine


class MultipartStorageUploadingStage(Handler):
    """
    Uploads all data blocks to the storage single object as a set of parts.
    """

    stype = StageType.STORAGE

    def __init__(self, config: dict, loader: PipeLineCompatibleStorageEngine, remote_path: str) -> None:
        self._config = config
        self._loader = loader
        self._remote_path = remote_path
        self._upload_id: Optional[str] = None
        self._sent_size = 0
        self._thread_pool = ExecPool(ThreadPoolExecutor(max_workers=config.get('uploading_threads', 4)))

    def on_start(self) -> None:
        self._upload_id = self._loader.create_multipart_upload(remote_path=self._remote_path)

    def _compose_job_id(self, part_num: int) -> str:
        return f"Upload:{self._remote_path}(part:{part_num})"

    def __call__(self, data: bytes, index: int) -> None:
        assert self._upload_id is not None

        part_num = index + 1
        self._thread_pool.submit(self._compose_job_id(part_num), self._loader.upload_part, data, self._remote_path,
                                 self._upload_id, part_num)

    def on_done(self) -> None:
        assert self._upload_id is not None

        self._thread_pool.wait_all()
        self._loader.complete_multipart_upload(remote_path=self._remote_path, upload_id=self._upload_id)
