"""
Storage pipeline stages module
"""
import os
from abc import ABCMeta, abstractmethod
from math import ceil
from typing import Any, List, Optional, Tuple, Union

from ch_backup.util import retry

from ..engine import get_storage_engine
from .base import BufferedIterStage, InputStage

STAGE_TYPE = "storage"


class UploadStorageStage(BufferedIterStage, metaclass=ABCMeta):
    """
    Base class for uploading data using configured storage engine.
    """

    stype = STAGE_TYPE

    def __init__(self, conf: dict, params: dict) -> None:
        super().__init__(conf, params)
        self._max_chunk_count = conf["max_chunk_count"]
        self._loader = get_storage_engine(conf)
        self._remote_path: Optional[str] = None
        self._upload_id: Optional[str] = None
        self._skip_deleted = params.get("skip_deleted", False)

    def _pre_process(self, src_key: Any, dst_key: Any) -> bool:
        try:
            self._remote_path = dst_key

            src_size = self._source_size(src_key)

            # use multi-part upload if source data size > chunk_size
            if src_size > self._chunk_size:
                self._upload_id = self._loader.create_multipart_upload(
                    remote_path=dst_key
                )

            chunk_count = src_size / self._chunk_size
            if chunk_count > self._max_chunk_count:
                multiplier = ceil(chunk_count / self._max_chunk_count)
                self._buffer_size *= multiplier
                self._chunk_size *= multiplier
            return True
        except FileNotFoundError:
            if self._skip_deleted:
                return False
            raise

    def _process(self, data):
        assert self._remote_path is not None
        if self._upload_id:
            self._loader.upload_part(
                data, remote_path=self._remote_path, upload_id=self._upload_id
            )

    def _post_process(self) -> str:
        assert self._remote_path

        if self._upload_id:
            self._loader.complete_multipart_upload(
                remote_path=self._remote_path, upload_id=self._upload_id
            )
        else:
            self._loader.upload_data(self._buffer.getvalue(), self._remote_path)

        return self._remote_path

    @abstractmethod
    def _source_size(self, source):
        pass


class UploadDataStorageStage(UploadStorageStage):
    """
    UploadStorageStage for uploading data objects.
    """

    def _source_size(self, source: str) -> int:
        return len(source)


class UploadFileStorageStage(UploadStorageStage):
    """
    UploadStorageStage for uploading local files.
    """

    def _source_size(self, source: Union[str, Tuple[str, List[str]]]) -> int:
        if isinstance(source, str):
            return os.path.getsize(source)
        return sum(os.path.getsize(os.path.join(source[0], file)) for file in source[1])


class DownloadStorageStage(InputStage):
    """
    Downloads data from iterator by multipart download
    """

    stype = STAGE_TYPE

    def __init__(self, conf, _params):
        self._chunk_size = conf["chunk_size"]
        self._loader = get_storage_engine(conf)
        self._download_id = None

    def _pre_process(self, src_key: str) -> bool:
        self._download_id = self._loader.create_multipart_download(remote_path=src_key)
        return True

    def _process(self):
        return self._loader.download_part(
            download_id=self._download_id, part_len=self._chunk_size
        )

    def _post_process(self):
        self._loader.complete_multipart_download(download_id=self._download_id)
        self._download_id = None


class DeleteStorageStage(InputStage):
    """
    Delete file from storage
    """

    stype = STAGE_TYPE

    def __init__(self, conf, _params):
        self._loader = get_storage_engine(conf)
        self._remote_path: Optional[str] = None

    def _pre_process(self, src_key: str) -> bool:
        self._remote_path = src_key
        return True

    def _process(self):
        assert self._remote_path is not None
        self._loader.delete_file(self._remote_path)

    def _post_process(self):
        return self._remote_path


class DeleteMultipleStorageStage(InputStage):
    """
    Delete multiple files from storage
    """

    stype = STAGE_TYPE

    def __init__(self, conf, _params):
        self._loader = get_storage_engine(conf)
        self._bulk_delete_chunk_size = conf["bulk_delete_chunk_size"]
        self._files_iter = None

    def _pre_process(self, src_key: List) -> bool:
        self._files_iter = iter(
            [
                src_key[i : i + self._bulk_delete_chunk_size]
                for i in range(0, len(src_key), self._bulk_delete_chunk_size)
            ]
        )
        return True

    def _process(self):
        assert self._files_iter

        @retry()
        def _delete_files(elem):
            self._loader.delete_files(elem)
            return elem

        try:
            elem = next(self._files_iter)
        except StopIteration:
            # Iterator is empty, all needed files are deleted
            return None
        else:
            return _delete_files(elem)

    def _post_process(self):
        pass
