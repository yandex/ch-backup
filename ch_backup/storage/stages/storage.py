"""
Storage pipeline stages module
"""
import os
from abc import ABCMeta, abstractmethod
from math import ceil

from ..engine import get_storage_engine
from .base import BufferedIterStage, InputStage

STAGE_TYPE = 'storage'


class UploadStorageStage(BufferedIterStage, metaclass=ABCMeta):
    """
    Base class for uploading data using configured storage engine.
    """

    stype = STAGE_TYPE

    def __init__(self, conf: dict) -> None:
        super().__init__(conf)
        self._max_chunk_count = conf['max_chunk_count']
        self._loader = get_storage_engine(conf)
        self._remote_path = None
        self._upload_id = None
        self._processed = False

    def _pre_process(self, src_key, dst_key):
        self._remote_path = dst_key

        src_size = self._source_size(src_key)

        # use multi-part upload if source data size > chunk_size
        if src_size > self._chunk_size:
            self._upload_id = self._loader.create_multipart_upload(
                remote_path=dst_key)

        chunk_count = src_size / self._chunk_size
        if chunk_count > self._max_chunk_count:
            multiplier = ceil(chunk_count / self._max_chunk_count)
            self._buffer_size *= multiplier
            self._chunk_size *= multiplier

    def _process(self, data):
        assert not self._processed, 'already processed'

        if self._upload_id:
            self._loader.upload_part(
                data, remote_path=self._remote_path, upload_id=self._upload_id)
        else:
            self._loader.upload_data(data, self._remote_path)
            self._processed = True

    def _post_process(self) -> str:
        assert self._remote_path

        if not self._processed:
            if self._upload_id:
                self._loader.complete_multipart_upload(
                    remote_path=self._remote_path, upload_id=self._upload_id)
            else:
                self._loader.upload_data(b'', self._remote_path)
            self._processed = True

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

    def _source_size(self, source: str) -> int:
        return os.path.getsize(source)


class DownloadStorageStage(InputStage):
    """
    Downloads data from iterator by multipart download
    """

    stype = STAGE_TYPE

    def __init__(self, conf):
        self._chunk_size = conf['chunk_size']
        self._loader = get_storage_engine(conf)
        self._download_id = None

    def _pre_process(self, src_key):
        self._download_id = self._loader.create_multipart_download(
            remote_path=src_key)

    def _process(self):
        return self._loader.download_part(
            download_id=self._download_id, part_len=self._chunk_size)

    def _post_process(self):
        self._loader.complete_multipart_download(download_id=self._download_id)
        self._download_id = None


class DeleteStorageStage(InputStage):
    """
    Delete file from storage
    """

    stype = STAGE_TYPE

    def __init__(self, conf):
        self._loader = get_storage_engine(conf)
        self._remote_path = None

    def _pre_process(self, src_key):
        self._remote_path = src_key

    def _process(self):
        self._loader.delete_file(self._remote_path)

    def _post_process(self):
        return self._remote_path
