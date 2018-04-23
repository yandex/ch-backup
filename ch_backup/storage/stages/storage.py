"""
Storage pipeline stages module
"""
import os
from abc import ABCMeta, abstractmethod

from ..engine import get_storage_engine
from .base import InputFileStage, IterBufferedStage

STAGE_TYPE = 'storage'


class UploadStorageStage(IterBufferedStage, metaclass=ABCMeta):
    """
    Base class for uploading data using configured storage engine.
    """

    stype = STAGE_TYPE

    def __init__(self, conf):
        super().__init__(conf)
        self._loader = get_storage_engine(conf)
        self._remote_path = None
        self._upload_id = None
        self._processed = False

    def _pre_process(self, src_key=None, dst_key=None):
        self._remote_path = dst_key

        # use multi-part upload if source data size > chunk_size
        if self._source_size(src_key) > self._chunk_size:
            self._upload_id = self._loader.create_multipart_upload(
                remote_path=dst_key)

    def _process(self, data):
        assert not self._processed, 'already processed'

        if self._upload_id:
            self._loader.upload_part(
                data, remote_path=self._remote_path, upload_id=self._upload_id)
        else:
            self._loader.upload_data(data, self._remote_path)
            self._processed = True

    def _post_process(self):
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

    def _source_size(self, source):
        return len(source)


class UploadFileStorageStage(UploadStorageStage):
    """
    UploadStorageStage for uploading local files.
    """

    def _source_size(self, source):
        return os.path.getsize(source)


class DownloadStorageStage(InputFileStage):
    """
    Downloads data from iterator by multipart download
    """

    stype = STAGE_TYPE

    def __init__(self, conf):
        super().__init__(conf)
        self._loader = get_storage_engine(conf)
        self._download_id = None

    def _pre_process(self, src_key=None, dst_key=None):
        self._download_id = self._loader.create_multipart_download(
            remote_path=src_key)

    def _process(self, data):
        return self._loader.download_part(
            download_id=self._download_id, part_len=self._chunk_size)

    def _post_process(self):
        self._loader.complete_multipart_download(download_id=self._download_id)
        self._download_id = None


class DeleteStorageStage:  # pylint: disable=too-few-public-methods
    """
    Delete file from storage
    """

    stype = STAGE_TYPE

    def __init__(self, conf):
        self._loader = get_storage_engine(conf)

    def __call__(self, src_key=None, dst_key=None):
        yield
        return self._loader.delete_file(remote_path=src_key)
