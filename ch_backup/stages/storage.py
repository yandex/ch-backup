"""
Storage pipeline stages module
"""

from ch_backup.stages.base import InputFileStage, IterBufferedStage
from ch_backup.storages import get_storage_loader

STAGE_TYPE = 'storage'


class UploadStorageStage(IterBufferedStage):
    # pylint: disable=too-few-public-methods
    """
    Uploads data from iterator by multipart upload
    """

    stype = STAGE_TYPE

    def __init__(self, conf):
        super().__init__(conf)
        self._loader = get_storage_loader(conf['type'], conf)
        self._dst_key = None
        self._upload_id = None

    def _pre_process(self, src_key=None, dst_key=None):
        self._dst_key = dst_key
        self._upload_id = self._loader.create_multipart_upload(
            remote_path=dst_key)

    def _process(self, data):
        self._loader.upload_part(
            data, remote_path=self._dst_key, upload_id=self._upload_id)

    def _post_process(self):
        self._loader.complete_multipart_upload(
            remote_path=self._dst_key, upload_id=self._upload_id)
        return self._dst_key


class DownloadStorageStage(InputFileStage):
    # pylint: disable=too-few-public-methods
    """
    Downloads data from iterator by multipart download
    """

    stype = STAGE_TYPE

    def __init__(self, conf):
        super().__init__(conf)
        self._loader = get_storage_loader(conf['type'], conf)
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


class StorageListDirStage(object):  # pylint: disable=too-few-public-methods
    """
    List files in storage dir
    """

    stype = STAGE_TYPE

    def __init__(self, conf):
        self._loader = get_storage_loader(conf['type'], conf)
        self._dst_key = None

    def __call__(self, src_key=None, dst_key=None):
        yield
        return self._loader.list_dir(dst_key)


class PathExistsStorageStage(object):  # pylint: disable=too-few-public-methods
    """
    Ensure storage path exists
    """

    stype = STAGE_TYPE

    def __init__(self, conf):
        self._loader = get_storage_loader(conf['type'], conf)
        self._dst_key = None

    def __call__(self, src_key=None, dst_key=None):
        yield
        return self._loader.path_exists(dst_key)
