"""
Module providing API for storage management (upload and download data, check
remote path on existence, etc.).
"""
from typing import List, Sequence

from .engine import get_storage_engine
from .pipeline import PipelineLoader


class StorageLoader:
    """
    Class providing API for storing and retrieving data in uniform way.

    Internally, it uses storage engines to abstract underlying storage
    technologies and execution pool to provide capabilities for parallel
    execution.
    """
    def __init__(self, config):
        self._config = config
        self._engine = get_storage_engine(config['storage'])
        self._ploader = PipelineLoader(config)

    def upload_data(self, data, remote_path, is_async=False, encryption=False, encoding='utf-8'):
        """
        Upload data to storage.

        Data can be either byte-like object or string. The latter will be
        converted to bytes using provided encoding.
        """
        if isinstance(data, str):
            data = data.encode(encoding)

        return self._ploader.upload_data(data, remote_path, is_async=is_async, encryption=encryption)

    def upload_file(self, local_path, remote_path, is_async=False, encryption=False, delete=False):
        """
        Upload file from local filesystem.

        If delete is True, the file will be deleted after upload.
        """
        self._ploader.upload_file(local_path, remote_path, is_async=is_async, encryption=encryption, delete=delete)
        return remote_path

    def upload_files_tarball(self,
                             dir_path: str,
                             files: List[str],
                             remote_path: str,
                             is_async: bool = False,
                             encryption: bool = False,
                             delete: bool = False) -> str:
        """
        Upload multiple files as tarball.

        If delete is True, the file will be deleted after upload.
        """
        self._ploader.upload_files_tarball(dir_path,
                                           files,
                                           remote_path,
                                           is_async=is_async,
                                           encryption=encryption,
                                           delete=delete)
        return remote_path

    def download_data(self, remote_path, is_async=False, encryption=False, encoding='utf-8'):
        """
        Download file from storage and return its content.

        Unless encoding is None, the data will be decoded and returned as
        a string.
        """
        data = self._ploader.download_data(remote_path, is_async=is_async, encryption=encryption)
        return data.decode(encoding) if encoding else data

    def download_file(self,
                      remote_path: str,
                      local_path: str,
                      is_async: bool = False,
                      encryption: bool = False) -> None:
        """
        Download file to local filesystem.
        """
        self._ploader.download_file(remote_path, local_path, is_async=is_async, encryption=encryption)

    def download_files(self,
                       remote_path: str,
                       local_path: str,
                       is_async: bool = False,
                       encryption: bool = False) -> None:
        """
        Download file to local filesystem.
        """
        self._ploader.download_files(remote_path, local_path, is_async=is_async, encryption=encryption)

    def delete_file(self, remote_path, is_async=False, encryption=False):
        """
        Delete file from storage.
        """
        return self._ploader.delete_file(remote_path, is_async=is_async, encryption=encryption)

    def delete_files(self, remote_paths: Sequence[str], is_async: bool = False, encryption: bool = False) -> None:
        """
        Delete multiple files from storage.
        """
        return self._ploader.delete_files(remote_paths, is_async=is_async, encryption=encryption)

    def wait(self) -> None:
        """
        Wait for completion of async operations.
        """
        self._ploader.wait()

    def list_dir(self, remote_path: str, recursive: bool = False, absolute: bool = False) -> Sequence[str]:
        """
        Return list of entries in a remote path.
        """
        return self._engine.list_dir(remote_path, recursive=recursive, absolute=absolute)

    def path_exists(self, remote_path: str) -> bool:
        """
        Check whether a remote path exists or not.
        """
        return self._engine.path_exists(remote_path)

    def get_file_size(self, remote_path: str) -> int:
        """
        Return actual size of the remote file in bytes.
        """
        return self._engine.get_object_size(remote_path)
