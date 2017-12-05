"""
Module providing API for storage management (upload and download data, check
remote path on existence, etc.).
"""
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

    def upload_data(self, data, remote_path, is_async=False):
        """
        Upload given bytes or file-like object.
        """
        return self._ploader.upload_data(data, remote_path, is_async)

    def upload_file(self, local_path, remote_path, is_async=False):
        """
        Upload file from local filesystem.
        """
        return self._ploader.upload_file(local_path, remote_path, is_async)

    def download_data(self, remote_path, is_async=False):
        """
        Download file from storage and return its content as a string.
        """
        return self._ploader.download_data(remote_path, is_async)

    def download_file(self, remote_path, local_path, is_async=False):
        """
        Download file to local filesystem.
        """
        return self._ploader.download_file(remote_path, local_path, is_async)

    def await(self):
        """
        Wait for completion of async operations.
        """
        return self._ploader.await()

    def list_dir(self, remote_path):
        """
        Return list of entries in a remote path.
        """
        return self._engine.list_dir(remote_path)

    def path_exists(self, remote_path):
        """
        Check whether a remote path exists or not.
        """
        return self._engine.path_exists(remote_path)
