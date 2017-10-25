"""
Abstract class for storages
"""

from abc import ABCMeta, abstractmethod


class Loader(metaclass=ABCMeta):
    """
    Base class for storage loaders
    """

    @abstractmethod
    def upload_file(self, local_path, remote_path, path_prefix):
        """
        Upload file from filesystem
        """
        pass

    @abstractmethod
    def upload_data(self, data, remote_path, path_prefix):
        """
        Upload given bytes or file-like object
        """
        pass

    @abstractmethod
    def upload_dir(self, dir_path, path_prefix):
        """
        Upload directory from filesystem
        """
        pass

    @abstractmethod
    def download_file(self, remote_path, local_path, path_prefix):
        """
        Download file from storage to filesystem
        """
        pass

    @abstractmethod
    def download_data(self, remote_path, path_prefix):
        """
        Download file from storage to str object
        """
        pass

    @abstractmethod
    def list_dir(self, remote_path, abs_path):
        """
        Get directory listing
        """
        pass

    @abstractmethod
    def path_exists(self, remote_path):
        """
        Check if path exists
        """
        pass

    @abstractmethod
    def download_dir(self, remote_path, local_path):
        """
        Download files from directory to filesystem
        """
        pass

    @abstractmethod
    def get_abs_path(self, rel_path):
        """
        Get absolute path using prefix
        """
        pass
