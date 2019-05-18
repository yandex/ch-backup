"""
Interfaces for storage engines.
"""

from abc import ABCMeta, abstractmethod
from typing import Sequence


class StorageEngine(metaclass=ABCMeta):
    """
    Base class for storage engines.
    """

    @abstractmethod
    def upload_file(self, local_path: str, remote_path: str) -> str:
        """
        Upload file from local filesystem.
        """

    @abstractmethod
    def upload_data(self, data: bytes, remote_path: str) -> str:
        """
        Upload given bytes or file-like object.
        """

    @abstractmethod
    def download_file(self, remote_path: str, local_path: str) -> None:
        """
        Download file from storage to local filesystem.
        """

    @abstractmethod
    def download_data(self, remote_path: str) -> bytes:
        """
        Download file from storage and return its content as a string.
        """

    def delete_file(self, remote_path: str) -> None:
        """
        Delete file from storage
        """

    @abstractmethod
    def list_dir(self, remote_path: str, recursive: bool = False, absolute: bool = False) -> Sequence[str]:
        """
        Get directory listing.
        """

    @abstractmethod
    def path_exists(self, remote_path: str) -> bool:
        """
        Check if remote path exists.
        """


class PipeLineCompatibleStorageEngine(StorageEngine):
    """
    Base class for pipeline-compatible storage engines.
    """

    @abstractmethod
    def create_multipart_upload(self, remote_path: str) -> str:
        """
        Start multipart upload.
        """

    @abstractmethod
    def upload_part(self, data, remote_path, upload_id):
        """
        Upload data part in multipart upload.
        """

    @abstractmethod
    def complete_multipart_upload(self, remote_path, upload_id):
        """
        Finish multipart upload.
        """

    @abstractmethod
    def create_multipart_download(self, remote_path):
        """
        Start multipart download.
        """

    @abstractmethod
    def download_part(self, download_id, part_len=None):
        """
        Download data part in multipart download.
        """

    @abstractmethod
    def complete_multipart_download(self, download_id):
        """
        Finish multipart download.
        """
