"""
Interfaces for storage engines.
"""

from abc import ABCMeta, abstractmethod
from typing import Optional, Sequence


class StorageEngine(metaclass=ABCMeta):
    """
    Base class for storage engines.
    """

    @abstractmethod
    def upload_file(self, local_path: str, remote_path: str) -> str:
        """
        Upload file from local filesystem.
        """
        pass

    @abstractmethod
    def upload_data(self, data: bytes, remote_path: str) -> str:
        """
        Upload given bytes or file-like object.
        """
        pass

    @abstractmethod
    def download_file(self, remote_path: str, local_path: str) -> None:
        """
        Download file from storage to local filesystem.
        """
        pass

    @abstractmethod
    def download_data(self, remote_path: str) -> bytes:
        """
        Download file from storage and return its content as a string.
        """
        pass

    def delete_file(self, remote_path: str) -> None:
        """
        Delete file from storage
        """
        pass

    @abstractmethod
    def list_dir(
        self, remote_path: str, recursive: bool = False, absolute: bool = False
    ) -> Sequence[str]:
        """
        Get directory listing.
        """
        pass

    @abstractmethod
    def path_exists(self, remote_path: str) -> bool:
        """
        Check if remote path exists.
        """
        pass


class PipeLineCompatibleStorageEngine(StorageEngine):
    """
    Base class for pipeline-compatible storage engines.
    """

    @abstractmethod
    def create_multipart_upload(self, remote_path: str) -> str:
        """
        Start multipart upload.
        """
        pass

    @abstractmethod
    def upload_part(
        self,
        data: bytes,
        remote_path: str,
        upload_id: str,
        part_num: Optional[int] = None,
    ) -> None:
        """
        Upload data part in multipart upload.
        """
        pass

    @abstractmethod
    def complete_multipart_upload(self, remote_path, upload_id):
        """
        Finish multipart upload.
        """
        pass

    @abstractmethod
    def create_multipart_download(self, remote_path):
        """
        Start multipart download.
        """
        pass

    @abstractmethod
    def download_part(self, download_id, part_len=None):
        """
        Download data part in multipart download.
        """
        pass

    @abstractmethod
    def complete_multipart_download(self, download_id):
        """
        Finish multipart download.
        """
        pass

    @abstractmethod
    def delete_files(self, remote_paths: Sequence[str]) -> None:
        """
        Delete files from storage.
        """
        pass

    @abstractmethod
    def get_object_size(self, remote_path: str) -> int:
        """
        Return object size.
        """
        pass
