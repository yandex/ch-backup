"""
Module providing API for storage management (upload and download data, check
remote path on existence, etc.).
"""

from typing import BinaryIO, Callable, List, Optional, Sequence, Union

from ch_backup.storage.async_pipeline.pipeline_executor import PipelineExecutor
from ch_backup.storage.engine import get_storage_engine


class StorageLoader:
    """
    Class providing API for storing and retrieving data in uniform way.

    Internally, it uses storage engines to abstract underlying storage
    technologies and execution pool to provide capabilities for parallel
    execution.
    """

    def __init__(self, config):
        self._config = config
        self._engine = get_storage_engine(config["storage"])
        self._ploader = PipelineExecutor(config)

    # pylint: disable=too-many-positional-arguments
    def upload_data(
        self, data, remote_path, is_async=False, encryption=False, encoding="utf-8"
    ):
        """
        Upload data to storage.

        Data can be either byte-like object or string. The latter will be
        converted to bytes using provided encoding.
        """
        if isinstance(data, str):
            data = data.encode(encoding, errors="surrogateescape")

        return self._ploader.upload_data(
            data, remote_path, is_async=is_async, encryption=encryption
        )

    # pylint: disable=too-many-positional-arguments
    def upload_file(
        self,
        local_path,
        remote_path,
        is_async=False,
        encryption=False,
        delete=False,
        skip_deleted=False,
    ):
        """
        Upload file from local filesystem.

        If delete is True, the file will be deleted after upload.
        """
        self._ploader.upload_file(
            local_path,
            remote_path,
            is_async=is_async,
            encryption=encryption,
            delete=delete,
            params={"skip_deleted": skip_deleted},
        )
        return remote_path

    # pylint: disable=too-many-positional-arguments
    def upload_files_tarball_scan(
        self,
        dir_path: str,
        remote_path: str,
        tar_base_dir: Optional[str] = None,
        exclude_file_names: Optional[List[str]] = None,
        is_async: bool = False,
        encryption: bool = False,
        delete: bool = False,
        callback: Optional[Callable] = None,
        compression: bool = False,
    ) -> str:
        """
        Scan given directory for files a upload them as tarball.
        Do not load all file names in memory.

        If delete is True, the file will be deleted after upload.
        """
        self._ploader.upload_files_tarball_scan(
            dir_path,
            remote_path,
            tar_base_dir=tar_base_dir,
            exclude_file_names=exclude_file_names,
            is_async=is_async,
            encryption=encryption,
            delete=delete,
            callback=callback,
            compression=compression,
        )
        return remote_path

    # pylint: disable=too-many-positional-arguments
    def upload_files_tarball(
        self,
        dir_path: str,
        remote_path: str,
        files: List[str],
        is_async: bool = False,
        encryption: bool = False,
        delete: bool = False,
        callback: Optional[Callable] = None,
        compression: bool = False,
    ) -> str:
        """
        Upload multiple files as tarball.

        If delete is True, the file will be deleted after upload.
        """
        self._ploader.upload_files_tarball(
            dir_path,
            remote_path,
            files=files,
            is_async=is_async,
            encryption=encryption,
            delete=delete,
            callback=callback,
            compression=compression,
        )
        return remote_path

    def download_data(
        self, remote_path, is_async=False, encryption=False, encoding="utf-8"
    ):
        """
        Download file from storage and return its content.

        Unless encoding is None, the data will be decoded and returned as
        a string.
        """
        # pylint: disable=no-member
        data = self._ploader.download_data(
            remote_path, is_async=is_async, encryption=encryption
        )
        return data.decode(encoding) if encoding else data

    def download_file(
        self,
        remote_path: str,
        local_path: Union[str, BinaryIO],
        is_async: bool = False,
        encryption: bool = False,
        compression: bool = False,
    ) -> None:
        """
        Download file to local filesystem.
        """
        self._ploader.download_file(
            remote_path,
            local_path,
            is_async=is_async,
            encryption=encryption,
            compression=compression,
        )

    # pylint: disable=too-many-positional-arguments
    def download_files(
        self,
        remote_path: str,
        local_path: str,
        is_async: bool = False,
        encryption: bool = False,
        compression: bool = False,
        callback: Optional[Callable] = None,
    ) -> None:
        """
        Download file to local filesystem.
        """
        self._ploader.download_files(
            remote_path,
            local_path,
            is_async=is_async,
            encryption=encryption,
            compression=compression,
            callback=callback,
        )

    def delete_files(
        self,
        remote_paths: Sequence[str],
        is_async: bool = False,
        encryption: bool = False,
    ) -> None:
        """
        Delete multiple files from storage.
        """
        self._ploader.delete_files(
            remote_paths, is_async=is_async, encryption=encryption
        )

    def wait(self, keep_going: bool = False) -> None:
        """
        Wait for completion of async operations.
        """
        self._ploader.wait(keep_going)

    def list_dir(
        self, remote_path: str, recursive: bool = False, absolute: bool = False
    ) -> Sequence[str]:
        """
        Return list of entries in a remote path.
        """
        return self._engine.list_dir(
            remote_path, recursive=recursive, absolute=absolute
        )

    def path_exists(self, remote_path: str, is_dir: bool = False) -> bool:
        """
        Check whether a remote path exists or not.
        """
        return self._engine.path_exists(remote_path, is_dir)

    def get_file_size(self, remote_path: str) -> int:
        """
        Return actual size of the remote file in bytes.
        """
        return self._engine.get_object_size(remote_path)
