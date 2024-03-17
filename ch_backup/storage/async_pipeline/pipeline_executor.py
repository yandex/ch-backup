"""
New pipelines executor module.
"""

from concurrent.futures import ProcessPoolExecutor
from functools import partial
from pathlib import Path
from typing import Any, AnyStr, Callable, List, Optional, Sequence

from ch_backup.storage.async_pipeline.base_pipeline.exec_pool import ExecPool
from ch_backup.storage.async_pipeline.pipelines import (
    delete_multiple_storage_pipeline,
    download_data_pipeline,
    download_file_pipeline,
    download_files_pipeline,
    upload_data_pipeline,
    upload_file_pipeline,
    upload_files_tarball_pipeline,
    upload_files_tarball_scan_pipeline,
)
from ch_backup.storage.async_pipeline.suppress_exceptions import suppress_exceptions
from ch_backup.util import current_func_name


class PipelineExecutor:
    """
    Executor of pipeline-based storage operations.

    Provide facility for starting pipeline storage operations both in multiprocessing pool
    or in-place(synchronously).
    """

    def __init__(self, config: dict) -> None:
        self._config = config
        self._exec_pool: Optional[ExecPool] = None

        worker_count = self._config["multiprocessing"].get("workers")
        if worker_count:
            self._exec_pool = ExecPool(ProcessPoolExecutor(max_workers=worker_count))

    def upload_data(
        self, data: AnyStr, remote_path: str, is_async: bool, encryption: bool
    ) -> None:
        """
        Upload given bytes or file-like object.
        """
        job_id = self._make_job_id(current_func_name(), "<data>", remote_path)

        pipeline = partial(
            upload_data_pipeline, self._config, data, remote_path, encryption
        )
        self._exec_pipeline(job_id, pipeline, is_async)

    def upload_file(
        self,
        local_path: str,
        remote_path: str,
        is_async: bool,
        encryption: bool,
        delete: bool,
        params: dict,
    ) -> None:
        """
        Upload file from local filesystem.
        """
        job_id = self._make_job_id(current_func_name(), local_path, remote_path)

        pipeline = partial(
            upload_file_pipeline,
            self._config,
            Path(local_path),
            remote_path,
            encryption,
            delete_after=delete,
        )

        if params.get("skip_deleted", False):
            pipeline = partial(suppress_exceptions, pipeline, [FileNotFoundError])

        self._exec_pipeline(job_id, pipeline, is_async)

    def upload_files_tarball_scan(
        self,
        dir_path: str,
        remote_path: str,
        is_async: bool,
        encryption: bool,
        delete: bool,
        compression: bool,
        callback: Optional[Callable] = None,
    ) -> None:
        """
        Archive to tarball and upload files from local filesystem.
        """
        job_id = self._make_job_id(current_func_name(), remote_path)

        pipeline = partial(
            upload_files_tarball_scan_pipeline,
            self._config,
            Path(dir_path),
            remote_path,
            encryption,
            delete_after=delete,
            compression=compression,
        )
        self._exec_pipeline(job_id, pipeline, is_async, callback)

    def upload_files_tarball(
        self,
        dir_path: str,
        remote_path: str,
        is_async: bool,
        encryption: bool,
        delete: bool,
        compression: bool,
        files: List[str],
        callback: Optional[Callable] = None,
    ) -> None:
        """
        Archive to tarball and upload files from local filesystem.
        """
        job_id = self._make_job_id(current_func_name(), remote_path)

        pipeline = partial(
            upload_files_tarball_pipeline,
            self._config,
            Path(dir_path),
            files,
            remote_path,
            encryption,
            delete_after=delete,
            compression=compression,
        )
        self._exec_pipeline(job_id, pipeline, is_async, callback)

    def download_data(
        self, remote_path: str, is_async: bool, encryption: bool
    ) -> bytes:
        """
        Download file from storage and return its content as a string.
        """
        job_id = self._make_job_id(current_func_name(), remote_path)
        pipeline = partial(
            download_data_pipeline, self._config, remote_path, encryption
        )

        return self._exec_pipeline(job_id, pipeline, is_async)

    def download_file(
        self, remote_path: str, local_path: str, is_async: bool, encryption: bool
    ) -> None:
        """
        Download file to local filesystem.
        """
        job_id = self._make_job_id(current_func_name(), remote_path, local_path)

        pipeline = partial(
            download_file_pipeline,
            self._config,
            remote_path,
            Path(local_path),
            encryption,
        )
        self._exec_pipeline(job_id, pipeline, is_async)

    def download_files(
        self,
        remote_path: str,
        local_path: str,
        is_async: bool,
        encryption: bool,
        compression: bool,
    ) -> None:
        """
        Download and unarchive tarball to files on local filesystem.
        """
        job_id = self._make_job_id(current_func_name(), remote_path, local_path)

        pipeline = partial(
            download_files_pipeline,
            self._config,
            remote_path,
            Path(local_path),
            encryption,
            compression,
        )
        self._exec_pipeline(job_id, pipeline, is_async)

    # pylint: disable=unused-argument
    def delete_files(
        self, remote_paths: Sequence[str], is_async: bool, encryption: bool
    ) -> None:
        """
        Delete files from storage.
        """
        job_id = self._make_job_id("delete_files", remote_paths)

        pipeline = partial(delete_multiple_storage_pipeline, self._config, remote_paths)
        self._exec_pipeline(job_id, pipeline, is_async)

    def wait(self, keep_going: bool = False) -> None:
        """
        Wait for completion of async operations.
        """
        if self._exec_pool:
            self._exec_pool.wait_all(keep_going)

    def _exec_pipeline(
        self,
        job_id: str,
        pipeline: Callable,
        is_async: bool,
        callback: Optional[Callable] = None,
    ) -> Any:
        """
        Run pipeline inplace or schedule for exec in process pool
        """

        if is_async and self._exec_pool:
            return self._exec_pool.submit(job_id, pipeline, callback)

        result = pipeline()
        if callback:
            callback()
        return result

    @staticmethod
    def _make_job_id(job_name: str, *args: Any) -> str:
        """
        Return job id composed of name and its arguments.
        """
        job_args = ", ".join(map(repr, args))
        return f"{job_name}({job_args})"
