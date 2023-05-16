"""
Pipeline module

Pipeline builder, loader and runner
"""

from concurrent.futures import ALL_COMPLETED, Future, ProcessPoolExecutor
from concurrent.futures import wait as executor_wait
from functools import partial
from typing import Any, Callable, Dict, List, Sequence, Tuple

from .. import logging
from .stages.encryption import DecryptStage, EncryptStage
from .stages.filesystem import (CollectDataStage, DeleteFilesStage, DeleteFileStage, ReadDataStage, ReadFilesStage,
                                ReadFileStage, WriteFilesStage, WriteFileStage)
from .stages.storage import (DeleteMultipleStorageStage, DeleteStorageStage, DownloadStorageStage,
                             UploadDataStorageStage, UploadFileStorageStage)


class Pipeline:
    """
    Pipeline class

    stores and runs pipe stages
    """
    def __init__(self) -> None:
        self._func_list: List[Tuple[Callable, Sequence, dict]] = []

    def append(self, func: Callable, *args: Any, **kwargs: Any) -> None:
        """
        Append stage cmd
        """
        self._func_list.append((func, args, kwargs))

    def __call__(self, src_key=None, dst_key=None):
        func_part = None
        for func, fargs, fkwargs in self._func_list:
            if func_part:
                func_part = partial(func, func_part, *fargs, **fkwargs)
            else:
                func_part = partial(func, *fargs, **fkwargs)

        # return value from generator
        # https://www.python.org/dev/peps/pep-0380/#id20
        assert func_part
        try:
            gen = func_part(src_key, dst_key)
            while True:
                next(gen)
        except StopIteration as e:
            return e.value


class ExecPool:
    """
    Multiprocessing runner

    runs
    uses concurrent.futures.ProcessPoolExecutor
    """
    def __init__(self, worker_count: int) -> None:
        self._futures: Dict[str, Future] = {}
        self._pool = ProcessPoolExecutor(max_workers=worker_count)

    def shutdown(self, graceful: bool = True) -> None:
        """
        Wait workers for complete jobs and shutdown workers
        """
        self._pool.shutdown(wait=graceful)

    def submit(self, future_id: str, func: Callable, *args: Any, **kwargs: Any) -> None:
        """
        Schedule job for execution
        """
        future = self._pool.submit(func, *args, **kwargs)
        future.add_done_callback(logging.debug('Future "%s" completed', future_id))
        self._futures[future_id] = future

    def wait_all(self) -> None:
        """
        Wait workers for complete jobs and
        """

        executor_wait(self._futures.values(), return_when=ALL_COMPLETED)

        for future_id, future in self._futures.items():
            try:
                future.result()
            except Exception:
                logging.error('Future "%s" generated an exception:', future_id, exc_info=True)
                raise
        self._futures = {}


def pipeline_wrapper(config: dict, stages: Sequence, *args: Any, **kwargs: Any) -> Any:
    """
    Build and execute pipeline.
    """
    pipeline = Pipeline()
    params = kwargs.pop('params', {})

    for stage in stages:
        stage_conf = config[stage.stype]
        pipeline.append(stage(stage_conf, params))

    return pipeline(*args, **kwargs)


class PipelineLoader:
    """
    Pipeline-based storage loader
    """
    def __init__(self, config: dict) -> None:
        self._config = config
        self._exec_pool = None

        worker_count = self._config['multiprocessing'].get('workers')
        if worker_count:
            self._exec_pool = ExecPool(worker_count)

    def _execute_pipeline(self, id_tuple, stages, *args, is_async, encryption, **kwargs):
        """
        Run pipeline inplace or schedule for exec in process pool
        """
        if not encryption:
            stages = [s for s in stages if s.stype != 'encryption']

        # we have to create pipelines inside running job, because
        # multiproc futures must be pickable, but boto3 is not pickable
        # https://github.com/boto/botocore/issues/636
        pipeline_runner = partial(pipeline_wrapper, self._config, stages, *args, **kwargs)

        if is_async and self._exec_pool:
            job_args = ', '.join(map(repr, id_tuple[1:]))
            job_id = f'{id_tuple[0]}({job_args})'

            return self._exec_pool.submit(job_id, pipeline_runner)

        return pipeline_runner()

    def upload_data(self, data, *args, **kwargs):
        """
        Upload given bytes or file-like object.
        """
        stages = [
            ReadDataStage,
            EncryptStage,
            UploadDataStorageStage,
        ]

        return self._execute_pipeline((self.upload_data.__name__, '<data>', *args), stages, data, *args, **kwargs)

    def upload_file(self, *args, delete, **kwargs):
        """
        Upload file from local filesystem.
        """
        stages = [
            ReadFileStage,
            EncryptStage,
            UploadFileStorageStage,
        ]
        if delete:
            stages.append(DeleteFileStage)

        self._execute_pipeline((self.upload_file.__name__, *args), stages, *args, **kwargs)

    def upload_files_tarball(self, dir_path, files, *args, delete, **kwargs):
        """
        Upload file from local filesystem.
        """
        stages = [
            ReadFilesStage,
            EncryptStage,
            UploadFileStorageStage,
        ]
        if delete:
            stages.append(DeleteFilesStage)

        self._execute_pipeline((self.upload_files_tarball.__name__, *args), stages, (dir_path, files), *args, **kwargs)

    def download_data(self, *args, **kwargs):
        """
        Download file from storage and return its content as a string.
        """
        stages = [
            DownloadStorageStage,
            DecryptStage,
            CollectDataStage,
        ]

        return self._execute_pipeline((self.download_data.__name__, *args), stages, *args, **kwargs)

    def download_file(self, *args, **kwargs):
        """
        Download file to local filesystem.
        """
        stages = [
            DownloadStorageStage,
            DecryptStage,
            WriteFileStage,
        ]

        self._execute_pipeline((self.download_file.__name__, *args), stages, *args, **kwargs)

    def download_files(self, *args, **kwargs):
        """
        Download file to local filesystem.
        """
        stages = [
            DownloadStorageStage,
            DecryptStage,
            WriteFilesStage,
        ]

        self._execute_pipeline((self.download_files.__name__, *args), stages, *args, **kwargs)

    def delete_file(self, *args, **kwargs):
        """
        Delete file from storage.
        """
        stages = [
            DeleteStorageStage,
        ]

        return self._execute_pipeline((self.delete_file.__name__, *args), stages, *args, **kwargs)

    def delete_files(self, *args, **kwargs):
        """
        Delete files from storage.
        """
        stages = [
            DeleteMultipleStorageStage,
        ]

        return self._execute_pipeline((self.delete_files.__name__, *args), stages, *args, **kwargs)

    def wait(self) -> None:
        """
        Wait for completion of async operations.
        """
        if self._exec_pool:
            self._exec_pool.wait_all()
