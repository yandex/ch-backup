"""
Pipeline module

Pipeline builder, loader and runner
"""

import logging
from concurrent.futures import wait as executor_wait
from concurrent.futures import ALL_COMPLETED, ProcessPoolExecutor
from functools import partial

from .stages.encryption import DecryptStage, EncryptStage
from .stages.filesystem import (CollectDataStage, ReadDataStage, ReadFileStage,
                                WriteFileStage)
from .stages.storage import DownloadStorageStage, UploadStorageStage


class Pipeline:
    """
    Pipeline class

    stores and runs pipe stages
    """

    def __init__(self):
        self._func_list = []

    def append(self, func, *args, **kwargs):
        """
        Append stage cmd
        """

        return self._func_list.append((func, args, kwargs))

    def __call__(self, src_key=None, dst_key=None):
        func_part = None
        for func, fargs, fkwargs in self._func_list:
            if func_part:
                func_part = partial(func, func_part, *fargs, **fkwargs)
            else:
                func_part = partial(func, *fargs, **fkwargs)

        # return value from generator
        # https://www.python.org/dev/peps/pep-0380/#id20
        try:
            gen = func_part(src_key, dst_key)
            while True:
                next(gen)
        except StopIteration as ex:
            return ex.value


class ExecPool:
    """
    Multiprocessing runner

    runs
    uses concurrent.futures.ProcessPoolExecutor
    """

    def __init__(self, config):
        self._futures = {}
        self._pool = ProcessPoolExecutor(max_workers=config['workers'])

    def shutdown(self, graceful=True):
        """
        Wait workers for complete jobs and shutdown workers
        """

        self._pool.shutdown(wait=graceful)

    def submit(self, future_id, func, *args, **kwargs):
        """
        Schedule job for execution
        """

        self._futures[future_id] = \
            self._pool.submit(func, *args, **kwargs)

    def wait_all(self):
        """
        Wait workers for complete jobs and
        """

        executor_wait(self._futures.values(), return_when=ALL_COMPLETED)

        for future_id, future in self._futures.items():
            try:
                future_result = future.result()
            except Exception as exc:
                logging.error(
                    'Future "%s" generated an exception: %s',
                    future_id,
                    exc,
                    exc_info=True)
                raise
            else:
                logging.debug('Future "%s" returned: %s', future_id,
                              future_result)
        self._futures = {}


def pipeline_wrapper(config, stages, *args, **kwargs):
    """
    Build and execute pipeline.
    """
    pipeline = Pipeline()

    for stage in stages:
        stage_conf = config[stage.stype]
        pipeline.append(stage(stage_conf))

    return pipeline(*args, **kwargs)


class PipelineLoader:
    """
    Pipeline-based storage loader
    """

    # pylint: disable=missing-kwoa

    def __init__(self, config):
        self._config = config
        self._exec_pool = None

        multiprocessing_conf = self._config.get('multiprocessing')
        if multiprocessing_conf:
            self._exec_pool = ExecPool(multiprocessing_conf)

    def _execute_pipeline(self, id_tuple, stages, *args, is_async, encryption):
        """
        Run pipeline inplace or schedule for exec in process pool
        """
        if not encryption:
            stages = [s for s in stages if s.stype != 'encryption']

        # we have to create pipelines inside running job, because
        # multiproc futures must be pickable, but boto3 is not pickable
        # https://github.com/boto/botocore/issues/636
        pipeline_runner = partial(pipeline_wrapper, self._config, stages,
                                  *args)

        if is_async and self._exec_pool:
            job_id = "{id}({args})".format(
                id=id_tuple[0], args=', '.join(map(repr, id_tuple[1:])))

            return self._exec_pool.submit(job_id, pipeline_runner)

        return pipeline_runner()

    def upload_data(self, data, *args, **kwargs):
        """
        Upload given bytes or file-like object.
        """
        return self._execute_pipeline(
            (self.upload_data.__name__, '<data>', *args),
            (ReadDataStage, EncryptStage, UploadStorageStage), data, *args,
            **kwargs)

    def upload_file(self, *args, **kwargs):
        """
        Upload file from local filesystem.
        """
        return self._execute_pipeline(
            (self.upload_file.__name__, *args),
            (ReadFileStage, EncryptStage, UploadStorageStage), *args, **kwargs)

    def download_data(self, *args, **kwargs):
        """
        Download file from storage and return its content as a string.
        """
        return self._execute_pipeline(
            (self.download_data.__name__, *args),
            (DownloadStorageStage, DecryptStage, CollectDataStage), *args,
            **kwargs)

    def download_file(self, *args, **kwargs):
        """
        Download file to local filesystem.
        """
        return self._execute_pipeline(
            (self.download_file.__name__, *args),
            (DownloadStorageStage, DecryptStage, WriteFileStage), *args,
            **kwargs)

    def wait(self):
        """
        Wait for completion of async operations.
        """
        if self._exec_pool:
            self._exec_pool.wait_all()