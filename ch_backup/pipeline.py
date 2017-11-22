"""
Pipeline module

Pipeline builder, loader and runner
"""

import logging
from concurrent.futures import wait as executor_wait
from concurrent.futures import ALL_COMPLETED, ProcessPoolExecutor
from functools import partial

from ch_backup.stages.encryption import DecryptStage, EncryptStage
from ch_backup.stages.filesystem import (CollectDataStage, ReadDataStage,
                                         ReadFileStage, WriteFileStage)
from ch_backup.stages.storage import (DownloadStorageStage,
                                      PathExistsStorageStage,
                                      StorageListDirStage, UploadStorageStage)
from ch_backup.storages.base import BaseLoader

DEFAULT_PIPELINE_RULES = {
    'upload_file': (
        ReadFileStage,
        EncryptStage,
        UploadStorageStage,
    ),
    'list_dir': (StorageListDirStage, ),
    'path_exists': (PathExistsStorageStage, ),
    'upload_data': (
        ReadDataStage,
        EncryptStage,
        UploadStorageStage,
    ),
    'download_data': (
        DownloadStorageStage,
        DecryptStage,
        CollectDataStage,
    ),
    'download_file': (
        DownloadStorageStage,
        DecryptStage,
        WriteFileStage,
    ),
}

STAGE_TYPES = (
    'filesystem',
    'storage',
    'encryption',
)


class Pipeline(object):  # pylint: disable=too-few-public-methods
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


class PipelineBuilder(object):  # pylint: disable=too-few-public-methods
    """
    Pipeline builder

    constructs pipeline based on rules
    """

    def __init__(self, config, rules=None):
        if rules is None:
            rules = DEFAULT_PIPELINE_RULES

        self._pipeline_rules = rules
        self._config = config
        # self._update_cfg(config)

    def __getattr__(self, item):
        try:
            pipeline_stages = self._pipeline_rules[item]
        except KeyError:
            raise AttributeError('Unknown stage: %s', item)

        pipeline = Pipeline()
        for stage in pipeline_stages:
            try:
                stage_conf = self._config[stage.stype]
            except KeyError:
                logging.debug(
                    'Skipping stage "%s": type "%s" is not configured', item,
                    stage.stype)
                continue

            pipeline.append(stage(stage_conf))
        return pipeline


class ExecPool(object):
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


def pipeline_wrapper(pipeline_cls, pipeline_config, pipeline_type, *args,
                     **kwargs):
    """
    Build pipeline and run it

    builds pipeline and executes
    """

    pipeline_builder = pipeline_cls(pipeline_config)
    pipeline = getattr(pipeline_builder, pipeline_type)
    return pipeline(*args, **kwargs)


class PipelineLoader(BaseLoader):
    # TODO: refactor loader classes structure
    # pylint: disable=arguments-differ
    """
    Pipeline-based storage loader
    """

    def __init__(self, config):
        self._config = config
        self._exec_pool = None

        multiprocessing_conf = self._config.get('multiprocessing')
        if multiprocessing_conf:
            self._exec_pool = ExecPool(multiprocessing_conf)

    def _execute_pipeline(self, async_exec, pipeline_type, *args, **kwargs):
        """
        Run pipeline inplace or schedule for exec in process pool
        """

        # we have to create pipelines inside running job, because
        # multiproc futures must be pickable, but boto3 is not pickable
        # https://github.com/boto/botocore/issues/636
        pipeline_runner = partial(pipeline_wrapper, PipelineBuilder,
                                  self._config, pipeline_type, *args, **kwargs)

        if async_exec and self._exec_pool:
            pipeline_id = (pipeline_type, args)
            return self._exec_pool.submit(pipeline_id, pipeline_runner)

        return pipeline_runner()

    def upload_file(self, local_path, remote_path, is_async=False):
        return self._execute_pipeline(is_async, 'upload_file', local_path,
                                      remote_path)

    def download_data(self, remote_path, is_async=False):
        return self._execute_pipeline(is_async, 'download_data', remote_path)

    def download_file(self, remote_path, local_path, is_async=False):
        return self._execute_pipeline(is_async, 'download_file', remote_path,
                                      local_path)

    def gather_async(self):
        """
        Wait for async jobs complete
        """

        if self._exec_pool:
            self._exec_pool.wait_all()

    def upload_data(self, data, remote_path, is_async=False):
        return self._execute_pipeline(is_async, 'upload_data', data,
                                      remote_path)

    def list_dir(self, remote_path, is_async=False):
        return self._execute_pipeline(is_async, 'list_dir', None, remote_path)

    def path_exists(self, remote_path, is_async=False):
        return self._execute_pipeline(is_async, 'path_exists', None,
                                      remote_path)
