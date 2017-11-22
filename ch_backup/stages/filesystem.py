"""
Filesystem pipeline stages module
"""

import io

from ch_backup.stages.base import (InputFileStage, IterBufferedStage,
                                   OutputDataStage)

STAGE_TYPE = 'filesystem'


class ReadDataStage(object):  # pylint: disable=too-few-public-methods
    """
    Simple yield of consumed data
    """

    stype = STAGE_TYPE

    def __init__(self, conf):
        self._conf = conf

    def __call__(self, src_key=None, dst_key=None):
        yield src_key


class WriteFileStage(IterBufferedStage):
    # pylint: disable=too-few-public-methods
    """
    Write consumed from iter data to file
    """

    stype = STAGE_TYPE

    def __init__(self, conf):
        super().__init__(conf)
        self._fobj = None

    def _pre_process(self, src_key=None, dst_key=None):
        self._fobj = open(dst_key, 'bw', 0)

    def _process(self, data):
        self._fobj.write(data)

    def _post_process(self):
        self._fobj.close()


class ReadFileStage(InputFileStage):  # pylint: disable=too-few-public-methods
    """
    Reads data from file unlimited
    """

    stype = STAGE_TYPE

    def __init__(self, conf):
        super().__init__(conf)
        self._fobj = None

    def _pre_process(self, src_key=None, dst_key=None):
        self._fobj = open(src_key, 'br')

    def _process(self, data):
        return self._fobj.read(self._chunk_size)

    def _post_process(self):
        self._fobj.close()


class CollectDataStage(OutputDataStage):
    # pylint: disable=too-few-public-methods
    """
    Gathers all data from iterable and returns
    """

    stype = STAGE_TYPE

    def __init__(self, conf):
        super().__init__(conf)
        self._buffer = io.BytesIO()
        self._dst_key = None

    def _post_process(self):
        self._buffer.seek(0)
        resp = self._buffer.read()
        self._buffer.seek(0)
        self._buffer.truncate()
        return resp
