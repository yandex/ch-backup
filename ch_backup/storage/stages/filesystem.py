"""
Filesystem pipeline stages module
"""

import io
from typing import IO, Optional

from .base import BufferedIterStage, InputStage, IterStage

STAGE_TYPE = 'filesystem'


class ReadDataStage(InputStage):
    """
    Simple yield of consumed data
    """

    stype = STAGE_TYPE

    def __init__(self, *_):
        self._src_key = None
        self._read = False

    def _pre_process(self, src_key):
        self._src_key = src_key

    def _process(self):
        if not self._read:
            self._read = True
            return self._src_key


class WriteFileStage(BufferedIterStage):
    """
    Write consumed from iter data to file
    """

    stype = STAGE_TYPE

    def __init__(self, conf):
        super().__init__(conf)
        self._fobj = None

    def _pre_process(self, src_key, dst_key):
        self._fobj = open(dst_key, 'bw', 0)

    def _process(self, data):
        self._fobj.write(data)

    def _post_process(self):
        self._fobj.close()


class ReadFileStage(InputStage):
    """
    Reads data from file unlimited
    """

    stype = STAGE_TYPE

    def __init__(self, conf: dict) -> None:
        self._chunk_size = conf['chunk_size']
        self._fobj = None  # type: Optional[IO]

    def _pre_process(self, src_key) -> None:
        self._fobj = open(src_key, 'br')

    def _process(self):
        return self._fobj.read(self._chunk_size)

    def _post_process(self) -> None:
        if self._fobj:
            self._fobj.close()


class CollectDataStage(IterStage):
    """
    Gathers all data from iterable and returns
    """

    stype = STAGE_TYPE

    def __init__(self, *_):
        self._buffer = io.BytesIO()

    def _process(self, data):
        return self._buffer.write(data)

    def _post_process(self):
        self._buffer.seek(0)
        resp = self._buffer.read()
        self._buffer.seek(0)
        self._buffer.truncate()
        return resp
