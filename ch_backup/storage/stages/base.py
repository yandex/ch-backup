"""
Base pipeline stages module.
"""

import io
from abc import ABCMeta, abstractmethod
from typing import Any, Callable, Generator, Optional


class InputStage(metaclass=ABCMeta):
    """
    Base input stage.

    - gathers data from external source by required chunks.
    - produces collected data.
    """

    stype: Optional[str] = None

    def __call__(self, src_key, dst_key):
        self._pre_process(src_key)

        while True:
            data = self._process()
            if not data:
                break
            yield data

        self._post_process()

    # pylint: disable=unused-argument
    def _pre_process(self, src_key: Any) -> bool:
        return True

    @abstractmethod
    def _process(self):
        pass

    def _post_process(self):
        pass


class IterStage(metaclass=ABCMeta):
    """
    Base middleware stage.
    """

    stype: Optional[str] = None

    def __call__(self, src_iter: Callable, src_key: Any, dst_key: Any) -> Generator:
        if not self._pre_process(src_key, dst_key):
            return

        for data in src_iter(src_key, dst_key):
            yield self._process(data)

        return self._post_process()

    # pylint: disable=unused-argument
    def _pre_process(self, src_key: Any, dst_key: Any) -> bool:
        return True

    @abstractmethod
    def _process(self, data):
        pass

    def _post_process(self):
        pass


class BufferedIterStage(IterStage, metaclass=ABCMeta):
    """
    Base middleware buffered stage.

    - gathers data from iterator.
    - collects chunk of desired size in memory buffer.
    - produces processed data.
    """

    def __init__(self, conf: dict, _params: dict) -> None:
        self._buffer_size = conf["buffer_size"]
        self._chunk_size = conf["chunk_size"]
        self._buffer = io.BytesIO()

    def __call__(self, src_iter: Callable, src_key: Any, dst_key: Any) -> Generator:
        """
        Handles incoming data.
        """
        if not self._pre_process(src_key, dst_key):
            return

        consumed_size = 0
        total_size = 0
        unread_size = 0

        for data in src_iter(src_key, dst_key):
            # append new data to the end of buffer stream
            self._buffer.seek(total_size)
            self._buffer.write(data)
            total_size = self._buffer.tell()

            # return to unread position
            unread_size = total_size - consumed_size
            self._buffer.seek(consumed_size)

            while unread_size >= self._chunk_size:
                chunk = self._buffer.read(self._chunk_size)
                yield self._process(chunk)
                unread_size -= self._chunk_size

            # truncate stream buffer if limit exceeded
            if total_size > self._buffer_size:
                chunk = self._buffer.read()
                # TODO: mb recreate accumulator
                self._buffer.seek(0)
                self._buffer.truncate()
                self._buffer.write(chunk)
                total_size = self._buffer.tell()
                consumed_size = 0
                unread_size = total_size - consumed_size
            else:
                consumed_size = self._buffer.tell()

        # send unread buffer
        if unread_size:
            self._buffer.seek(consumed_size)
            chunk = self._buffer.read()
            yield self._process(chunk)

        return self._post_process()

    @abstractmethod
    def _process(self, data):
        pass
