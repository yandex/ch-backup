"""
Base pipeline stages module.
"""

import io
from abc import ABCMeta, abstractmethod


class InputStage(metaclass=ABCMeta):
    """
    Base input stage.

    - gathers data from external source by required chunks.
    - produces collected data.
    """

    stype = None

    def __call__(self, src_key, dst_key):
        self._pre_process(src_key)

        while True:
            data = self._process()
            if not data:
                break
            yield data

        self._post_process()

    def _pre_process(self, src_key):
        pass

    @abstractmethod
    def _process(self):
        pass

    def _post_process(self):
        pass


class IterStage(metaclass=ABCMeta):
    """
    Base middleware stage.
    """

    stype = None

    def __call__(self, src_iter, src_key, dst_key):
        self._pre_process(src_key, dst_key)

        for data in src_iter(src_key, dst_key):
            yield self._process(data)

        return self._post_process()

    def _pre_process(self, src_key, dst_key):
        pass

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

    def __init__(self, conf):
        self._buffer_size = conf['buffer_size']
        self._chunk_size = conf['chunk_size']

    def __call__(self, src_iter, src_key, dst_key):
        """
        Handles incoming data.
        """
        self._pre_process(src_key, dst_key)

        consumed_size = 0
        total_size = 0
        unread_size = 0
        buffer = io.BytesIO()

        for data in src_iter(src_key, dst_key):
            # append new data to the end of buffer stream
            buffer.seek(total_size)
            buffer.write(data)
            total_size = buffer.tell()

            # return to unread position
            unread_size = total_size - consumed_size
            buffer.seek(consumed_size)

            while unread_size >= self._chunk_size:
                chunk = buffer.read(self._chunk_size)
                yield self._process(chunk)
                unread_size -= self._chunk_size

            # truncate stream buffer if limit exceeded
            if total_size > self._buffer_size:
                chunk = buffer.read()
                # TODO: mb recreate accumulator
                buffer.seek(0)
                buffer.truncate()
                buffer.write(chunk)
                total_size = buffer.tell()
                consumed_size = 0
                unread_size = total_size - consumed_size
            else:
                consumed_size = buffer.tell()

        # send unread buffer
        if unread_size:
            buffer.seek(consumed_size)
            chunk = buffer.read()
            yield self._process(chunk)

        return self._post_process()

    @abstractmethod
    def _process(self, data):
        pass
