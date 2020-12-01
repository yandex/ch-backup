"""
Filesystem pipeline stages module
"""

import io
import os
import tarfile
from tarfile import BLOCKSIZE, NUL  # type: ignore
from typing import IO, Iterator, List, Optional, Tuple

from .base import BufferedIterStage, InputStage, IterStage

STAGE_TYPE = 'filesystem'


class FileStream:
    """
    Data buffer that stores global offset, but removes data after read.
    """
    def __init__(self):
        self.buffer = io.BytesIO()
        self.offset = 0

    def write(self, s):
        """
        Write data into buffer.
        """
        self.buffer.write(s)
        self.offset += len(s)

    def close(self):
        """
        Close buffer, stop IO operations.
        """
        self.buffer.close()

    def tell(self):
        """
        Get current offset.
        """
        return self.offset

    def len(self):
        """
        Get uread data size.
        """
        return len(self.buffer.getvalue())

    def read(self, n=None):
        """
        Remove and return n bytes from buffer start.
        """
        data = self.buffer.getvalue()
        self.buffer.close()
        if n and len(data) < n:
            n = len(data)
        buf = data[:n]
        self.buffer = io.BytesIO(data[n:])
        return buf


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
        assert self._fobj
        self._fobj.write(data)

    def _post_process(self):
        if self._fobj:
            self._fobj.close()


class WriteFilesStage(BufferedIterStage):
    """
    Decompresses tarball into multiple files.
    """

    stype = STAGE_TYPE

    def __init__(self, conf):
        super().__init__(conf)
        self._dir: Optional[str] = None
        self._tarstream = FileStream()
        self._tarinfo: Optional[tarfile.TarInfo] = None
        self._fobj: Optional[IO] = None
        self._bytes_read: int = 0
        self._bytes_to_skip: int = 0

    def _pre_process(self, src_key, dst_key):
        self._dir = dst_key

    def _process(self, data):
        self._tarstream.write(data)
        while self._process_buffer():
            pass

    def _process_buffer(self):
        if self._bytes_to_skip > 0:
            buf = self._tarstream.read(self._bytes_to_skip)
            self._bytes_to_skip -= len(buf)
            if self._bytes_to_skip > 0:
                return False

        if not self._tarinfo:
            if not self._next_file():
                return False

        assert self._tarinfo
        assert self._fobj

        buf = self._tarstream.read(self._tarinfo.size - self._bytes_read)
        self._fobj.write(buf)
        self._bytes_read += len(buf)

        if self._bytes_read == self._tarinfo.size:
            self._bytes_to_skip = 0
            if self._tarinfo.size % BLOCKSIZE > 0:
                self._bytes_to_skip += BLOCKSIZE - self._tarinfo.size % BLOCKSIZE
            self._tarinfo = None
            return True

        return False

    def _next_file(self) -> bool:
        if self._fobj:
            self._fobj.close()
        if self._tarstream.len() < BLOCKSIZE:
            return False
        buf = self._tarstream.read(BLOCKSIZE)
        self._tarinfo = tarfile.TarInfo.frombuf(buf, tarfile.ENCODING, "surrogateescape")
        self._bytes_read = 0
        assert self._tarinfo
        assert self._dir
        self._fobj = open(os.path.join(self._dir, self._tarinfo.name), 'wb')
        return True

    def _post_process(self):
        if self._tarstream:
            self._tarstream.close()
        if self._fobj:
            self._fobj.close()


class ReadFileStage(InputStage):
    """
    Reads data from file unlimited
    """

    stype = STAGE_TYPE

    def __init__(self, conf: dict) -> None:
        self._chunk_size = conf['chunk_size']
        self._fobj: Optional[IO] = None

    def _pre_process(self, src_key: str) -> None:
        self._fobj = open(src_key, 'br')

    def _process(self):
        assert self._fobj
        return self._fobj.read(self._chunk_size)

    def _post_process(self) -> None:
        if self._fobj:
            self._fobj.close()


class ReadFilesStage(InputStage):
    """
    Reads data from multiple files unlimited
    """

    stype = STAGE_TYPE

    def __init__(self, conf: dict) -> None:
        self._chunk_size = conf['chunk_size']
        self._tarstream = FileStream()
        self._tarobj = tarfile.open(fileobj=self._tarstream, mode='w')  # type: ignore
        self._file_iter: Iterator[str] = iter([])
        self._fobj: Optional[IO] = None
        self._dir_path: Optional[str] = None
        self._tarinfo: Optional[tarfile.TarInfo] = None

    def _pre_process(self, src_key: Tuple[str, List[str]]) -> None:
        self._dir_path = src_key[0]
        self._file_iter = iter(src_key[1])
        self._open_next_file()

    def _process(self):
        try:
            buf = self._tarstream.read(self._chunk_size)
            if buf:
                return buf
            self._read_file_data()
            return self._process()
        except StopIteration:
            return None

    def _open_next_file(self):
        filename = next(self._file_iter)
        assert self._dir_path
        filepath = os.path.join(self._dir_path, filename)
        if self._fobj:
            self._fobj.close()
        self._fobj = open(filepath, 'br')
        stat = os.stat(filepath)
        self._tarinfo = tarfile.TarInfo(filename)
        assert self._tarinfo
        self._tarinfo.mtime, self._tarinfo.size = int(stat.st_mtime), stat.st_size
        self._tarobj.addfile(self._tarinfo)

    def _read_file_data(self):
        assert self._fobj
        filedata = self._fobj.read(self._chunk_size)
        if filedata:
            assert self._tarobj.fileobj
            self._tarobj.fileobj.write(filedata)
            if len(filedata) < self._chunk_size:
                assert self._tarinfo
                blocks, remainder = divmod(self._tarinfo.size, BLOCKSIZE)
                if remainder > 0:
                    self._tarobj.fileobj.write(NUL * (BLOCKSIZE - remainder))
                    blocks += 1
                self._tarobj.offset += blocks * BLOCKSIZE  # type: ignore
        else:
            self._open_next_file()
            self._read_file_data()

    def _post_process(self) -> None:
        if self._fobj:
            self._fobj.close()
        self._tarobj.close()
        self._tarstream.close()


class DeleteFileStage(IterStage):
    """
    Delete file from file system
    """

    stype = STAGE_TYPE

    def __init__(self, _config):
        self._local_path = None

    def _pre_process(self, src_key, dst_key):
        self._local_path = src_key

    def _process(self, _data):
        pass

    def _post_process(self):
        assert self._local_path
        os.remove(self._local_path)


class DeleteFilesStage(IterStage):
    """
    Delete file from file system
    """

    stype = STAGE_TYPE

    def __init__(self, _config):
        self._dir = None
        self._files = None

    def _pre_process(self, src_key: Tuple[str, List[str]], dst_key: str) -> None:
        self._dir, self._files = src_key

    def _process(self, _data):
        pass

    def _post_process(self):
        assert self._dir
        assert self._files
        for file in self._files:
            os.remove(os.path.join(self._dir, file))


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
