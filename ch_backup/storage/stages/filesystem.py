"""
Filesystem pipeline stages module
"""

import io
import os
import tarfile
from enum import Enum
from tarfile import BLOCKSIZE, GNUTYPE_LONGNAME, NUL  # type: ignore
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


class State(Enum):
    """
    WriteFilesStage state representation.
    """
    READ_HEADER = 1
    # If filename longer than 100 chars special entry created
    # Header with special type L and name @LongLink
    # Several data blocks containing actual name
    READ_LONG_NAME = 2
    READ_DATA = 3
    SKIP_BYTES = 4


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
        self._state: State = State.READ_HEADER
        self._bytes_to_process: int = 0
        self._name_from_buffer: Optional[bytes] = None

    def _pre_process(self, src_key, dst_key):
        self._dir = dst_key

    def _process(self, data):
        self._tarstream.write(data)
        actions = {
            State.READ_HEADER: self._read_header,
            State.READ_LONG_NAME: self._process_long_name,
            State.READ_DATA: self._process_data,
            State.SKIP_BYTES: self._skip_bytes,
        }

        while actions[self._state]():
            pass

    def _read_header(self) -> bool:
        if self._fobj:
            self._fobj.close()
        if self._tarstream.len() < BLOCKSIZE:
            return False
        buf = self._tarstream.read(BLOCKSIZE)
        self._tarinfo = tarfile.TarInfo.frombuf(buf, tarfile.ENCODING, "surrogateescape")
        assert self._tarinfo
        assert self._dir
        self._bytes_to_process = self._tarinfo.size
        if self._tarinfo.type == GNUTYPE_LONGNAME:
            self._state = State.READ_LONG_NAME
            self._name_from_buffer = b''
        else:
            self._state = State.READ_DATA
            if self._name_from_buffer:
                self._tarinfo.name = self._name_from_buffer[:-1].decode()  # ignore string null terminator
                self._name_from_buffer = None
            self._fobj = open(os.path.join(self._dir, self._tarinfo.name), 'wb')
        return True

    def _process_long_name(self) -> bool:
        assert self._tarinfo
        buf = self._tarstream.read(self._bytes_to_process)
        self._name_from_buffer += buf
        self._bytes_to_process -= len(buf)

        if self._bytes_to_process > 0:
            return False

        self._state = State.SKIP_BYTES
        if self._tarinfo.size % BLOCKSIZE > 0:
            self._bytes_to_process = BLOCKSIZE - (self._tarinfo.size % BLOCKSIZE)

        return True

    def _process_data(self) -> bool:
        assert self._tarinfo
        assert self._fobj

        buf = self._tarstream.read(self._bytes_to_process)
        self._fobj.write(buf)
        self._bytes_to_process -= len(buf)

        if self._bytes_to_process > 0:
            return False

        self._state = State.SKIP_BYTES
        if self._tarinfo.size % BLOCKSIZE > 0:
            self._bytes_to_process = BLOCKSIZE - (self._tarinfo.size % BLOCKSIZE)
        return True

    def _skip_bytes(self) -> bool:
        buf = self._tarstream.read(self._bytes_to_process)
        assert buf.count(NUL) == len(buf), buf
        self._bytes_to_process -= len(buf)
        if self._bytes_to_process > 0:
            return False

        self._state = State.READ_HEADER
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
            if not self._read_file_data():
                self._open_next_file()
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
        self._tarstream.write(self._tarinfo.tobuf())

    def _read_file_data(self) -> bool:
        assert self._fobj
        filedata = self._fobj.read(self._chunk_size)
        if filedata:
            assert self._tarstream
            self._tarstream.write(filedata)
            if len(filedata) < self._chunk_size:
                assert self._tarinfo
                remainder = self._tarinfo.size % BLOCKSIZE
                if remainder > 0:
                    self._tarstream.write(NUL * (BLOCKSIZE - remainder))
        return filedata

    def _post_process(self) -> None:
        if self._fobj:
            self._fobj.close()
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
