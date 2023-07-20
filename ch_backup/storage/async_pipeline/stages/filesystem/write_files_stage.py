"""
Writing files from TAR stream stage.
"""
from enum import Enum
from pathlib import Path
from tarfile import BLOCKSIZE, ENCODING, GNUTYPE_LONGNAME, NUL, TarInfo
from typing import IO, Optional

from ch_backup.storage.async_pipeline.base_pipeline.bytes_fifo import BytesFIFO
from ch_backup.storage.async_pipeline.base_pipeline.handler import Handler
from ch_backup.storage.async_pipeline.stages.types import StageType


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


class WriteFilesStage(Handler):
    """
    Unarchive and save files to the local filesystem from TAR stream.
    """

    stype = StageType.FILESYSTEM

    def __init__(self, config: dict, dir_path: Path, buffer_size: int) -> None:
        self._config = config

        if buffer_size < BLOCKSIZE:
            raise ValueError(
                f"Size of TAR stream buffer cannot be less than TAR BLOCKSIZE: {BLOCKSIZE}"
            )

        self._dir: Path = dir_path
        self._tarstream = BytesFIFO(buffer_size)
        self._state: State = State.READ_HEADER

        self._bytes_to_process: int = 0
        self._tarinfo: Optional[TarInfo] = None
        self._fobj: Optional[IO] = None
        self._name_from_buffer: Optional[bytes] = None

    def __call__(self, data: bytes, index: int) -> None:
        written = self._tarstream.write(data)
        if written != len(data):
            raise RuntimeError(
                f"TAR stream buffer size {len(self._tarstream)} is full. And cannot accept chunk of size {len(data)}. "
                f"Maybe the buffer size setting is too small"
            )

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

        if len(self._tarstream) < BLOCKSIZE:
            return False

        buf = self._tarstream.read(BLOCKSIZE)
        self._tarinfo = TarInfo.frombuf(buf, ENCODING, "surrogateescape")

        self._bytes_to_process = self._tarinfo.size

        if self._tarinfo.type == GNUTYPE_LONGNAME:
            self._state = State.READ_LONG_NAME
            self._name_from_buffer = b""
        else:
            self._state = State.READ_DATA
            if self._name_from_buffer:
                self._tarinfo.name = self._name_from_buffer[
                    :-1
                ].decode()  # ignore string null terminator
                self._name_from_buffer = None

            filepath = self._dir / self._tarinfo.name
            filepath.parent.mkdir(parents=True, exist_ok=True)
            self._fobj = filepath.open("wb")

        return True

    def _process_long_name(self) -> bool:
        assert self._tarinfo
        assert self._name_from_buffer is not None

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

    def on_done(self) -> None:
        if self._fobj:
            self._fobj.close()
