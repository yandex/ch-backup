"""
Writing files from TAR stream stage.
"""

from enum import Enum
from pathlib import Path
from tarfile import BLOCKSIZE, ENCODING, GNUTYPE_LONGNAME, NUL, TarInfo
from typing import IO, Any, Iterator

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


class TarStreamProcessorBase(Handler):
    """
    Base class for processing TAR stream.
    """

    stype = StageType.FILESYSTEM

    def __init__(self, config: dict, buffer_size: int) -> None:
        self._config = config

        if buffer_size < BLOCKSIZE:
            raise ValueError(
                f"Size of TAR stream buffer cannot be less than TAR BLOCKSIZE: {BLOCKSIZE}"
            )

        self._tarstream = BytesFIFO(buffer_size)
        self._state: State = State.READ_HEADER

        self._bytes_to_process: int = 0
        self._tarinfo: TarInfo | None = None
        self._name_from_buffer: bytes | None = None

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
        self._on_file_complete()

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

            self._on_file_start()

        return True

    def _on_file_complete(self) -> None:
        pass

    def _on_file_start(self) -> None:
        pass

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

        buf = self._tarstream.read(self._bytes_to_process)
        self._write_data(buf)
        self._bytes_to_process -= len(buf)

        if self._bytes_to_process > 0:
            return False

        self._state = State.SKIP_BYTES
        if self._tarinfo.size % BLOCKSIZE > 0:
            self._bytes_to_process = BLOCKSIZE - (self._tarinfo.size % BLOCKSIZE)

        return True

    def _write_data(self, data: bytes) -> None:
        raise NotImplementedError

    def _skip_bytes(self) -> bool:
        buf = self._tarstream.read(self._bytes_to_process)
        assert buf.count(NUL) == len(buf), buf
        self._bytes_to_process -= len(buf)
        if self._bytes_to_process > 0:
            return False

        self._state = State.READ_HEADER
        return True

    def on_done(self) -> Any:
        tarinfo = self._tarinfo
        self._on_file_complete()
        # Early writers omitted all final padding if the last read filled a chunk.
        # Accept only a complete regular file with its entire padding outstanding.
        if (
            self._state == State.SKIP_BYTES
            and tarinfo is not None
            and tarinfo.isreg()
            and self._bytes_to_process == -tarinfo.size % BLOCKSIZE
        ):
            return
        if (
            self._state != State.READ_HEADER
            or len(self._tarstream)
            or self._name_from_buffer is not None
        ):
            raise ValueError("Truncated TAR stream")


class WriteFilesStage(TarStreamProcessorBase):
    """
    Unarchive and save files to the local filesystem from TAR stream.
    """

    def __init__(self, config: dict, dir_path: Path, buffer_size: int) -> None:
        super().__init__(config, buffer_size)
        self._dir: Path = dir_path
        self._fobj: IO | None = None

    def _on_file_complete(self) -> None:
        if self._fobj:
            self._fobj.close()
            self._fobj = None

    def _on_file_start(self) -> None:
        assert self._tarinfo
        filepath = self._dir / self._tarinfo.name
        filepath.parent.mkdir(parents=True, exist_ok=True)
        self._fobj = filepath.open("wb")

    def _write_data(self, data: bytes) -> None:
        assert self._fobj
        self._fobj.write(data)


class UnpackTarballStage(TarStreamProcessorBase):
    """
    Unpack TAR stream to in-memory data pairs (filename, data).
    """

    def __init__(
        self, config: dict, buffer_size: int, include_empty: bool = False
    ) -> None:
        """Collect completed files, optionally preserving empty metadata members."""
        super().__init__(config, buffer_size)
        self._include_empty = include_empty
        self._current_data: bytearray = bytearray()
        self._results: list[tuple[str, bytes]] = []

    def _on_file_complete(self) -> None:
        """Emit a completed member and clear its state."""
        tarinfo = self._tarinfo
        if tarinfo is None:
            return
        include_empty_file = (
            self._include_empty and tarinfo.isfile() and tarinfo.size == 0
        )
        if not self._current_data and not include_empty_file:
            return
        self._results.append((tarinfo.name, bytes(self._current_data)))
        self._current_data = bytearray()
        self._tarinfo = None

    def _on_file_start(self) -> None:
        self._current_data = bytearray()

    def _write_data(self, data: bytes) -> None:
        self._current_data.extend(data)

    def on_done(self) -> Iterator[tuple[str, bytes]]:
        super().on_done()
        yield from self.take_files()

    def take_files(self) -> list[tuple[str, bytes]]:
        """Release files completed by the last input chunk."""
        files, self._results = self._results, []
        return files
