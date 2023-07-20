"""
Bytes FIFO module.
"""
import io


class BytesFIFO:
    """
    A FIFO that can store a fixed number of bytes.

    Implemented in terms of circular buffer.
    """
    def __init__(self, init_size: int) -> None:
        """
        Create a FIFO of ``init_size`` bytes.
        """
        self._buffer = io.BytesIO(b'\x00' * init_size)
        self._size = init_size
        self._filled = 0
        self._read_ptr = 0
        self._write_ptr = 0

    def read(self, size: int = -1) -> bytes:
        """
        Read at most ``size`` bytes from the FIFO.

        If less than ``size`` bytes are available, or ``size`` is negative,
        return all remaining bytes.
        """
        if size < 0:
            size = self._filled

        # Go to read pointer
        self._buffer.seek(self._read_ptr)

        # Figure out how many bytes we can really read
        size = min(size, self._filled)
        contiguous_size = self._size - self._read_ptr
        contiguous_read = min(contiguous_size, size)

        ret = self._buffer.read(contiguous_read)
        self._read_ptr += contiguous_read
        if contiguous_read < size:
            leftover_size = size - contiguous_read
            self._buffer.seek(0)
            ret += self._buffer.read(leftover_size)
            self._read_ptr = leftover_size

        self._filled -= size

        return ret

    def write(self, data: bytes) -> int:
        """
        Write as many bytes of ``data`` as are free in the FIFO.

        If less than ``len(data)`` bytes are free, write as many as can be written.
        Returns the number of bytes written.
        """
        free = self.free()
        write_size = min(len(data), free)

        if write_size:
            contiguous_size = self._size - self._write_ptr
            contiguous_write = min(contiguous_size, write_size)

            self._buffer.seek(self._write_ptr)
            self._buffer.write(data[:contiguous_write])
            self._write_ptr += contiguous_write

            if contiguous_size < write_size:
                self._buffer.seek(0)
                self._buffer.write(data[contiguous_write:write_size])
                self._write_ptr = write_size - contiguous_write

        self._filled += write_size

        return write_size

    def flush(self) -> None:
        """Flush all data from the FIFO."""
        self._filled = 0
        self._read_ptr = 0
        self._write_ptr = 0

    def empty(self) -> bool:
        """Return ```True``` if FIFO is empty."""
        return self._filled == 0

    def full(self) -> bool:
        """Return ``True`` if FIFO is full."""
        return self._filled == self._size

    def free(self) -> int:
        """Return the number of bytes that can be written to the FIFO."""
        return self._size - self._filled

    def capacity(self) -> int:
        """Return the total space allocated for this FIFO."""
        return self._size

    def __len__(self) -> int:
        """Return the amount of data filled in FIFO"""
        return self._filled

    def resize(self, new_size: int) -> None:
        """
        Resize FIFO to contain ``new_size`` bytes. If FIFO currently has
        more than ``new_size`` bytes filled, :exc:`ValueError` is raised.
        If ``new_size`` is less than 1, :exc:`ValueError` is raised.

        If ``new_size`` is smaller than the current size, the internal
        buffer is not contracted (yet).
        """
        if new_size < 1:
            raise ValueError('Cannot resize to zero or less bytes.')

        if new_size < self._filled:
            raise ValueError(f'Cannot contract FIFO to less than {self._filled} bytes, or data will be lost.')

        # original data is non-contiguous. we need to copy old data,
        # re-write to the beginning of the buffer, and re-sync
        # the read and write pointers.
        if self._read_ptr >= self._write_ptr:
            old_data = self.read(self._filled)
            self._buffer.seek(0)
            self._buffer.write(old_data)
            self._filled = len(old_data)
            self._read_ptr = 0
            self._write_ptr = self._filled

        self._size = new_size
