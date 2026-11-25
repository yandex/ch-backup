"""
GZIP compression module
"""

from typing import Iterator
from zlib import (
    DEFLATED,
    Z_DEFAULT_COMPRESSION,
    Z_FULL_FLUSH,
    compressobj,
    decompressobj,
)

from ch_backup.compression.base import BaseCompression

# Add gzip header to the compressed data
Z_DEFAULT_WBITS = 31


class GZIPCompression(BaseCompression):
    """
    gzip compression
    """

    def __init__(self):
        self._compressobj = compressobj(
            Z_DEFAULT_COMPRESSION, DEFLATED, Z_DEFAULT_WBITS
        )
        self._decompressobj = decompressobj(Z_DEFAULT_WBITS)

    def compress(self, data: bytes) -> bytes:
        """
        Compress given data
        """
        return self._compressobj.compress(data)

    def decompress(self, data: bytes) -> bytes:
        """
        Decompress given data
        """
        return self._decompressobj.decompress(data)

    def decompress_chunks(self, data: bytes, chunk_size: int) -> Iterator[bytes]:
        """Drain compressed input and buffered output in bounded chunks."""
        if chunk_size <= 0:
            raise ValueError("Decompression chunk size must be positive")
        while True:
            chunk = self._decompressobj.decompress(data, chunk_size)
            data = self._decompressobj.unconsumed_tail
            if chunk:
                yield chunk
            if self._decompressobj.eof or (not data and len(chunk) < chunk_size):
                return

    def flush_compress(self) -> bytes:
        """
        Return all buffered compressed data
        """
        return self._compressobj.flush(Z_FULL_FLUSH)

    def flush_decompress(self) -> bytes:
        """
        Return all buffered decompressed data
        """
        return self._decompressobj.flush()
