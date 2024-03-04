"""
GZIP compression module
"""

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
