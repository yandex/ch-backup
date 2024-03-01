"""
ZLIB compression module
"""

from zlib import (
    DEFLATED,
    Z_DEFAULT_COMPRESSION,
    Z_FULL_FLUSH,
    compressobj,
    decompressobj,
)

from ch_backup.compression.base import BaseCompression

Z_DEFAULT_WBITS = 15


class ZLIBCompression(BaseCompression):
    """
    zlib compression.
    """

    def __init__(self, conf):
        self._compressobj = compressobj(
            Z_DEFAULT_COMPRESSION, DEFLATED, Z_DEFAULT_WBITS
        )
        self._decompressobj = decompressobj(Z_DEFAULT_WBITS)

    def compress(self, data):
        """
        Compress given data
        """
        return self._compressobj.compress(data)

    def decompress(self, data):
        """
        Decompress given data
        """
        return self._decompressobj.decompress(data)

    def flush_compress(self):
        return self._compressobj.flush(Z_FULL_FLUSH)

    def flush_decompress(self):
        return self._decompressobj.flush()
