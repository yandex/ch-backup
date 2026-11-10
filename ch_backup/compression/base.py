"""
Base compression classes module
"""

from abc import ABCMeta, abstractmethod
from typing import Iterator


class BaseCompression(metaclass=ABCMeta):
    """
    Compression base class
    """

    def __init__(self):
        pass

    @abstractmethod
    def compress(self, data: bytes) -> bytes:
        """
        Compress given data
        """
        pass

    @abstractmethod
    def flush_compress(self) -> bytes:
        """
        Return all buffered compressed data
        """
        pass

    @abstractmethod
    def decompress(self, data: bytes) -> bytes:
        """
        Decompress given data
        """
        pass

    @abstractmethod
    def flush_decompress(self) -> bytes:
        """
        Return all buffered decompressed data
        """
        pass

    def decompress_chunks(self, data: bytes, chunk_size: int) -> Iterator[bytes]:
        """Split legacy decoder output; override to also bound decompression memory."""
        if chunk_size <= 0:
            raise ValueError("Decompression chunk size must be positive")
        output = self.decompress(data)
        for offset in range(0, len(output), chunk_size):
            yield output[offset : offset + chunk_size]
