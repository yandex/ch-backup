"""
Base compression classes module
"""

from abc import ABCMeta, abstractmethod


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
