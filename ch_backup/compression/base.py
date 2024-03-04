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
    def compress(self, data):
        """
        Compress given data
        """
        pass

    @abstractmethod
    def flush_compress(self):
        """
        Return all buffered compressed data
        """
        pass

    @abstractmethod
    def decompress(self, data):
        """
        Decompress given data
        """
        pass

    @abstractmethod
    def flush_decompress(self):
        """
        Return all buffered decompressed data
        """
        pass
