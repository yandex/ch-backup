"""
Base compression classes module
"""

from abc import ABCMeta, abstractmethod


class BaseCompression(metaclass=ABCMeta):
    """
    Compression base class
    """

    def __init__(self, config):
        pass

    @abstractmethod
    def compress(self, data):
        """
        Compress piece of data
        """
        pass

    @abstractmethod
    def flush_compress(self):
        """
        Flush compressed buffered data
        """
        pass

    @abstractmethod
    def decompress(self, data):
        """
        Decompress piece of data
        """
        pass

    @abstractmethod
    def flush_decompress(self):
        """
        Flush decompressed buffered data
        """
        pass
