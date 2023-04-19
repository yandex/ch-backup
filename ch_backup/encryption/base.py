"""
Base encryption classes module
"""

from abc import ABCMeta, abstractmethod


class BaseEncryption(metaclass=ABCMeta):
    """
    Encryption base class
    """
    def __init__(self, config):
        pass

    @abstractmethod
    def encrypt(self, data):
        """
        Encrypt piece of data
        """
        pass

    @abstractmethod
    def decrypt(self, data):
        """
        Decrypt piece of data
        """
        pass

    @abstractmethod
    def metadata_size(self):
        """
        Returns extra amount of extra space for encryption stuff.
        """
