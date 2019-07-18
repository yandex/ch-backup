"""
Base encryption classes module
"""

from abc import ABCMeta, abstractmethod


class BaseEncryption(metaclass=ABCMeta):
    """
    Encryption base class
    """
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
