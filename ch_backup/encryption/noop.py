"""
Noop encryption module
"""

from ch_backup.encryption.base import BaseEncryption


class NoopEncryption(BaseEncryption):
    """
    Doing nothing, just for tests.
    """
    def encrypt(self, data):
        """
        Do nothing.
        """
        return data

    def decrypt(self, data):
        """
        Do nothing.
        """
        return data

    def metadata_size(self):
        """
        Extra space :)
        """
        return 0
