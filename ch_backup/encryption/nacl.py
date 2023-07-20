"""
NaCl encryption module
"""

from nacl.secret import SecretBox
from nacl.utils import random

from ch_backup.encryption.base import BaseEncryption


class NaClEncryption(BaseEncryption):
    """
    NaCl (libsodium) encryption

    https://pynacl.readthedocs.io/en/latest/secret/
    """

    def __init__(self, conf):
        super().__init__(conf)
        self._box = SecretBox(conf["key"].encode("utf-8"))

    def encrypt(self, data):
        return self._box.encrypt(data)

    def decrypt(self, data):
        return self._box.decrypt(data)

    @staticmethod
    def metadata_size():
        """
        Computes NaCl metadata size
        """

        # https://pynacl.readthedocs.io/en/latest/secret/#example
        return (
            SecretBox.NONCE_SIZE + 16
        )  # TODO use SecretBox.MACBYTES after PyNaCl update

    @staticmethod
    def gen_secret_key(size=None):
        """
        Generates random nacl key in binary format
        """

        if size is None:
            size = SecretBox.KEY_SIZE
        return random(size)
