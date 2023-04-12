"""
Encryption pipeline stages module
"""

from ch_backup.encryption import get_encryption

from .base import BufferedIterStage

STAGE_TYPE = 'encryption'


class EncryptStage(BufferedIterStage):
    """
    Encrypts data by chunk of specified size
    """

    stype = STAGE_TYPE

    def __init__(self, conf, params):
        super().__init__(conf, params)
        self._crypto = get_encryption(conf['type'], conf)

    def _process(self, data):
        return self._crypto.encrypt(data)


class DecryptStage(BufferedIterStage):
    """
    Encrypts data by chunk of specified size + encrypt metadata size
    """

    stype = STAGE_TYPE

    def __init__(self, conf, params):
        super().__init__(conf, params)
        self._crypto = get_encryption(conf['type'], conf)
        self._chunk_size += self._crypto.metadata_size()

    def _process(self, data):
        return self._crypto.decrypt(data)
