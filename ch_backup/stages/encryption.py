"""
Encryption pipeline stages module
"""

from ch_backup.encryption import get_encryption
from ch_backup.stages.base import IterBufferedStage

STAGE_TYPE = 'encryption'


class EncryptStage(IterBufferedStage):
    """
    Encrypts data by chunk of specified size
    """

    stype = STAGE_TYPE

    def __init__(self, conf):
        super().__init__(conf)
        self._crypto = get_encryption(conf['type'], conf)

    def _process(self, data):
        return self._crypto.encrypt(data)


class DecryptStage(IterBufferedStage):
    """
    Encrypts data by chunk of specified size + encrypt metadata size
    """

    stype = STAGE_TYPE

    def __init__(self, conf):
        super().__init__(conf)
        self._crypto = get_encryption(conf['type'], conf)
        self._chunk_size += self._crypto.metadata_size()

    def _process(self, data):
        return self._crypto.decrypt(data)
