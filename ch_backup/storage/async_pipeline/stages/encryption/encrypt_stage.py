"""
Encrypting stage.
"""
from ch_backup.encryption import BaseEncryption
from ch_backup.storage.async_pipeline.base_pipeline.handler import Handler
from ch_backup.storage.async_pipeline.stages.types import StageType


class EncryptStage(Handler):
    """
    Encrypts data by chunk of specified size.
    """

    stype = StageType.ENCRYPTION

    def __init__(self, crypto: BaseEncryption) -> None:
        self._crypto = crypto

    def __call__(self, data: bytes, index: int) -> bytes:
        encrypted_data = self._crypto.encrypt(data)
        return encrypted_data
