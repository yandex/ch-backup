"""
Encrypting stage.
"""

from typing import Tuple

from ch_backup.encryption import BaseEncryption
from ch_backup.storage.async_pipeline.base_pipeline.handler import Handler
from ch_backup.storage.async_pipeline.stages.backup.stage_communication import (
    PartPipelineInfo,
)
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


class EncryptPartStage(Handler):
    """
    Encrypts data by chunk of specified size.
    """

    stype = StageType.ENCRYPTION

    def __init__(self, crypto: BaseEncryption) -> None:
        self._crypto = crypto

    def __call__(
        self, data_and_info: Tuple[bytes, PartPipelineInfo], index: int
    ) -> Tuple[bytes, PartPipelineInfo]:
        data, part_info = data_and_info
        encrypted_data = self._crypto.encrypt(data)
        return encrypted_data, part_info
