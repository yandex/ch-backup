"""
Encryption package
"""

from typing import Mapping, Type

from ch_backup.encryption.base import BaseEncryption
from ch_backup.encryption.nacl import NaClEncryption
from ch_backup.encryption.noop import NoopEncryption
from ch_backup.exceptions import UnknownEncryptionError

SUPPORTED_CRYPTO: Mapping[str, Type[BaseEncryption]] = {
    'noop': NoopEncryption,
    'nacl': NaClEncryption,
}


def get_encryption(type_id: str, config: dict) -> BaseEncryption:
    """
    Get supported storage
    """
    try:
        return SUPPORTED_CRYPTO[type_id](config)
    except KeyError:
        raise UnknownEncryptionError(f'Unknown encryption type "{type_id}"')
