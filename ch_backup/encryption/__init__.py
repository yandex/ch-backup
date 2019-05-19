"""
Encryption package
"""

from ch_backup.encryption.nacl import NaClEncryption
from ch_backup.exceptions import UnknownEncryptionError

SUPPORTED_CRYPTO = {
    'nacl': NaClEncryption,
}


def get_encryption(type_id, config):
    """
    Get supported storage
    """
    try:
        return SUPPORTED_CRYPTO[type_id](config)
    except KeyError:
        raise UnknownEncryptionError(f'Unknown encryption type "{type_id}"')
