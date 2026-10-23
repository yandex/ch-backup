"""
Encryption package
"""

from typing import Mapping

from ch_backup.config import Config
from ch_backup.encryption.base import BaseEncryption
from ch_backup.encryption.nacl import NaClEncryption
from ch_backup.encryption.noop import NoopEncryption
from ch_backup.exceptions import UnknownEncryptionError

SUPPORTED_CRYPTO: Mapping[str, type[BaseEncryption]] = {
    "noop": NoopEncryption,
    "nacl": NaClEncryption,
}


def get_encryption_class(type_id: str) -> type[BaseEncryption]:
    """
    Resolve an encryption provider without requiring its key.
    """
    try:
        return SUPPORTED_CRYPTO[type_id]
    except KeyError as e:
        raise UnknownEncryptionError(f'Unknown encryption type "{type_id}"') from e


def get_encryption(type_id: str, config: dict) -> BaseEncryption:
    """
    Resolve and instantiate a supported encryption provider.
    """
    return get_encryption_class(type_id)(config)


def get_cloud_storage_data_encryption_config(config: Config | dict) -> dict:
    """
    Return encryption settings for copied cloud storage data objects.

    Dedicated settings override the main backup encryption settings. This lets
    copy use either the main backup key or a separate session key.
    """
    result = dict(config["encryption"])
    result.update(config["cloud_storage"].get("data_encryption") or {})
    return result
