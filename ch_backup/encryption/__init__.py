"""
Encryption package
"""

import hmac
from hashlib import sha256
from typing import Any, Mapping

from ch_backup.calculators import calc_encrypted_size
from ch_backup.config import Config
from ch_backup.encryption.base import BaseEncryption
from ch_backup.encryption.nacl import NaClEncryption
from ch_backup.encryption.noop import NoopEncryption
from ch_backup.exceptions import UnknownEncryptionError

SUPPORTED_CRYPTO: Mapping[str, type[BaseEncryption]] = {
    "noop": NoopEncryption,
    "nacl": NaClEncryption,
}

CLOUD_STORAGE_DATA_ENCRYPTION_VERSION = 1
_CLOUD_STORAGE_DATA_KEY_ID_CONTEXT = b"ch-backup cloud data key id"


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


def get_cloud_storage_data_encryption_metadata(config: dict) -> dict[str, Any]:
    """Return the non-secret identity of the cloud data encryption format and key."""
    key_id = hmac.new(
        config["key"].encode("utf-8"),
        _CLOUD_STORAGE_DATA_KEY_ID_CONTEXT,
        sha256,
    ).hexdigest()[:32]
    return {
        "version": CLOUD_STORAGE_DATA_ENCRYPTION_VERSION,
        "type": config["type"],
        "chunk_size": config["chunk_size"],
        "key_id": key_id,
    }


def calc_encrypted_size_from_config(config: dict, data_size: int) -> int:
    """Calculate encrypted object size without requiring its encryption key."""
    metadata_size = get_encryption_class(config["type"]).metadata_size()
    return calc_encrypted_size(data_size, config["chunk_size"], metadata_size)
