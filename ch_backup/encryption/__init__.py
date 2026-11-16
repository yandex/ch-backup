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
from ch_backup.exceptions import ConfigurationError, UnknownEncryptionError

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


def validate_cloud_storage_data_encryption_config(
    config: Config | dict, metadata: Mapping[str, Any]
) -> dict:
    """Return cloud data encryption config after matching it to backup metadata."""
    encryption_config = get_cloud_storage_data_encryption_config(config)
    if not encryption_config.get("key"):
        raise ConfigurationError(
            "cloud_storage.data_encryption.key or encryption.key is required "
            "to restore encrypted cloud storage data"
        )

    get_encryption(encryption_config["type"], encryption_config)
    expected = get_cloud_storage_data_encryption_metadata(encryption_config)
    for field in ("version", "type", "chunk_size"):
        if metadata.get(field) != expected[field]:
            raise ConfigurationError(
                f"cloud_storage.data_encryption.{field} does not match backup metadata"
            )

    key_id = metadata.get("key_id")
    if not isinstance(key_id, str) or not hmac.compare_digest(
        key_id, expected["key_id"]
    ):
        raise ConfigurationError(
            "cloud_storage.data_encryption.key does not match backup metadata"
        )
    return encryption_config


def calc_encrypted_size_from_config(config: dict, data_size: int) -> int:
    """Calculate encrypted object size without requiring its encryption key."""
    metadata_size = get_encryption_class(config["type"]).metadata_size()
    return calc_encrypted_size(data_size, config["chunk_size"], metadata_size)
