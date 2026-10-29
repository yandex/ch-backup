"""Restore staging for encrypted object storage data."""

from ch_backup.config import Config
from ch_backup.exceptions import ConfigurationError
from ch_backup.storage.object_copy import (
    get_s3_disk_location,
    make_storage_config,
    storage_id,
)

DEFAULT_DECRYPTED_SUFFIX = "_decrypted"


def resolve_decryption_staging(
    config: Config | dict,
    source_storage: dict,
    source_path: str,
    destination_disk: dict,
) -> tuple[dict, str]:
    """Return storage and key prefix for plaintext cloud storage objects."""
    disk_location = get_s3_disk_location(destination_disk)
    if disk_location is None:
        raise ConfigurationError(
            "Failed to determine decryption staging location from ClickHouse disk; "
            "set restore.cloud_storage.decryption_staging explicitly"
        )
    disk_endpoint, disk_bucket, _ = disk_location

    staging_config = config["restore"]["cloud_storage"]["decryption_staging"]
    endpoint_url = staging_config.get("endpoint_url") or disk_endpoint
    bucket = staging_config.get("bucket") or disk_bucket
    if staging_config.get("access_key_id"):
        access_key_id = staging_config["access_key_id"]
        secret_access_key = staging_config.get("secret_access_key")
    else:
        access_key_id = destination_disk.get("access_key_id")
        secret_access_key = destination_disk.get("secret_access_key")

    storage = make_storage_config(
        config["storage"],
        endpoint_url,
        bucket,
        access_key_id,
        secret_access_key,
    )
    source_path = _normalize_prefix(source_path)
    configured_path = staging_config.get("path")
    if configured_path is None:
        staging_path = source_path
        if _same_storage(source_storage, storage):
            staging_path = f"{source_path}{DEFAULT_DECRYPTED_SUFFIX}"
    else:
        staging_path = _normalize_prefix(configured_path)

    if _same_storage(source_storage, storage) and staging_path == source_path:
        raise ConfigurationError(
            "Decryption staging must not overwrite encrypted cloud storage data"
        )
    return storage, staging_path


def _same_storage(first: dict, second: dict) -> bool:
    first_id = storage_id(first)
    second_id = storage_id(second)
    return (
        first_id[0] == second_id[0]
        and str(first_id[1]).rstrip("/") == str(second_id[1]).rstrip("/")
        and first_id[2] == second_id[2]
    )


def _normalize_prefix(path: str) -> str:
    prefix = path.strip("/")
    if any(part in (".", "..") for part in prefix.split("/")):
        raise ConfigurationError(f'Unsafe decryption staging path "{path}"')
    return prefix
