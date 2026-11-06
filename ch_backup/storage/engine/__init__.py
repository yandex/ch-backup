"""
Package with definition of storage engines providing base API for working with
storage (S3, filesystem, etc.) in uniform way.

It's currently supported only S3 storage engine.
"""

from ch_backup.exceptions import ConfigurationError
from ch_backup.storage.engine.base import PipeLineCompatibleStorageEngine
from ch_backup.storage.engine.s3 import S3StorageEngine
from ch_backup.storage.engine.s3.s3_client_factory import get_cached_s3_client_factory

SUPPORTED_STORAGES = {
    "s3": S3StorageEngine,
}


def get_storage_engine(config: dict) -> PipeLineCompatibleStorageEngine:
    """
    Return storage engine corresponding to passed in configuration.
    """
    try:
        engine_id = config["type"]
    except KeyError:
        raise ConfigurationError("Storage type is missing in the config")

    try:
        return SUPPORTED_STORAGES[engine_id](
            config, client_factory=get_cached_s3_client_factory(config)
        )
    except KeyError:
        raise ConfigurationError(f'Unknown storage "{engine_id}"')
