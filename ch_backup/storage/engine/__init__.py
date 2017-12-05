"""
Package with definition of storage engines providing base API for working with
storage (S3, filesystem, etc.) in uniform way.

It's currently supported only S3 storage engine.
"""

from ch_backup.exceptions import ConfigurationError
from .s3 import S3StorageEngine

SUPPORTED_STORAGES = {
    's3': S3StorageEngine,
}


def get_storage_engine(config):
    """
    Return storage engine corresponding to passed in configuration.
    """
    try:
        engine_id = config['type']
    except KeyError:
        raise ConfigurationError('Storage type is missing in the config')

    try:
        return SUPPORTED_STORAGES[engine_id](config)
    except KeyError:
        raise ConfigurationError('Unknown storage "{0}"'.format(engine_id))
