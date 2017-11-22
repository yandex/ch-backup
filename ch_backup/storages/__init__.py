"""
Pluggable storages for backups
"""

from ch_backup.exceptions import StorageUnknownError
from ch_backup.storages.s3 import S3Loader

SUPPORTED_STORAGES = {
    's3': S3Loader,
}


def get_storage_loader(loader_id, config):
    """
    Get supported storage
    """
    try:
        return SUPPORTED_STORAGES[loader_id](config)
    except KeyError:
        raise StorageUnknownError(
            'Unknown storage "{loader_id}"'.format(loader_id=loader_id))
