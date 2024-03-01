"""
Compression package
"""

from typing import Mapping, Type

from ch_backup.compression.base import BaseCompression
from ch_backup.compression.zlib import ZLIBCompression
from ch_backup.exceptions import UnknownEncryptionError

SUPPORTED_COMPRESSION: Mapping[str, Type[BaseCompression]] = {
    "zlib": ZLIBCompression,
}


def get_compression(type_id: str, config: dict) -> BaseCompression:
    """
    Return supported compression
    """
    try:
        return SUPPORTED_COMPRESSION[type_id](config)
    except KeyError:
        raise UnknownEncryptionError(f'Unknown encryption type "{type_id}"')
