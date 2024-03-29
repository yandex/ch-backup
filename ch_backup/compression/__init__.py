"""
Compression package
"""

from typing import Mapping, Type

from ch_backup.compression.base import BaseCompression
from ch_backup.compression.gzip import GZIPCompression
from ch_backup.exceptions import UnknownEncryptionError

SUPPORTED_COMPRESSION: Mapping[str, Type[BaseCompression]] = {
    "gzip": GZIPCompression,
}


def get_compression(type_id: str) -> BaseCompression:
    """
    Return supported compression
    """
    try:
        return SUPPORTED_COMPRESSION[type_id]()
    except KeyError:
        raise UnknownEncryptionError(f'Unknown encryption type "{type_id}"')
