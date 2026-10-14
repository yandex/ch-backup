"""
Copying of objects between storages.
"""

from typing import NamedTuple


class CopyEntry(NamedTuple):
    """
    Object to copy from one storage to another.

    Storages are described by dicts with the same structure as the "storage"
    config section. Object data is copied as is, without decryption or decompression.
    """

    src_storage: dict
    dst_storage: dict
    src_path: str
    dst_path: str
    size: int
    server_side: bool = False


def storage_id(storage: dict) -> tuple:
    """
    Return identifier of the bucket described by the storage config.
    """
    credentials = storage["credentials"]
    return storage["type"], credentials["endpoint_url"], credentials["bucket"]


def server_side_copy_possible(src_storage: dict, dst_storage: dict) -> bool:
    """
    Return True if objects can be copied between the storages on the server side,
    i.e. both buckets belong to the same S3 service and are accessible with the same credentials.
    """
    if src_storage["type"] != "s3" or dst_storage["type"] != "s3":
        return False

    src_credentials = src_storage["credentials"]
    dst_credentials = dst_storage["credentials"]
    return all(
        src_credentials.get(key) == dst_credentials.get(key)
        for key in ("endpoint_url", "access_key_id", "secret_access_key")
    )
