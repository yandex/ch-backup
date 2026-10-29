"""
Copying of objects between storages.
"""

import copy
import re
from typing import NamedTuple
from urllib.parse import urlparse


class CopyEntry(NamedTuple):
    """
    Object to copy from one storage to another.

    Storages are described by dicts with the same structure as the "storage"
    config section. Object data is copied as is unless ``encrypt`` or ``decrypt``
    is set.
    """

    src_storage: dict
    dst_storage: dict
    src_path: str
    dst_path: str
    size: int
    dst_size: int
    server_side: bool = False
    encrypt: bool = False
    decrypt: bool = False


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


def make_storage_config(
    base: dict,
    endpoint_url: str,
    bucket: str,
    access_key_id: str | None,
    secret_access_key: str | None,
) -> dict:
    """Make storage config for a bucket, retaining non-credential settings."""
    storage = copy.deepcopy(base)
    storage["credentials"] = {
        "endpoint_url": endpoint_url,
        "bucket": bucket,
        "access_key_id": access_key_id,
        "secret_access_key": secret_access_key,
    }
    # Proxy is configured for backup storage only.
    storage["proxy_resolver"] = {"uri": None, "proxy_port": None}
    return storage


def get_s3_disk_location(disk_config: dict) -> tuple[str, str, str] | None:
    """Return endpoint URL, bucket and key prefix of a ClickHouse S3 disk."""
    endpoint = disk_config.get("endpoint")
    if not isinstance(endpoint, str):
        return None

    url = urlparse(endpoint)
    # Follow ClickHouse's S3 host recognition; AWS PrivateLink uses path-style buckets.
    virtual_host = re.fullmatch(r"(.+)\.(s3[.-][a-z0-9.:-]+)", url.netloc)
    private_link = re.fullmatch(
        r"bucket\.vpce-[a-z0-9.-]+\.vpce\.amazonaws\.com(:\d{1,5})?", url.netloc
    )
    if virtual_host and not private_link:
        bucket, host = virtual_host.groups()
        return f"{url.scheme}://{host}", bucket, url.path.strip("/")

    bucket, _, path = url.path.strip("/").partition("/")
    return f"{url.scheme}://{url.netloc}", bucket, path
