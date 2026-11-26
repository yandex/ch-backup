"""
Copying of objects between storages.
"""

import copy
import re
from enum import Enum
from typing import Any, NamedTuple
from urllib.parse import urlparse

from botocore.configloader import load_config
from botocore.exceptions import ConfigNotFound, ConfigParseError

from ch_backup.exceptions import ConfigurationError


class ServerSideCopyMode(str, Enum):
    """Policy for server-side copying and fallback through ch-backup."""

    STRICT = "strict"
    AUTO = "auto"
    DISABLED = "disabled"

    def __str__(self) -> str:
        """Display the configuration value in logs."""
        return self.value


def parse_server_side_copy_mode(value: Any) -> ServerSideCopyMode:
    """Validate the configured server-side copy mode."""
    try:
        return ServerSideCopyMode(value)
    except (TypeError, ValueError) as e:
        choices = ", ".join(mode.value for mode in ServerSideCopyMode)
        raise ConfigurationError(
            f"copy.server_side_copy_mode must be one of: {choices}"
        ) from e


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
    if not all(
        src_credentials.get(key) == dst_credentials.get(key)
        for key in ("endpoint_url", "access_key_id", "secret_access_key")
    ):
        return False

    source = src_credentials.get("aws_config_file") or None
    target = dst_credentials.get("aws_config_file") or None
    if source == target:
        return src_storage.get("_aws_config") == dst_storage.get("_aws_config")
    if not source or not target:
        return False
    try:
        return (src_storage.get("_aws_config") or load_aws_config(src_credentials)) == (
            dst_storage.get("_aws_config") or load_aws_config(dst_credentials)
        )
    except ConfigurationError:
        return False


class _AWSConfig(dict):
    """A parsed AWS config bound to the file selected at startup."""

    def __init__(self, path: str):
        """Capture the SDK settings and the path they belong to."""
        super().__init__(load_config(path))
        self.path = path


def load_aws_config(credentials: dict, snapshot: dict | None = None) -> dict | None:
    """Validate the selected authentication method and load its AWS profile."""
    path = credentials.get("aws_config_file")
    access = credentials.get("access_key_id")
    secret = credentials.get("secret_access_key")
    if bool(access) != bool(secret):
        raise ConfigurationError(
            "access_key_id and secret_access_key must be set together"
        )
    if path is None or path == "":
        return None
    if not isinstance(path, str):
        raise ConfigurationError(
            "storage.credentials.aws_config_file must be a path string"
        )
    if access or secret:
        raise ConfigurationError(
            "aws_config_file and static credentials are mutually exclusive"
        )
    endpoint = credentials.get("endpoint_url")
    if not isinstance(endpoint, str) or not endpoint.strip():
        raise ConfigurationError(
            "storage.credentials.endpoint_url is required with aws_config_file"
        )
    if isinstance(snapshot, _AWSConfig) and snapshot.path == path:
        return snapshot
    try:
        settings = _AWSConfig(path)
    except (ConfigNotFound, ConfigParseError, OSError, ValueError) as error:
        raise ConfigurationError(
            f"Cannot read storage.credentials.aws_config_file ({type(error).__name__})"
        ) from None
    process = settings["profiles"].get("default", {}).get("credential_process")
    if not isinstance(process, str) or not process.strip():
        raise ConfigurationError(
            "aws_config_file requires [default].credential_process"
        )
    return settings


def validate_aws_config(storage: dict, snapshot: dict | None = None) -> None:
    """Capture storage settings before accessing S3 or starting any workers."""
    settings = load_aws_config(storage["credentials"], snapshot)
    if settings is None:
        storage.pop("_aws_config", None)
    else:
        storage["_aws_config"] = settings


def make_storage_config(
    base: dict,
    endpoint_url: str,
    bucket: str,
    access_key_id: str | None,
    secret_access_key: str | None,
    *,
    aws_config_file: str | None = None,
) -> dict:
    """Make storage config for a bucket, retaining non-credential settings."""
    storage = copy.deepcopy(base)
    storage["credentials"] = {
        "endpoint_url": endpoint_url,
        "bucket": bucket,
        "access_key_id": access_key_id,
        "secret_access_key": secret_access_key,
    }
    if aws_config_file is not None:
        storage["credentials"]["aws_config_file"] = aws_config_file
    if not aws_config_file or aws_config_file != base["credentials"].get(
        "aws_config_file"
    ):
        storage.pop("_aws_config", None)
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
