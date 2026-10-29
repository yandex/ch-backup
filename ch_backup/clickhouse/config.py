"""
Module for working with Clickhouse configuration file.
"""

from typing import Any
from urllib.parse import urlparse

import xmltodict

from ch_backup.config import Config
from ch_backup.util import s3_uri_from_path_style_to_virtual_hosted


class ClickhouseConfig:
    """
    Class for working with Clickhouse configuration file.
    """

    def __init__(self, config: Config) -> None:
        self._ch_config: dict[str, Any] = {}
        self._ch_config_path = config["clickhouse"]["preprocessed_config_path"]

    def load(self) -> None:
        """
        Loads clickhouse configuration file.
        """
        with open(self._ch_config_path, "r", encoding="utf-8") as file:
            config = xmltodict.parse(file.read(), disable_entities=False)
            self._ch_config = config.get("clickhouse", config.get("yandex"))

    @property
    def config(self) -> dict:
        """
        Returns CH config, loads it if necessary.
        """
        if not self._ch_config:
            self.load()

        return self._ch_config


def set_backup_storage(
    disk_config: dict, storage_config: dict, key_prefix: str
) -> None:
    """
    Point a disk configuration to a location in the backup storage.
    """
    credentials = storage_config["credentials"]
    disk_config["endpoint"] = _backup_storage_endpoint(storage_config, key_prefix)
    disk_config["access_key_id"] = credentials["access_key_id"]
    disk_config["secret_access_key"] = credentials["secret_access_key"]

    proxy = _backup_storage_proxy(storage_config)
    if proxy:
        disk_config["proxy"] = proxy


def _backup_storage_endpoint(storage_config: dict, key_prefix: str) -> str:
    """
    Build the backup storage URL of a given key prefix.

    "auto" addressing style is built as path-style, since it puts no
    requirements on the bucket name.
    """
    credentials = storage_config["credentials"]
    endpoint_url = credentials["endpoint_url"].rstrip("/")
    url = f"{endpoint_url}/{credentials['bucket']}/{key_prefix.strip('/')}/"

    if storage_config["boto_config"]["addressing_style"] == "virtual":
        return s3_uri_from_path_style_to_virtual_hosted(url)

    return url


def _backup_storage_proxy(storage_config: dict) -> dict | None:
    """
    Build the proxy configuration for reaching the backup storage, if any.

    ClickHouse is given the resolver rather than a host resolved once, so that
    it picks a proxy per request and drops one that fails. The endpoint always
    gets a path: the resolve request line is built out of the path alone.
    """
    proxy_resolver = storage_config.get("proxy_resolver", {})
    resolver_uri = proxy_resolver.get("uri")
    if not resolver_uri:
        return None

    endpoint = urlparse(resolver_uri)
    if not endpoint.path:
        endpoint = endpoint._replace(path="/")

    return {
        "resolver": {
            "endpoint": endpoint.geturl(),
            "proxy_scheme": "http",
            "proxy_port": str(proxy_resolver["proxy_port"]),
        }
    }
