"""
S3 client factory.
"""
import socket
import threading
from functools import wraps
from typing import Callable, Optional, Union

import boto3
import requests
from botocore.config import Config

from ch_backup.type_hints.boto3.s3 import S3Client
from ch_backup.util import retry


class S3BalancerUnknownHost(ValueError):
    """
    Used in case S3 balancer implementation returned an unknown host.
    """


class S3ClientFactory:
    """
    Factory to create S3 client instances.
    """

    def __init__(self, config: dict):
        credentials_config = config["credentials"]
        self._config = config
        self._s3_session = boto3.session.Session(
            aws_access_key_id=credentials_config["access_key_id"],
            aws_secret_access_key=credentials_config["secret_access_key"],
            region_name=self._config["boto_config"]["region_name"],
        )

    @retry((S3BalancerUnknownHost, requests.RequestException))
    def _resolve_proxies(
        self, resolver_path: str, proxy_port: int
    ) -> Union[dict, None]:
        """
        Get proxy host name via a special handler
        """
        # pylint: disable=missing-timeout

        if resolver_path is None:
            return None
        req = requests.get(resolver_path)
        req.raise_for_status()
        host = req.text
        # Try to resolve hostname to check the output
        try:
            socket.getaddrinfo(host, 0)
        except socket.gaierror:
            raise S3BalancerUnknownHost(
                f'{resolver_path} returned unknown hostname: "{host}"'
            )
        return {
            "http": f"{host}:{proxy_port}",
            "https": f"{host}:{proxy_port}",
        }

    def create_s3_client(self) -> S3Client:
        """
        Creates S3 client.
        """
        credentials_config = self._config["credentials"]
        boto_config = self._config["boto_config"]

        return self._s3_session.client(
            service_name="s3",
            endpoint_url=credentials_config["endpoint_url"],
            config=Config(
                s3={
                    "addressing_style": boto_config["addressing_style"],
                    "region_name": boto_config["region_name"],
                },
                proxies=self._resolve_proxies(
                    self._config.get("proxy_resolver", {}).get("uri"),
                    self._config.get("proxy_resolver", {}).get("proxy_port"),
                ),
                retries={"max_attempts": 0},  # Disable internal retrying mechanism.
            ),
        )


def synchronized(method: Callable) -> Callable:
    """
    Decorator for synchronizing access to the instance's methods
    """
    lock = threading.Lock()

    @wraps(method)
    def _decorator(self, *args, **kwargs):
        with lock:
            return method(self, *args, **kwargs)

    return _decorator


class S3ClientCachedFactory:
    """
    Thread safe S3 client factory returning cached client or creating new.
    """

    def __init__(self, s3_client_factory: S3ClientFactory) -> None:
        self._s3_client_factory = s3_client_factory
        self._cached_s3_client: Optional[S3Client] = None

    @synchronized
    def create_s3_client(self, cached: bool = True) -> S3Client:
        """
        Return cached S3 client. Or re-create it if needed.
        """
        if not cached or self._cached_s3_client is None:
            self._cached_s3_client = self._s3_client_factory.create_s3_client()

        return self._cached_s3_client

    @synchronized
    def reset(self) -> None:
        """
        Reset cached client.
        """
        self._cached_s3_client = None
