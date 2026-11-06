"""
S3 client factory.
"""

import socket
import threading
from typing import Any

import boto3
import requests
from botocore.config import Config
from botocore.endpoint import convert_to_response_dict
from botocore.parsers import create_parser

from ch_backup import logging
from ch_backup.type_hints.boto3.s3 import S3Client


def _repair_copy_error_response(response: Any, operation: Any, **_kwargs: Any) -> None:
    """Keep embedded S3 errors readable on older supported botocore versions."""
    if response is None:
        return
    http_response, parsed = response
    # Older botocore detects HTTP 200 errors only after parsing, changes the
    # HTTP status to 500, but leaves a success-shaped result without Error.Code.
    # Run after that handler and parse again with the corrected status. Newer
    # versions already normalize before parsing and need no repair.
    if (
        http_response.status_code >= 300
        and parsed.get("ResponseMetadata", {}).get("HTTPStatusCode") == 200
    ):
        parsed.clear()
        parsed.update(
            create_parser(operation.metadata["protocol"]).parse(
                convert_to_response_dict(http_response, operation),
                operation.output_shape,
            )
        )


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

    def _resolve_proxies(self, resolver_path: str, proxy_port: int) -> dict | None:
        """
        Get a proxy once; failures share the S3 operation's retry budget.
        """
        if resolver_path is None:
            return None
        # requests.get creates and closes a separate Session on every call.
        # Keep a fresh connection so the balancer can select another proxy.
        with requests.get(
            resolver_path,
            timeout=self._config.get("proxy_resolver", {}).get("timeout", 10),
        ) as req:
            req.raise_for_status()
            host = req.text.strip()
        logging.debug(
            "S3 proxy resolver {} returned hostname {!r}", resolver_path, host
        )
        # Try to resolve hostname to check the output
        try:
            socket.getaddrinfo(host, 0)
        except socket.gaierror as e:
            raise S3BalancerUnknownHost(
                f'{resolver_path} returned unknown hostname: "{host}"; DNS error: {e}'
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

        client = self._s3_session.client(
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
        for operation in ("CopyObject", "UploadPartCopy", "CompleteMultipartUpload"):
            client.meta.events.register(
                f"needs-retry.s3.{operation}", _repair_copy_error_response
            )
        return client


class S3ClientCachedFactory:
    """
    Thread safe S3 client factory returning cached client or creating new.
    """

    def __init__(self, s3_client_factory: S3ClientFactory) -> None:
        self._s3_client_factory = s3_client_factory
        self._cached_s3_client: S3Client | None = None
        self._lock = threading.Lock()
        self._thread_state = threading.local()

    def create_s3_client(self, cached: bool = True) -> S3Client:
        """
        Return cached S3 client. Or re-create it if needed.
        """
        # Failed discovery must not invalidate a client created by another worker.
        self._thread_state.client = None
        with self._lock:
            if not cached or self._cached_s3_client is None:
                self._cached_s3_client = self._s3_client_factory.create_s3_client()
            self._thread_state.client = self._cached_s3_client
            return self._cached_s3_client

    def reset(self) -> None:
        """
        Invalidate only the client most recently obtained by this thread.
        """
        client = getattr(self._thread_state, "client", None)
        with self._lock:
            if client is not None and self._cached_s3_client is client:
                self._cached_s3_client = None
        self._thread_state.client = None
