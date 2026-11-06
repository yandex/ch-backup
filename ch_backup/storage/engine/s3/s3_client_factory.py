"""
S3 client factory.
"""

import socket
import threading
from copy import deepcopy
from sys import float_info
from time import monotonic
from typing import Any
from urllib.parse import urlsplit

import boto3
import requests
from botocore.config import Config

from ch_backup import logging
from ch_backup.config import DEFAULT_CONFIG
from ch_backup.exceptions import ConfigurationError
from ch_backup.type_hints.boto3.s3 import S3Client

_MAX_CACHED_FACTORIES = 16

# ProcessExecPool uses spawn, so each worker owns its sessions and connections.
_FACTORIES: list[tuple[dict, "S3ClientCachedFactory"]] = []
_FACTORIES_LOCK = threading.Lock()


def _validate_client_ttl(client_ttl: float) -> float:
    if (
        isinstance(client_ttl, bool)
        or not isinstance(client_ttl, (int, float))
        or not 0 < client_ttl <= float_info.max
    ):
        raise ConfigurationError(
            "storage.client_ttl must be a finite positive number of seconds"
        )
    return client_ttl


def _mask_url_credentials(url: str) -> str:
    """Replace the entire userinfo without matching credentials elsewhere."""
    parsed = urlsplit(url)
    if "@" not in parsed.netloc:
        return url
    host = parsed.netloc.rsplit("@", 1)[1]
    return parsed._replace(netloc=f"***:***@{host}").geturl()


class _ProxyResolverSession(requests.Session):
    """Keep URL credentials out of transport errors and response URLs."""

    def send(
        self, request: requests.PreparedRequest, **kwargs: Any
    ) -> requests.Response:
        # Requests has already applied URL/netrc auth to the headers. Mask the
        # userinfo, including on redirects, before errors can expose it. Retain
        # its presence: older Requests uses netloc when looking up netrc auth.
        if request.url:
            request.url = _mask_url_credentials(request.url)
        return super().send(request, **kwargs)


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

    def _resolve_proxies(
        self, resolver_path: str | None, proxy_port: int
    ) -> dict | None:
        """
        Get a proxy once; failures share the S3 operation's retry budget.
        """
        if resolver_path is None:
            return None
        error = "storage.proxy_resolver.uri: invalid resolver request configuration"
        if not isinstance(resolver_path, str):
            raise ConfigurationError(error)
        try:
            # Keep a fresh connection so the balancer can select another proxy.
            with _ProxyResolverSession() as session:
                with session.get(
                    resolver_path,
                    timeout=self._config.get("proxy_resolver", {}).get("timeout", 10),
                ) as req:
                    req.raise_for_status()
                    host = req.text.strip()
        except ValueError:
            # URL parsing and auth preparation errors may contain credentials.
            # HTTP/network/TLS errors keep their types and the existing retries.
            raise ConfigurationError(error) from None
        safe_resolver_path = _mask_url_credentials(resolver_path.lstrip())
        logging.debug(
            "S3 proxy resolver {} returned hostname {!r}", safe_resolver_path, host
        )
        # Try to resolve hostname to check the output
        try:
            socket.getaddrinfo(host, 0)
        except socket.gaierror as e:
            raise S3BalancerUnknownHost(
                f'{safe_resolver_path} returned unknown hostname: "{host}"; DNS error: {e}'
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


class S3ClientCachedFactory:
    """
    Thread safe S3 client factory returning cached client or creating new.
    """

    def __init__(
        self,
        s3_client_factory: S3ClientFactory,
        *,
        client_ttl: float = DEFAULT_CONFIG["storage"]["client_ttl"],
    ) -> None:
        self._client_ttl = _validate_client_ttl(client_ttl)
        self._next_client_refresh_at = 0.0
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
            if (
                not cached
                or self._cached_s3_client is None
                or monotonic() >= self._next_client_refresh_at
            ):
                # Schedule the next refresh even if this attempt fails.
                self._next_client_refresh_at = monotonic() + self._client_ttl
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


def get_cached_s3_client_factory(config: dict) -> S3ClientCachedFactory:
    """Return the process-local factory shared by engines with an equal config."""
    client_ttl = _validate_client_ttl(config["client_ttl"])
    with _FACTORIES_LOCK:
        for index, (cached_config, factory) in enumerate(_FACTORIES):
            if config == cached_config:
                # Move this factory to the end to mark it as most recently used.
                _FACTORIES.append(_FACTORIES.pop(index))
                return factory
        # Keep the cache key and factory independent of caller mutations.
        snapshot = deepcopy(config)
        factory = S3ClientCachedFactory(
            S3ClientFactory(snapshot),
            client_ttl=client_ttl,
        )
        _FACTORIES.append((snapshot, factory))
        # Remove older entries from the front, keeping the most recently used ones.
        del _FACTORIES[:-_MAX_CACHED_FACTORIES]
        return factory
