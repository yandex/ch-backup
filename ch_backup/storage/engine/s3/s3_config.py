"""Configuration passed directly to botocore and boto3 managed transfers."""

from copy import deepcopy
from math import isfinite
from typing import Any

from boto3.s3.transfer import TransferConfig
from botocore.config import Config
from botocore.exceptions import BotoCoreError
from urllib3.exceptions import LocationParseError
from urllib3.util import parse_url

from ch_backup.exceptions import ConfigurationError

_PREFIX = "storage.module_config"


def _mapping(value: Any, path: str) -> dict:
    if not isinstance(value, dict) or any(not isinstance(key, str) for key in value):
        raise ConfigurationError(f"{path} must be a mapping with string keys")
    return value


def _module_options(storage: dict, module: str) -> dict:
    modules = _mapping(storage.get("module_config", {}), _PREFIX)
    if modules.keys() - {"botocore", "transfer"}:
        raise ConfigurationError(
            f"{_PREFIX} only supports botocore and transfer sections"
        )
    for name, value in modules.items():
        _mapping(value, f"{_PREFIX}.{name}")
    # Botocore normalizes nested dictionaries while constructing a client.
    return deepcopy(modules.get(module, {}))


def _number(value: Any, path: str, *, integer: bool, minimum: int = 1) -> None:
    valid = isinstance(value, int) and not isinstance(value, bool)
    if not integer and isinstance(value, float):
        valid = isfinite(value)
    if not valid or value < minimum:
        kind = "an integer" if integer else "a finite number"
        raise ConfigurationError(f"{path} must be {kind} of at least {minimum}")


def _validate_proxy_urls(proxies: dict | None) -> None:
    for proxy_url in (proxies or {}).values():
        # Botocore accepts null and an empty string to disable a proxy.
        if proxy_url is None or proxy_url == "":
            continue
        error = f"{_PREFIX}.botocore.proxies: invalid proxy URL"
        if not isinstance(proxy_url, str):
            raise ConfigurationError(error)
        if "://" in proxy_url and not proxy_url.startswith(("http://", "https://")):
            raise ConfigurationError(error)
        # Match botocore's handling of proxy addresses without a scheme.
        if proxy_url.startswith("//"):
            proxy_url = f"http:{proxy_url}"
        elif not proxy_url.startswith(("http:", "https:")):
            proxy_url = f"http://{proxy_url}"
        try:
            parsed = parse_url(proxy_url)
        except LocationParseError:
            # SDK request logging would expose credentials in the parser error.
            raise ConfigurationError(error) from None
        if not parsed.host or any(char.isspace() for char in parsed.host):
            raise ConfigurationError(error)


def _validate_proxy_settings(settings: dict | None) -> None:
    if settings is None:
        return
    prefix = f"{_PREFIX}.botocore.proxies_config"
    ca_bundle = settings.get("proxy_ca_bundle")
    if ca_bundle is not None and not isinstance(ca_bundle, str):
        raise ConfigurationError(f"{prefix}.proxy_ca_bundle must be a string or null")
    client_cert = settings.get("proxy_client_cert")
    if client_cert is not None and not isinstance(client_cert, str):
        if not (
            isinstance(client_cert, tuple)
            and len(client_cert) == 2
            and all(isinstance(path, str) for path in client_cert)
        ):
            raise ConfigurationError(
                f"{prefix}.proxy_client_cert must be a string, a pair of strings or null"
            )
    forwarding = settings.get("proxy_use_forwarding_for_https")
    if forwarding is not None and not isinstance(forwarding, bool):
        raise ConfigurationError(
            f"{prefix}.proxy_use_forwarding_for_https must be a boolean or null"
        )


def build_botocore_config(storage: dict) -> Config:
    """Apply explicit SDK options over legacy settings without doing any I/O."""
    options = _module_options(storage, "botocore")
    prefix = f"{_PREFIX}.botocore"
    for name in ("s3", "retries", "proxies", "proxies_config"):
        if name in options and options[name] is not None:
            _mapping(options[name], f"{prefix}.{name}")
    _validate_proxy_urls(options.get("proxies"))
    _validate_proxy_settings(options.get("proxies_config"))
    for name in ("connect_timeout", "read_timeout"):
        if name in options and options[name] is not None:
            value = options[name]
            _number(value, f"{prefix}.{name}", integer=False, minimum=0)
            if value == 0:
                raise ConfigurationError(f"{prefix}.{name} must be greater than 0")
    if "max_pool_connections" in options:
        _number(
            options["max_pool_connections"],
            f"{prefix}.max_pool_connections",
            integer=True,
        )
    retries = options.get("retries")
    if isinstance(retries, dict):
        for name, minimum in (("max_attempts", 0), ("total_max_attempts", 1)):
            if name in retries:
                _number(
                    retries[name],
                    f"{prefix}.retries.{name}",
                    integer=True,
                    minimum=minimum,
                )

    legacy = storage["boto_config"]
    defaults: dict[str, Any] = {
        "region_name": legacy["region_name"],
        "s3": {
            "addressing_style": legacy["addressing_style"],
            "region_name": legacy["region_name"],
        },
        "retries": {"max_attempts": 0},
    }
    if isinstance(options.get("s3"), dict):
        options["s3"] = {**defaults["s3"], **options["s3"]}
    # Replace explicit retries wholesale: retaining max_attempts=0 would
    # silently disable retries when the user specifies only mode=standard.
    defaults.update(options)
    try:
        return Config(**defaults)
    except (TypeError, ValueError, BotoCoreError) as error:
        # SDK errors may include option values, such as authenticated proxy URLs.
        raise ConfigurationError(
            f"{prefix}: invalid botocore Config parameters ({type(error).__name__})"
        ) from None


def build_transfer_config(storage: dict) -> TransferConfig:
    """Build a managed-transfer config, filling only SDK validation gaps."""
    options = _module_options(storage, "transfer")
    prefix = f"{_PREFIX}.transfer"
    for name in (
        "multipart_threshold",
        "multipart_chunksize",
        "max_concurrency",
        "num_download_attempts",
        "max_io_queue",
        "io_chunksize",
        "max_bandwidth",
    ):
        if name in options and not (name == "max_bandwidth" and options[name] is None):
            _number(options[name], f"{prefix}.{name}", integer=True)
    if "use_threads" in options and not isinstance(options["use_threads"], bool):
        raise ConfigurationError(f"{prefix}.use_threads must be a boolean")
    try:
        return TransferConfig(**options)
    except (TypeError, ValueError) as error:
        raise ConfigurationError(
            f"{prefix}: invalid boto3 TransferConfig parameters ({type(error).__name__})"
        ) from None


def validate_s3_module_config(storage: dict) -> None:
    """Reject invalid SDK options before creating contexts, locks or clients."""
    build_botocore_config(storage)
    build_transfer_config(storage)
