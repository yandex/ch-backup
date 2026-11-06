"""Validation of SDK proxy options before storage access."""

import traceback
from typing import Any

import pytest

from ch_backup.config import DEFAULT_CONFIG
from ch_backup.exceptions import ConfigurationError
from ch_backup.storage.engine.s3.s3_config import build_botocore_config


@pytest.mark.parametrize(
    "options",
    [
        {"proxies": {"http": 123}},
        {"proxies": {"http": False}},
        {"proxies": {"http": []}},
        {"proxies": {"http": "http://"}},
        {"proxies": {"http": "socks5://localhost:1080"}},
        {"proxies": {"http": "http://user:secret@localhost:bad"}},
        {"proxies_config": {"proxy_ca_bundle": 123}},
        {"proxies_config": {"proxy_client_cert": 123}},
        {"proxies_config": {"proxy_client_cert": ("cert.pem",)}},
        {"proxies_config": {"proxy_client_cert": ("cert.pem", 123)}},
        {"proxies_config": {"proxy_use_forwarding_for_https": "false"}},
    ],
)
def test_invalid_proxy_options_are_rejected(options: dict[str, Any]) -> None:
    storage = {**DEFAULT_CONFIG["storage"], "module_config": {"botocore": options}}
    with pytest.raises(
        ConfigurationError, match="storage.module_config.botocore"
    ) as exc:
        build_botocore_config(storage)
    assert "secret" not in "".join(traceback.format_exception(exc.value))


@pytest.mark.parametrize(
    "options",
    [
        {"proxies": None},
        {"proxies": {}},
        {"proxies": {"http": None, "https": ""}},
        {"proxies": {"http": "localhost:8080", "https": "//localhost:8080"}},
        {"proxies": {"https": "https://user:secret@[::1]:8080"}},
        {"proxies_config": None},
        {"proxies_config": {"proxy_ca_bundle": None, "proxy_client_cert": None}},
        {
            "proxies_config": {
                "proxy_ca_bundle": "ca.pem",
                "proxy_client_cert": "cert.pem",
            }
        },
        {"proxies_config": {"proxy_client_cert": ("cert.pem", "key.pem")}},
        {"proxies_config": {"proxy_use_forwarding_for_https": False}},
        {"proxies_config": {"future_sdk_option": True}},
    ],
)
def test_supported_proxy_options_are_preserved(options: dict[str, Any]) -> None:
    storage = {**DEFAULT_CONFIG["storage"], "module_config": {"botocore": options}}
    config = build_botocore_config(storage)
    for name, value in options.items():
        assert getattr(config, name) == value
