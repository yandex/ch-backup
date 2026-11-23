"""Configuration passed directly to botocore and boto3 managed transfers."""

from copy import deepcopy
from typing import Any

from boto3.s3.transfer import TransferConfig
from botocore.config import Config
from botocore.exceptions import BotoCoreError

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


def build_botocore_config(storage: dict) -> Config:
    """Apply explicit SDK options over legacy settings without doing any I/O."""
    options = _module_options(storage, "botocore")
    prefix = f"{_PREFIX}.botocore"
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
    """Build a managed-transfer config using the SDK's own validation."""
    options = _module_options(storage, "transfer")
    prefix = f"{_PREFIX}.transfer"
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
