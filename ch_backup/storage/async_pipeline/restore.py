"""Validate bounded local restore pipeline settings."""

import os

from ch_backup.config import DEFAULT_CONFIG, Config
from ch_backup.exceptions import ConfigurationError


def local_restore_options(config: Config | dict) -> dict:
    """Resolve local restore options before starting downloads or changing tables."""
    restore = config["restore"]
    configured = restore.get("local_data", {})
    if not isinstance(configured, dict):
        raise ConfigurationError("restore.local_data must be a mapping")
    options = {**DEFAULT_CONFIG["restore"]["local_data"], **configured}
    if options["decrypt_threads"] == "auto":
        get_affinity = getattr(os, "sched_getaffinity", None)
        try:
            available = len(get_affinity(0)) if get_affinity else None
        except (OSError, NotImplementedError):
            available = None
        options["decrypt_threads"] = available or os.cpu_count() or 1

    for name in ("encryption_queue_size", "filesystem_queue_size", "decrypt_threads"):
        value = options[name]
        if isinstance(value, bool) or not isinstance(value, int) or value < 1:
            raise ConfigurationError(
                f"restore.local_data.{name} must be a positive integer"
            )
    value = options["download_range_size"]
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ConfigurationError(
            "restore.local_data.download_range_size must be a nonnegative integer"
        )
    for name in ("direct_chunks", "direct_write"):
        if not isinstance(options[name], bool):
            raise ConfigurationError(f"restore.local_data.{name} must be true or false")
    if not isinstance(restore.get("largest_parts_first", True), bool):
        raise ConfigurationError("restore.largest_parts_first must be true or false")
    return options
