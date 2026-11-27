"""Validate bounded local restore pipeline settings."""

import os
from pathlib import Path

from ch_backup.config import DEFAULT_CONFIG, Config
from ch_backup.exceptions import ConfigurationError


def _cpu_quota(directory: Path, unified: bool) -> int | None:
    """Return the number of whole CPUs allowed by one cgroup, with a minimum of one."""
    try:
        if unified:
            quota, period = (directory / "cpu.max").read_text(encoding="ascii").split()
        else:
            quota = (directory / "cpu.cfs_quota_us").read_text(encoding="ascii")
            period = (directory / "cpu.cfs_period_us").read_text(encoding="ascii")
        limit, interval = int(quota), int(period)
        if limit > 0 and interval > 0:
            return max(1, limit // interval)
    except (OSError, ValueError):
        pass
    return None


def _available_cpus() -> int:
    """Combine process affinity with CPU quotas visible in the cgroup hierarchy."""
    available = os.cpu_count() or 1
    affinity = getattr(os, "sched_getaffinity", None)
    if affinity is not None:
        try:
            available = len(affinity(0)) or available
        except (OSError, NotImplementedError):
            pass
    try:
        groups = os.fsdecode(Path("/proc/self/cgroup").read_bytes()).splitlines()
        mounts = os.fsdecode(Path("/proc/self/mountinfo").read_bytes()).splitlines()
    except OSError:
        return available
    for group in groups:
        _, controllers, group_path = group.split(":", 2)
        if controllers and "cpu" not in controllers.split(","):
            continue
        for mount in mounts:
            location, filesystem = mount.split(" - ", 1)
            kind, _, options = filesystem.split()[:3]
            if controllers:
                if kind != "cgroup" or "cpu" not in options.split(","):
                    continue
            elif kind != "cgroup2":
                continue
            root, target = (
                Path(field.replace(r"\040", " ")) for field in location.split()[3:5]
            )
            path = Path(group_path)
            relative = (
                path.relative_to(root)
                if path.is_relative_to(root)
                else path.relative_to("/")
            )
            directory = target if ".." in relative.parts else target / relative
            while True:
                quota = _cpu_quota(directory, not controllers)
                if quota is not None:
                    available = min(available, quota)
                if directory == target:
                    break
                directory = directory.parent
    return available


def local_restore_options(config: Config | dict) -> dict:
    """Resolve local restore options before starting downloads or changing tables."""
    restore = config["restore"]
    if not isinstance(restore, dict):
        raise ConfigurationError("restore must be a mapping")
    configured = restore.get("local_data", {})
    if not isinstance(configured, dict):
        raise ConfigurationError("restore.local_data must be a mapping")
    options = {**DEFAULT_CONFIG["restore"]["local_data"], **configured}
    if options["decrypt_threads"] == "auto":
        archives = max(1, config["multiprocessing"]["workers"])
        options["decrypt_threads"] = max(1, _available_cpus() // archives)

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
