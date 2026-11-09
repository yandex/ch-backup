"""
Logs management.
"""

import json
import logging
import os
from pathlib import Path

from docker.errors import NotFound
from docker.models.containers import Container

from tests.integration.diagnostics import record_stage_failure

from .docker import copy_container_path, get_containers
from .s3 import export_s3_data
from .typing import ContextT


def _save_selected_logs(container: Container, logs_dir: str) -> None:
    """Preserve useful logs before a scenario reset, including successful runs."""
    if not container.name.startswith(("clickhouse", "broken-proxy")):
        return
    # Container IDs keep earlier scenarios when Compose reuses container names.
    # Dependent scenarios share a container and update its cumulative snapshot.
    destination = Path(logs_dir) / f"{container.name}-{container.short_id}"
    destination.mkdir(parents=True, exist_ok=True)
    if container.name.startswith("broken-proxy"):
        with (destination / "docker.log").open("wb") as output:
            for chunk in container.logs(stream=True, follow=False, timestamps=True):
                output.write(chunk)
        return
    for path in (
        "/var/log/ch-backup/ch-backup.log",
        "/var/log/ch-backup/boto.log",
        "/var/log/clickhouse-server/clickhouse-server.err.log",
    ):
        try:
            copy_container_path(container, path, str(destination))
        except NotFound:
            # A scenario need not start ClickHouse or invoke ch-backup.
            continue


def save_logs(context: ContextT, *, full: bool = True) -> None:
    """
    Always save selected logs; collect full support materials for failed steps.
    """
    logs_dir = os.path.join(context.conf["staging_dir"], "logs")
    selected_logs_dir = os.getenv("INTEGRATION_SAVED_LOGS") or os.path.join(
        logs_dir, "saved-logs"
    )
    if full:
        try:
            os.makedirs(logs_dir, exist_ok=True)
            with open(
                os.path.join(logs_dir, "session_conf.json"), "w", encoding="utf-8"
            ) as out:
                json.dump(context.conf, out, default=repr, indent=4)
        except Exception as error:
            logging.exception("Failed to save test configuration")
            record_stage_failure("diagnostics:configuration", error)

    try:
        containers = get_containers(context)
    except Exception as error:
        logging.exception("Failed to list test containers")
        record_stage_failure("diagnostics:containers", error)
        containers = []
    for container in containers:
        try:
            _save_selected_logs(container, selected_logs_dir)
        except Exception as error:
            logging.exception("Failed to save selected logs for %s", container.name)
            record_stage_failure(f"diagnostics:selected:{container.name}", error)
        if not full:
            continue
        try:
            _save_container_logs(container, logs_dir)
        except Exception as error:
            # One disappearing container must not hide the original failure or
            # prevent collecting evidence from the other services.
            logging.exception("Failed to save logs for %s", container.name)
            record_stage_failure(f"diagnostics:{container.name}", error)

    if not full:
        return
    try:
        export_s3_data(context, logs_dir)
    except Exception as error:
        logging.exception("Failed to export S3 data")
        record_stage_failure("diagnostics:s3", error)


def _save_container_logs(container: Container, logs_dir: str) -> None:
    base = os.path.join(logs_dir, container.name)
    os.makedirs(base, exist_ok=True)

    container.reload()
    with open(os.path.join(base, "state.json"), "w", encoding="utf-8") as out:
        json.dump(container.attrs["State"], out, default=repr, indent=4)

    with open(os.path.join(base, "docker.log"), "wb") as out:
        out.write(container.logs(stdout=True, stderr=True, timestamps=True))

    copy_container_path(container, "/etc", base, exclude_pattern="shadow")
    copy_container_path(container, "/var/log", base)
