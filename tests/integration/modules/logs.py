"""
Logs management.
"""

import json
import logging
import os

from docker.models.containers import Container

from tests.integration.diagnostics import record_stage_failure

from .docker import copy_container_dir, get_containers
from .s3 import export_s3_data
from .typing import ContextT


def save_logs(context: ContextT) -> None:
    """
    Save logs and support materials.
    """
    logs_dir = os.path.join(context.conf["staging_dir"], "logs")
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
            _save_container_logs(container, logs_dir)
        except Exception as error:
            # One disappearing container must not hide the original failure or
            # prevent collecting evidence from the other services.
            logging.exception("Failed to save logs for %s", container.name)
            record_stage_failure(f"diagnostics:{container.name}", error)

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

    copy_container_dir(container, "/etc", base, exclude_pattern="shadow")
    copy_container_dir(container, "/var/log", base)
