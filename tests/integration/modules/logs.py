"""
Logs management.
"""

import json
import logging
import os

from docker.models.containers import Container

from .docker import copy_container_dir, get_containers
from .minio import export_s3_data


def save_logs(context) -> None:
    """
    Save logs and support materials.
    """
    try:
        logs_dir = os.path.join(context.conf['staging_dir'], 'logs')

        for container in get_containers(context):
            _save_container_logs(container, logs_dir)

        with open(os.path.join(logs_dir, 'session_conf.json'), 'w') as out:
            json.dump(context.conf, out, default=repr, indent=4)

        export_s3_data(context, logs_dir)
    except Exception:
        logging.exception('Failed to save logs')
        raise


def _save_container_logs(container: Container, logs_dir: str) -> None:
    base = os.path.join(logs_dir, container.name)
    os.makedirs(base, exist_ok=True)
    with open(os.path.join(base, 'docker.log'), 'wb') as out:
        out.write(container.logs(stdout=True, stderr=True, timestamps=True))

    copy_container_dir(container, '/var/log', base)
