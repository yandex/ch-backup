"""
Logs management.
"""

import io
import json
import os
import tarfile

from .docker import DOCKER_API


def save_logs(context):
    """
    Save logs and support materials.
    """
    network_name = context.conf['network_name']
    logs_dir = os.path.join(context.conf['staging_dir'], 'logs')

    for container in DOCKER_API.containers.list():
        networks = container.attrs['NetworkSettings']['Networks']
        if network_name in networks:
            _save_container_logs(container, logs_dir)

    with open(os.path.join(logs_dir, 'session_conf.json'), 'w') as out:
        json.dump(context.conf, out, default=repr, indent=4)


def _save_container_logs(container, logs_dir):
    base = os.path.join(logs_dir, container.name)
    os.makedirs(base, exist_ok=True)
    with open(os.path.join(base, 'docker.log'), 'wb') as out:
        out.write(container.logs(stdout=True, stderr=True, timestamps=True))

    var_log_archive, _ = container.get_archive('/var/log')

    raw_archive = io.BytesIO(var_log_archive.read())

    tar = tarfile.open(mode='r', fileobj=raw_archive)

    tar.extractall(path=base)
