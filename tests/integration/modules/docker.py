"""
Docker interface.
"""

import io
import os
import re
import tarfile
from typing import List, Sequence, Tuple
from urllib.parse import urlparse

import docker
from docker.models.containers import Container

from . import utils
from .typing import ContextT

DOCKER_API = docker.from_env()
ENVIRONMENT_LABEL = "ch-backup.integration.environment"


def get_containers(context: ContextT) -> Sequence[Container]:
    """
    Get containers.
    """
    containers = []
    for container in DOCKER_API.containers.list(all=True):
        networks = container.attrs["NetworkSettings"]["Networks"]
        if context.conf["network_name"] in networks:
            containers.append(container)

    return containers


def get_container(context: ContextT, prefix: str) -> Container:
    """
    Get container object by prefix.
    """
    network_name = context.conf["network_name"]
    return DOCKER_API.containers.get(f"{prefix}.{network_name}")


def get_exposed_port(container: Container, port: int) -> Tuple[str, int]:
    """
    Get pair of (host, port) for connection to exposed port.
    """
    host_url = os.getenv("DOCKER_HOST") or ""
    host = urlparse(host_url.strip()).hostname or "127.0.0.1"  # pin to IPv4 localhost

    binding = container.attrs["NetworkSettings"]["Ports"].get(f"{port}/tcp")
    if not binding:
        raise RuntimeError(f"Container {container.name} has no binding for port {port}")

    return host, binding[0]["HostPort"]


def copy_between_containers(
    container_from: Container, path_from: str, container_to: Container, path_to: str
) -> None:
    data, _ = container_from.get_archive(path_from)
    assert container_to.put_archive(path=path_to, data=data)


def put_file(container: Container, data: bytes, path: str) -> None:
    """
    Put provided bytes data to given path
    """
    tar_stream = io.BytesIO()
    with tarfile.open(fileobj=tar_stream, mode="w") as tar:
        tar_file = tarfile.TarInfo(name=path)
        tar_file.size = len(data)
        tar.addfile(tar_file, io.BytesIO(data))

    container.put_archive(path="/", data=tar_stream.getvalue())


def copy_container_dir(
    container: Container,
    container_dir: str,
    local_dir: str,
    exclude_pattern: str = None,
) -> None:
    """
    Save docker directory.
    """
    archive, _ = container.get_archive(container_dir)

    buffer = io.BytesIO()
    for chunk in archive:
        buffer.write(chunk)
    buffer.seek(0)

    with tarfile.open(mode="r", fileobj=buffer) as tar:
        members: List[tarfile.TarInfo] = []
        for member in tar.getmembers():
            if member.type == tarfile.SYMTYPE:
                continue
            if exclude_pattern and re.search(exclude_pattern, member.name):
                continue
            members.append(member)
        tar.extractall(path=local_dir, members=members)


def get_file_size(container: Container, path: str) -> int:
    """
    Return size of the specified file inside the container.
    """
    output = container.exec_run(f'stat --format "%s" "{path}"')
    return int(output.decode())


@utils.env_stage("create", fail=True)
def create_network(context: ContextT) -> None:
    """
    Create docker network specified in the config.
    """
    conf = context.conf
    net_name = conf["network_name"]
    try:
        network = DOCKER_API.networks.get(net_name)
    except docker.errors.NotFound:
        network = None
    if network is not None:
        if (network.attrs.get("Labels") or {}).get(ENVIRONMENT_LABEL) != net_name:
            raise RuntimeError(f"Network {net_name} belongs to another environment")
        return
    net_opts = {
        "com.docker.network.bridge.enable_ip_masquerade": "true",
        "com.docker.network.bridge.enable_icc": "true",
    }
    DOCKER_API.networks.create(
        net_name,
        options=net_opts,
        enable_ipv6=False,
        labels={ENVIRONMENT_LABEL: net_name},
    )


@utils.env_stage("stop", fail=False)
def shutdown_network(context: ContextT) -> None:
    """
    Stop docker network(s).
    """
    name = context.conf["network_name"]
    try:
        network = DOCKER_API.networks.get(name)
    except docker.errors.NotFound:
        return
    if (network.attrs.get("Labels") or {}).get(ENVIRONMENT_LABEL) != name:
        raise RuntimeError(f"Refusing to remove unowned network {name}")
    network.remove()
