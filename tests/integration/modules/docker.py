"""
Docker interface.
"""

import io
import os
import random
import tarfile
from distutils import dir_util
from typing import Sequence, Tuple
from urllib.parse import urlparse

import docker
from docker.models.containers import Container

from . import utils
from .typing import ContextT

DOCKER_API = docker.from_env()


def get_containers(context: ContextT) -> Sequence[Container]:
    """
    Get containers.
    """
    containers = []
    for container in DOCKER_API.containers.list():
        networks = container.attrs['NetworkSettings']['Networks']
        if context.conf['network_name'] in networks:
            containers.append(container)

    return containers


def get_container(context: ContextT, prefix: str) -> Container:
    """
    Get container object by prefix.
    """
    network_name = context.conf['network_name']
    return DOCKER_API.containers.get(f'{prefix}.{network_name}')


def get_exposed_port(container: Container, port: int) -> Tuple[str, int]:
    """
    Get pair of (host, port) for connection to exposed port.
    """
    host_url = os.getenv('DOCKER_HOST') or ''
    host = urlparse(host_url.strip()).hostname or 'localhost'

    binding = container.attrs['NetworkSettings']['Ports'].get(f'{port}/tcp')
    if not binding:
        raise RuntimeError(f'Container {container.name} has no binding for port {port}')

    return host, binding[0]['HostPort']


def put_file(container: Container, data: bytes, path: str) -> None:
    """
    Put provided bytes data to given path
    """
    tarstream = io.BytesIO()
    tar_data = tarfile.open(fileobj=tarstream, mode='w')
    tarinfo = tarfile.TarInfo(name=path)
    tarinfo.size = len(data)
    tar_data.addfile(tarinfo, io.BytesIO(data))
    tar_data.close()

    container.put_archive(path='/', data=tarstream.getvalue())


def copy_container_dir(container: Container, container_dir: str, local_dir: str) -> None:
    """
    Save docker directory.
    """
    archive, _ = container.get_archive(container_dir)

    buffer = io.BytesIO()
    for chunk in archive:
        buffer.write(chunk)
    buffer.seek(0)

    tar = tarfile.open(mode='r', fileobj=buffer)
    tar.extractall(path=local_dir)


def get_file_size(container: Container, path: str) -> int:
    """
    Return size of the specified file inside the container.
    """
    output = container.exec_run(f'stat --format "%s" "{path}"')
    return int(output.decode())


@utils.env_stage('create', fail=True)
def prep_images(context: ContextT) -> None:
    """
    Prepare images.
    """
    images_dir = context.conf['images_dir']
    staging_dir = context.conf['staging_dir']
    dir_util.copy_tree(images_dir, f'{staging_dir}/images', update=True)


@utils.env_stage('create', fail=True)
def create_network(context: ContextT) -> None:
    """
    Create docker network specified in the config.
    """
    conf = context.conf
    net_name = conf['network_name']
    # Unfortunately docker is retarded and not able to create
    # ipv6-only network (see https://github.com/docker/libnetwork/issues/1192)
    # Do not create new network if there is an another net with the same name.
    if DOCKER_API.networks.list(names=f'^{net_name}$'):
        return
    ip_subnet_pool = docker.types.IPAMConfig(pool_configs=[
        docker.types.IPAMPool(subnet=_generate_ipv4_subnet()),
        docker.types.IPAMPool(subnet=_generate_ipv6_subnet()),
    ])
    net_opts = {
        'com.docker.network.bridge.enable_ip_masquerade': 'true',
        'com.docker.network.bridge.enable_icc': 'true',
        'com.docker.network.bridge.name': net_name,
    }
    DOCKER_API.networks.create(net_name, options=net_opts, enable_ipv6=True, ipam=ip_subnet_pool)


@utils.env_stage('stop', fail=False)
def shutdown_network(context: ContextT) -> None:
    """
    Stop docker network(s).
    """
    nets = DOCKER_API.networks.list(names=context.conf['network_name'])
    for net in nets:
        net.remove()


def _generate_ipv6_subnet() -> str:
    """
    Generates a random IPv6 address in the provided subnet.
    """
    random_part = ':'.join([f'{random.randint(0, 16**4):x}' for _ in range(3)])
    return f'fd00:dead:beef:{random_part}::/96'


def _generate_ipv4_subnet() -> str:
    """
    Generates a random IPv4 address in the provided subnet.
    """
    random_part = '.'.join([str(random.randint(0, 255)) for _ in range(2)])
    return f'10.{random_part}.0/24'
