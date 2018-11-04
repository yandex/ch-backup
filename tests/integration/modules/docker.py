"""
Docker interface.
"""

import io
import os
import random
import subprocess
import tarfile
from distutils import dir_util

import docker

from . import utils

DOCKER_API = docker.from_env()


def get_containers(context):
    """
    Get containers.
    """
    containers = []
    for container in DOCKER_API.containers.list():
        networks = container.attrs['NetworkSettings']['Networks']
        if context.conf['network_name'] in networks:
            containers.append(container)

    return containers


def get_container(context, prefix):
    """
    Get container object by prefix.
    """
    return DOCKER_API.containers.get(
        '%s.%s' % (prefix, context.conf['network_name']))


def get_exposed_port(container, port):
    """
    Get pair of (host, port) for connection to exposed port.
    """
    machine_name = os.getenv('DOCKER_MACHINE_NAME')
    if machine_name:
        host = subprocess.check_output(
            ['docker-machine', 'ip', machine_name]).decode('utf-8').rstrip()
    else:
        host = 'localhost'

    binding = container.attrs['NetworkSettings']['Ports'].get('%d/tcp' % port)

    return (host, binding[0]['HostPort']) if binding else None


def put_file(container, data, path):
    """
    Put provided bytes data to given path
    """
    tarstream = io.BytesIO()
    tar_data = tarfile.open(fileobj=tarstream, mode='w')
    tarinfo = tarfile.TarInfo(name=path)
    tarinfo.size = len(data)
    tar_data.addfile(tarinfo, io.BytesIO(data))
    tar_data.close()

    return container.put_archive(path='/', data=tarstream.getvalue())


def copy_container_dir(container, container_dir, local_dir):
    """
    Save docker directory.
    """
    archive, _ = container.get_archive(container_dir)
    raw_archive = io.BytesIO(archive.read())
    tar = tarfile.open(mode='r', fileobj=raw_archive)
    tar.extractall(path=local_dir)


def generate_ipv6(subnet=None):
    """
    Generates a random IPv6 address in the provided subnet.
    """
    if subnet is None:
        subnet = 'fd00:dead:beef:%s::/96'
    random_part = ':'.join(['%x' % random.randint(0, 16**4) for _ in range(3)])
    return subnet % random_part


def generate_ipv4(subnet=None):
    """
    Generates a random IPv4 address in the provided subnet.
    """
    if subnet is None:
        subnet = '10.%s.0/24'
    random_part = '.'.join(['%d' % random.randint(0, 255) for _ in range(2)])
    return subnet % random_part


@utils.env_stage('create', fail=True)
def prep_images(context):
    """
    Prepare images.
    """
    images_dir = context.conf['images_dir']
    staging_dir = context.conf['staging_dir']
    dir_util.copy_tree(
        images_dir, '{0}/images'.format(staging_dir), update=True)


@utils.env_stage('create', fail=True)
def prep_network(context):
    """
    Creates ipv6-enabled docker network with random name and address space
    """
    conf = context.conf
    # Unfortunately docker is retarded and not able to create
    # ipv6-only network (see https://github.com/docker/libnetwork/issues/1192)
    # Do not create new network if there is an another net with the same name.
    if DOCKER_API.networks.list(names='^%s$' % conf['network_name']):
        return
    ip_subnet_pool = docker.types.IPAMConfig(pool_configs=[
        docker.types.IPAMPool(
            subnet=generate_ipv4(conf.get('docker_ip4_subnet'))),
        docker.types.IPAMPool(
            subnet=generate_ipv6(conf.get('docker_ip6_subnet'))),
    ])
    net_name = conf['network_name']
    net_opts = {
        'com.docker.network.bridge.enable_ip_masquerade': 'true',
        'com.docker.network.bridge.enable_icc': 'true',
        'com.docker.network.bridge.name': net_name,
    }
    DOCKER_API.networks.create(
        net_name, options=net_opts, enable_ipv6=True, ipam=ip_subnet_pool)


@utils.env_stage('stop', fail=False)
def shutdown_network(context):
    """
    Stop docker network(s)
    """
    nets = DOCKER_API.networks.list(names=context.conf['network_name'])
    for net in nets:
        net.remove()
