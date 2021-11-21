"""
Docker Compose interface.
"""

import copy
import os
import random
import shlex
import subprocess
from typing import cast

import yaml

from . import utils
from .typing import ContextT

# Default invariant config
BASE_CONF = {
    'version': '2',
    'networks': {
        'test_net': {
            'external': {
                'name': '{network}',
            },
        },
    },
    'services': {},
}


@utils.env_stage('create', fail=True)
def build_images(context: ContextT) -> None:
    """
    Build docker images.
    """
    _call_compose(context.conf, 'build')


@utils.env_stage('start', fail=True)
def startup_containers(context: ContextT) -> None:
    """
    Start up docker containers.
    """
    _call_compose(context.conf, 'up -d')


@utils.env_stage('stop', fail=False)
def shutdown_containers(context: ContextT) -> None:
    """
    Shutdown and remove docker containers.
    """
    _call_compose(context.conf, 'down --volumes --timeout 0')


@utils.env_stage('create', fail=True)
def create_config(context: ContextT) -> None:
    """
    Generate config file and write it.
    """
    compose_conf_path = _get_config_path(context.conf)
    compose_conf = _generate_compose_config(context.conf)
    _write_config(compose_conf_path, compose_conf)


def _write_config(path: str, compose_conf: dict) -> None:
    """
    Dumps compose config into a file in Yaml format.
    """
    catalog_name = os.path.dirname(path)
    os.makedirs(catalog_name, exist_ok=True)
    temp_file_path = f'{catalog_name}/.docker-compose-conftest-{random.randint(0, 100)}.yaml'
    with open(temp_file_path, 'w', encoding='utf-8') as conf_file:
        yaml.dump(compose_conf, stream=conf_file, default_flow_style=False, indent=4)
    try:
        _validate_config(temp_file_path)
        os.rename(temp_file_path, path)
    except subprocess.CalledProcessError as err:
        raise RuntimeError(f'Unable to write config: validation failed with {err}')

    # Remove config only if validated ok.
    try:
        os.unlink(temp_file_path)
    except FileNotFoundError:
        pass


def _get_config_path(conf: dict) -> str:
    """
    Return file path to docker compose config file.
    """
    return os.path.join(conf['staging_dir'], 'docker-compose.yml')


def _validate_config(config_path: str) -> None:
    """
    Perform config validation by calling `docker-compose config`
    """
    _call_compose_on_config(config_path, '__config_test', 'config')


def _generate_compose_config(config: dict) -> dict:
    """
    Create docker compose config.
    """
    services = config['services']
    network_name = config['network_name']

    compose_conf = cast(dict, copy.deepcopy(BASE_CONF))
    # Set net name at global scope so containers will be able to reference it.
    compose_conf['networks']['test_net']['external']['name'] = network_name
    # Generate service config for each image`s instance
    # Also relative to config file location.
    for name, props in services.items():
        instances = props.get('docker_instances', 1)
        if not instances:
            continue
        # This num is also used in hostnames, later in
        # generate_service_dict()
        for num in range(1, instances + 1):
            instance_name = f'{name}{num:02d}'
            service_conf = _generate_service_config(config, instance_name, props)
            # Fill in local placeholders with own context.
            # Useful when we need to reference stuff like
            # hostname or domainname inside of the other config value.
            service_conf = utils.format_object(service_conf, **service_conf)
            compose_conf['services'].update({instance_name: service_conf})
    return compose_conf


def _generate_service_config(config: dict, instance_name: str, instance_config: dict) -> dict:
    """
    Generates a single service config based on name and
    instance config.

    All paths are relative to the location of compose-config.yaml
    (which is ./staging/compose-config.yaml by default)
    """
    staging_dir = config['staging_dir']
    network_name = config['network_name']

    volumes = [f'./images/{instance_name}/config:/config:rw']
    # Take care of port forwarding
    ports_list = []
    for port in instance_config.get('expose', {}).values():
        ports_list.append(port)

    dependency_list = []
    for dependency in instance_config.get('depends_on', {}):
        for num in range(1, config['services'][dependency].get('docker_instances', 1) + 1):
            dependency_list.append(f'{dependency}{num:02d}')

    service = {
        'build': {
            'context': '..',
            'dockerfile': f'{staging_dir}/images/{instance_name}/Dockerfile',
            'args': instance_config.get('args', []),
        },
        'image': f'{instance_name}:{network_name}',
        'hostname': instance_name,
        'domainname': network_name,
        'depends_on': dependency_list,
        # Networks. We use external anyway.
        'networks': instance_config.get('networks', ['test_net']),
        'environment': instance_config.get('environment', []),
        # Nice container name with domain name part.
        # This results, however, in a strange rdns name:
        # the domain part will end up there twice.
        # Does not affect A or AAAA, though.
        'container_name': f'{instance_name}.{network_name}',
        # Ports exposure
        'ports': ports_list,
        'volumes': volumes + instance_config.get('volumes', []),
        # https://github.com/moby/moby/issues/12080
        'tmpfs': '/var/run',
        'external_links': instance_config.get('external_links', []),
    }

    return service


def _prepare_volumes(volumes: dict, local_basedir: str) -> list:
    """
    Form a docker-compose volume list, and create endpoints.
    """
    # pylint: disable=consider-using-f-string
    volume_list = []
    for props in volumes.values():
        # "local" params are expected to be relative to docker-compose.yaml, so prepend its location.
        os.makedirs(f'{local_basedir}/{props["local"]}', exist_ok=True)
        volume_list.append('{local}:{remote}:{mode}'.format(**props))
    return volume_list


def _call_compose(conf: dict, action: str) -> None:
    conf_path = _get_config_path(conf)
    project_name = conf['network_name']

    _call_compose_on_config(conf_path, project_name, action)


def _call_compose_on_config(conf_path: str, project_name: str, action: str) -> None:
    """
    Execute docker-compose action by invoking `docker-compose`.
    """
    compose_cmd = f'docker-compose --file {conf_path} -p {project_name} {action}'
    # Note: build paths are resolved relative to config file location.
    subprocess.check_call(shlex.split(compose_cmd))
