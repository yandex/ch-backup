"""
Docker Compose interface.
"""

import os
import shlex
import shutil
import subprocess

import yaml

from . import utils
from .typing import ContextT

COMPOSE_UP_DOWN_TIMEOUT = 30


@utils.env_stage("create", fail=True)
def build_images(context: ContextT) -> None:
    """
    Build docker images.
    """
    _call_compose(context.conf, command="build")


@utils.env_stage("start", fail=True)
def startup_containers(context: ContextT) -> None:
    """
    Start up docker containers.
    """
    _call_compose(
        context.conf,
        project_name=_project_name(context.conf),
        command=f"up -d --timeout {COMPOSE_UP_DOWN_TIMEOUT}",
    )


@utils.env_stage("stop", fail=False)
def shutdown_containers(context: ContextT) -> None:
    """
    Shutdown and remove docker containers.
    """
    project_name = _project_name(context.conf)
    _call_compose(context.conf, project_name=project_name, command="kill")
    _call_compose(
        context.conf,
        project_name=project_name,
        command=f"down --volumes --timeout {COMPOSE_UP_DOWN_TIMEOUT}",
    )


@utils.env_stage("create", fail=True)
def create_config(context: ContextT) -> None:
    """
    Generate docker-compose.yml file.
    """
    compose_config = _generate_compose_config(context)
    _write_compose_config(context, compose_config)


def _generate_compose_config(context: ContextT) -> dict:
    """
    Create docker compose config.
    """
    config = context.conf
    services = config["services"]
    network_name = config["network_name"]

    compose_conf: dict = {
        "networks": {
            "test_net": {
                "name": network_name,
                "external": True,
            },
        },
        "services": {},
    }

    # Generate service config for each image`s instance
    # Also relative to config file location.
    for name, props in services.items():
        instances = props.get("docker_instances", 1)
        if not instances:
            continue
        # This num is also used in hostnames, later in
        # generate_service_dict()
        for num in range(1, instances + 1):
            instance_name = f"{name}{num:02d}"
            service_conf = _generate_service_config(config, instance_name, props)
            # Fill in local placeholders with own context.
            # Useful when we need to reference stuff like
            # hostname or domainname inside of the other config value.
            service_conf = utils.format_object(service_conf, **service_conf)
            compose_conf["services"].update({instance_name: service_conf})

    return compose_conf


def _generate_service_config(
    config: dict, instance_name: str, instance_config: dict
) -> dict:
    """
    Generates a single service config based on name and
    instance config.

    All paths are relative to the location of compose-config.yaml
    (which is ./staging/compose-config.yaml by default)
    """
    staging_dir = config["staging_dir"]
    network_name = config["network_name"]

    volumes = [
        "../:/code:rw",
        "../ch_backup:/var/tmp/ch-backup/ch_backup:rw",
    ]
    if os.path.exists(os.path.join(staging_dir, f"images/{instance_name}/config")):
        volumes.append(f"./images/{instance_name}/config:/config:rw")

    # Take care of port forwarding
    ports_list = []
    for port in instance_config.get("expose", {}).values():
        ports_list.append(port)

    dependency_list = []
    for dependency in instance_config.get("depends_on", {}):
        for num in range(
            1, config["services"][dependency].get("docker_instances", 1) + 1
        ):
            dependency_list.append(f"{dependency}{num:02d}")

    service = {
        "build": {
            "context": "..",
            "dockerfile": f"{staging_dir}/images/{instance_name}/Dockerfile",
            "args": instance_config.get("args", []),
        },
        "image": f"{instance_name}:{network_name}",
        "hostname": instance_name,
        "domainname": network_name,
        "depends_on": dependency_list,
        # Networks. We use external anyway.
        "networks": instance_config.get("networks", ["test_net"]),
        "environment": instance_config.get("environment", []),
        # Nice container name with domain name part.
        # This results, however, in a strange rdns name:
        # the domain part will end up there twice.
        # Does not affect A or AAAA, though.
        "container_name": f"{instance_name}.{network_name}",
        # Ports exposure
        "ports": ports_list,
        "volumes": volumes + instance_config.get("volumes", []),
        # https://github.com/moby/moby/issues/12080
        "tmpfs": "/var/run",
        "external_links": instance_config.get("external_links", []),
    }

    return service


def _write_compose_config(context: ContextT, compose_config: dict) -> None:
    """
    Dumps compose config into a file in Yaml format.
    """
    config_path = _config_path(context.conf)

    os.makedirs(os.path.dirname(config_path), exist_ok=True)
    with open(config_path, "w", encoding="utf-8") as file:
        yaml.dump(compose_config, stream=file, default_flow_style=False, indent=4)

    try:
        _validate_config(context)
    except subprocess.CalledProcessError as err:
        raise RuntimeError(f"Config validation failed with: {err}")


def _validate_config(context: ContextT) -> None:
    """
    Perform config validation by calling `docker-compose config`
    """
    _call_compose(context.conf, command="config")


def _call_compose(conf: dict, *, command: str, project_name: str = None) -> None:
    """
    Execute Docker Compose command.
    """
    docker_compose = "docker-compose"
    if shutil.which(docker_compose) is None:
        docker_compose = "docker compose"

    shell_command = f"{docker_compose} --file {_config_path(conf)}"
    if project_name:
        shell_command += f" -p {project_name}"
    shell_command += f" {command}"

    # Note: build paths are resolved relative to config file location.
    subprocess.check_call(shlex.split(shell_command))


def _config_path(config: dict) -> str:
    """
    Return file path to docker compose config file.
    """
    return os.path.join(config["staging_dir"], "docker-compose.yml")


def _project_name(conf):
    return conf["network_name"]
