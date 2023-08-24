#!/usr/bin/env python3
"""
Manage test environment.
"""

import argparse
import logging
import pickle
from types import SimpleNamespace

from tests.integration import configuration
from tests.integration.modules import compose, docker, minio, templates

SESSION_STATE_CONF = ".session_conf.sav"
STAGES = {
    "create": [
        compose.create_config,
        templates.render_docker_configs,
        compose.build_images,
    ],
    "start": [
        docker.create_network,
        compose.startup_containers,
        minio.configure_s3_credentials,
        minio.create_s3_buckets,
    ],
    "update": [
        compose.create_config,
        templates.render_docker_configs,
    ],
    "restart": [
        compose.shutdown_containers,
        docker.create_network,
        compose.startup_containers,
    ],
    "stop": [
        compose.shutdown_containers,
        docker.shutdown_network,
    ],
}


def create(context):
    """
    Create test environment.
    """
    _run_stage("create", context)

    with open(context.state_file, "wb") as session_conf:
        pickle.dump(context.conf, session_conf)


def load(context):
    """
    Load test environment.
    """
    _init_context(context)


def start(context):
    """
    Start test environment runtime.
    """
    _run_stage("start", context)


def update(context):
    """
    Update test environment configuration.
    """
    _run_stage("update", context)


def restart(context):
    """
    Restart test environment runtime.
    """
    _run_stage("restart", context)


def stop(context):
    """
    Stop test environment runtime.
    """
    _run_stage("stop", context)


def _run_stage(stage, context):
    """
    Run stage steps.
    """
    assert stage in STAGES, stage + " not implemented"

    _init_context(context)

    for step in STAGES[stage]:
        step(context)


def _init_context(context):
    """
    Initialize context.
    """
    if getattr(context, "initialized", False):
        return

    if not hasattr(context, "state_file"):
        context.state_file = SESSION_STATE_CONF

    try:
        with open(context.state_file, "rb") as session_conf:
            context.conf = pickle.load(session_conf)
    except FileNotFoundError:
        logging.info("creating new test config")
        context.conf = configuration.create()

    if not hasattr(context, "feature_flags"):
        context.feature_flags = set(context.conf["default_feature_flags"])

    context.initialized = True


def cli_main():
    """
    CLI entry.
    """
    commands = {
        "create": create,
        "start": start,
        "stop": stop,
    }

    logging.basicConfig(
        format="%(asctime)s [%(levelname)s]:\t%(message)s",
        level=logging.INFO,
    )

    args = _parse_args(commands)

    context = SimpleNamespace(state_file=args.state_file)

    commands[args.command](context)


def _parse_args(commands):
    """
    Parse command-line arguments.
    """
    arg = argparse.ArgumentParser(
        description="""Testing environment initializer script"""
    )
    arg.add_argument("command", choices=list(commands), help="command to perform")
    arg.add_argument(
        "-s",
        "--state-file",
        dest="state_file",
        type=str,
        metavar="<path>",
        default=SESSION_STATE_CONF,
        help="path to state file (pickle dump)",
    )
    return arg.parse_args()


if __name__ == "__main__":
    cli_main()
