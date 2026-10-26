"""Run fault injection through the CLI inside a test container."""

import shlex

from .docker import get_container

FAULT_MODULE = "tests.integration.modules.fault_injection"


def run_fault(context, node, fault, *parameters):
    """Execute a fault and expose its command, output and exit code to steps."""
    arguments = [
        "python3",
        "-m",
        FAULT_MODULE,
        fault,
        *parameters,
        *shlex.split(context.text),
    ]
    context.command = shlex.join(arguments)
    result = get_container(context, node).exec_run(
        arguments, user="root", workdir="/code"
    )
    context.response = result.output.decode().strip()
    context.exit_code = result.exit_code
