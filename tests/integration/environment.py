"""
Behave entry point.

For details of env bootstrap, see env_control
"""
import logging

from tests.integration import env_control, logs


def before_all(context):
    """
    Prepare environment for tests.
    """
    context.state = env_control.create()
    context.conf = context.state['config']


def before_scenario(context, _scenario):
    """
    Cleanup function executing per feature scenario.
    """
    env_control.restart(state=context.state)


def after_step(context, step):
    """
    Save logs after failed step.
    """
    if step.status == 'failed':
        logs.save_logs(context)


def after_all(context):
    """
    Clean up.
    """
    if context.failed and not context.aborted:
        logging.warning('Remember to run `make clean` after you done')
        return
    env_control.stop(state=context.state)
