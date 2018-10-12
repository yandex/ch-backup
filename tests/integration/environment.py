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
    env_control.create(context)


def before_feature(context, _feature):
    """
    Cleanup function executing per feature.
    """
    env_control.restart(context)


def after_step(context, step):
    """
    Save logs after failed step.
    """
    if step.status == 'failed':
        logs.save_logs(context)
        if context.config.userdata.getbool('debug'):
            try:
                import ipdb as pdb
            except ImportError:
                import pdb
            pdb.post_mortem(step.exc_traceback)


def after_all(context):
    """
    Clean up.
    """
    if context.failed and not context.aborted:
        logging.warning('Remember to run `make clean` after you done')
        return
    env_control.stop(context)
