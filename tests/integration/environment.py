"""
Behave entry point.
"""
import logging
import os

from tests.integration import env_control
from tests.integration.modules.logs import save_logs

try:
    import ipdb as pdb
except ImportError:
    import pdb  # type: ignore


def before_all(context):
    """
    Prepare environment for tests.
    """
    if not context.config.userdata.getbool('skip_setup'):
        env_control.create(context)


def before_feature(context, _feature):
    """
    Cleanup function executing per feature.
    """
    if 'dependent-scenarios' in _feature.tags:
        env_control.restart(context)


def before_scenario(context, _scenario):
    """
    Cleanup function executing per scenario.
    """
    if 'dependent-scenarios' not in context.feature.tags:
        env_control.restart(context)
    _check_tags(_scenario)


def after_step(context, step):
    """
    Save logs after failed step.
    """
    if step.status == 'failed':
        save_logs(context)
        if context.config.userdata.getbool('debug'):
            pdb.post_mortem(step.exc_traceback)


def after_all(context):
    """
    Clean up.
    """
    if context.failed and not context.aborted:
        logging.warning('Remember to run `make clean` after you done')
        return
    env_control.stop(context)


def _check_tags(scenario):
    tags = list(filter(lambda tag: tag.startswith('require_version_'), scenario.tags))
    assert len(tags) <= 1, "Only one require_version_X_Y accepted"
    if len(tags) == 1:
        req_ver_parts = tags[0][len("require_version_"):].split('.')
        assert len(req_ver_parts) == 2, "Invalid required version"
        maj_req_ver, min_req_ver = int(req_ver_parts[0]), int(req_ver_parts[1])

        version_parts = os.getenv("CLICKHOUSE_VERSION", "0.0").split('.')
        assert len(version_parts) >= 2, "Invalid version string"
        maj_ver, min_ver = int(version_parts[0]), int(version_parts[1])

        if maj_ver < maj_req_ver or (maj_ver == maj_req_ver and min_ver < min_req_ver):
            scenario.mark_skipped()
