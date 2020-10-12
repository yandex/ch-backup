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

    version_parts = os.getenv("CLICKHOUSE_VERSION", "0.0").split('.')
    assert len(version_parts) >= 2, "Invalid version string"
    context.maj_ver, context.min_ver = int(version_parts[0]), int(version_parts[1])


def before_feature(context, _feature):
    """
    Cleanup function executing per feature.
    """
    if 'dependent-scenarios' in _feature.tags:
        env_control.restart(context)


def before_scenario(context, scenario):
    """
    Cleanup function executing per scenario.
    """
    if 'dependent-scenarios' not in context.feature.tags and _check_tags(context, scenario):
        env_control.restart(context)


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


def _check_tags(context, scenario):
    require_tags = list(filter(lambda tag: tag.startswith('require_version_'), scenario.tags))
    assert len(require_tags) <= 1, "Only one require_version_X_Y accepted"
    if len(require_tags) == 1:
        req_ver_parts = require_tags[0][len("require_version_"):].split('.')
        assert len(req_ver_parts) == 2, "Invalid required version"
        maj_req_ver, min_req_ver = int(req_ver_parts[0]), int(req_ver_parts[1])

        if context.maj_ver < maj_req_ver or (context.maj_ver == maj_req_ver and context.min_ver < min_req_ver):
            scenario.mark_skipped()
            return False

    legacy_tags = list(filter(lambda tag: tag.startswith('legacy_versions_prior_'), scenario.tags))
    assert len(legacy_tags) <= 1, "Only one legacy_version_X_Y accepted"
    if len(legacy_tags) == 1:
        leg_ver_parts = legacy_tags[0][len("legacy_versions_prior_"):].split('.')
        assert len(leg_ver_parts) == 2, "Invalid legacy version"
        maj_leg_ver, min_leg_ver = int(leg_ver_parts[0]), int(leg_ver_parts[1])

        if context.maj_ver > maj_leg_ver or (context.maj_ver == maj_leg_ver and context.min_ver > min_leg_ver):
            scenario.mark_skipped()
            return False
    return True
