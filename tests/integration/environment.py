"""
Behave entry point.
"""
import logging
import re

from tests.integration import env_control
from tests.integration.modules.logs import save_logs
from tests.integration.modules.utils import version_ge, version_lt

try:
    import ipdb as pdb
except ImportError:
    import pdb  # type: ignore


def before_all(context):
    """
    Prepare environment for tests.
    """
    if not context.config.userdata.getbool("skip_setup"):
        env_control.create(context)
    else:
        env_control.load(context)


def before_feature(context, _feature):
    """
    Cleanup function executing per feature.
    """
    if "dependent-scenarios" in _feature.tags:
        env_control.restart(context)


def before_scenario(context, scenario):
    """
    Cleanup function executing per scenario.
    """
    if "dependent-scenarios" not in context.feature.tags and _check_tags(
        context, scenario
    ):
        env_control.restart(context)


def after_step(context, step):
    """
    Save logs after failed step.
    """
    if step.status == "failed":
        save_logs(context)
        if context.config.userdata.getbool("debug"):
            pdb.post_mortem(step.exc_traceback)


def after_all(context):
    """
    Clean up.
    """
    if context.failed and not context.aborted:
        logging.warning("Remember to run `make clean` after you done")
        return
    env_control.stop(context)


def _check_tags(context, scenario):
    ch_version = context.conf["ch_version"]

    require_version = _parse_version_tag(scenario.tags, "require_version")
    if require_version:
        if not version_ge(ch_version, require_version):
            logging.info("Skipping scenario due to require_version mismatch")
            scenario.mark_skipped()
            return False

    require_lt_version = _parse_version_tag(scenario.tags, "require_version_less_than")
    if require_lt_version:
        if not version_lt(ch_version, require_lt_version):
            logging.info("Skipping scenario due to require_version_less_than mismatch")
            scenario.mark_skipped()
            return False

    return True


def _parse_version_tag(tags, prefix):
    tag_pattern = prefix + r"_(?P<version>[\d\.]+)"
    for tag in tags:
        match = re.fullmatch(tag_pattern, tag)
        if match:
            return match.group("version")

    return None
