"""
Behave entry point.
"""

import json
import logging
import os
import re
import time
from pathlib import Path

from tests.integration import env_control
from tests.integration.diagnostics import record_step_failure
from tests.integration.modules.logs import save_logs
from tests.integration.modules.utils import version_ge, version_lt

try:
    import ipdb as pdb
except ImportError:
    import pdb  # type: ignore


def before_all(context):
    """
    Top-level setup function.
    """
    if not context.config.userdata.getbool("skip_setup"):
        env_control.create(context)
    else:
        env_control.load(context)


def before_feature(context, feature):
    """
    Per-feature setup function.
    """
    context.feature_started = time.monotonic()
    context.selected_scenario_lines = {
        s.line for s in feature.walk_scenarios() if s.should_run(context.config)
    }
    _update_feature_flags(context, feature.tags)
    env_control.update(context)
    if "dependent-scenarios" in feature.tags:
        env_control.restart(context)


def after_feature(context, feature):
    """Record complete feature time and outcomes, including version skips."""
    destination = os.getenv("INTEGRATION_FEATURE_RESULT")
    if destination:
        Path(destination).write_text(
            json.dumps(
                {
                    "feature_seconds": time.monotonic() - context.feature_started,
                    "scenarios": [
                        {"name": s.name, "line": s.line, "status": s.status.name}
                        for s in feature.walk_scenarios()
                        if s.line in context.selected_scenario_lines
                    ],
                }
            ),
            encoding="utf-8",
        )


def before_scenario(context, scenario):
    """
    Per-scenario setup function.
    """

    if (
        _check_tags(context, scenario)
        and "dependent-scenarios" not in context.feature.tags
    ):
        env_control.restart(context)


def after_step(context, step):
    """
    Per-step cleanup function.
    """
    if step.status in ("failed", "error"):
        record_step_failure(context, step)
        save_logs(context)
        if context.config.userdata.getbool("debug"):
            pdb.post_mortem(step.exc_traceback)


def after_all(context):
    """
    Top-level cleanup function.
    """
    if context.failed and not context.aborted:
        logging.warning("Remember to run `make clean` after you done")
        return
    env_control.stop(context)


def _update_feature_flags(context, feature_tags):
    feature_flags = set(context.conf["default_feature_flags"])

    for tag in feature_tags:
        prefix_feature_pair = tag.split("_", 1)
        if len(prefix_feature_pair) < 2:
            continue

        prefix, feature = prefix_feature_pair
        if prefix == "with":
            feature_flags.add(feature)
        elif prefix == "without":
            feature_flags.discard(feature)

    context.feature_flags = feature_flags


def _check_tags(context, scenario):
    ch_version = context.conf["ch_version"]

    for tag in scenario.tags:
        require_version = _parse_version_tag(tag, "require_version")
        if require_version:
            if not version_ge(ch_version, require_version):
                logging.info("Skipping scenario due to require_version mismatch")
                scenario.mark_skipped()
                return False

        require_lt_version = _parse_version_tag(tag, "require_version_less_than")
        if require_lt_version:
            if not version_lt(ch_version, require_lt_version):
                logging.info(
                    "Skipping scenario due to require_version_less_than mismatch"
                )
                scenario.mark_skipped()
                return False

    return True


def _parse_version_tag(tag, prefix):
    tag_pattern = prefix + r"_(?P<version>[\d\.]+)"

    match = re.fullmatch(tag_pattern, tag)
    if match:
        return match.group("version")

    return None
