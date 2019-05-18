"""
Utility functions to use in implementation of test steps.
"""

from typing import Any

import yaml

from .templates import render_template
from .typing import ContextT


def get_step_data(context: ContextT) -> Any:
    """
    Return step data deserialized from YAML representation and processed by
    template engine.
    """
    if not context.text:
        return {}

    return yaml.load(render_template(context, context.text), yaml.SafeLoader)
