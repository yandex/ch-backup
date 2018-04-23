"""
Common steps.
"""

import yaml
from behave import given

from tests.integration.helpers.utils import merge


@given('default configuration')
@given('configuration')
def step_configuration(context):
    default = {'ch_backup': {'protocol': 'https'}}
    overridden_options = yaml.load(context.text or '') or {}
    for key, value in merge(default, overridden_options).items():
        context.__setattr__(key, value)
