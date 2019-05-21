"""
Common steps.
"""
import yaml
from behave import given

from tests.integration.modules.ch_backup import get_version
from tests.integration.modules.utils import merge


@given('default configuration')
@given('configuration')
def step_configuration(context):
    default = {'ch_backup': {'protocol': 'http'}}
    overridden_options = yaml.load(context.text or '', yaml.SafeLoader) or {}
    for key, value in merge(default, overridden_options).items():
        context.__setattr__(key, value)

    context.version = get_version()
