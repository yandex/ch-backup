"""
Common steps.
"""
import yaml
from behave import given, when

from tests.integration.modules.ch_backup import get_version
from tests.integration.modules.docker import get_container
from tests.integration.modules.steps import get_step_data
from tests.integration.modules.utils import merge


@given('default configuration')
@given('configuration')
def step_configuration(context):
    default = {
        'ch_backup': {
            'protocol': 'http',
        },
        'clickhouse_settings': {},
    }
    overridden_options = yaml.load(context.text or '', yaml.SafeLoader) or {}
    for key, value in merge(default, overridden_options).items():
        context.__setattr__(key, value)

    context.version = get_version()


@given('ClickHouse settings')
def step_update_ch_backup_config(context):
    context.clickhouse_settings = get_step_data(context)


@when('we try to execute command on {node:w}')
def step_try_command(context, node):
    container = get_container(context, node)
    context.command = context.text.strip()
    result = container.exec_run(['bash', '-c', context.command], user='root')
    context.response = result.output.decode().strip()
    context.exit_code = result.exit_code


@given('we have executed command on {node:w}')
@when('we execute command on {node:w}')
def step_command(context, node):
    step_try_command(context, node)
    assert context.exit_code == 0, (f'"{context.command}" failed with exit code {context.exit_code},'
                                    f' output:\n {context.response}')
