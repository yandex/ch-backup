"""
Module responsible for template rendering.
"""
import os

from jinja2 import BaseLoader, Environment, FileSystemLoader, StrictUndefined

from . import docker
from .datetime import decrease_time_str, increase_time_str
from .typing import ContextT
from .utils import context_to_dict, env_stage, version_ge, version_lt

TEMP_FILE_EXT = 'temp~'


@env_stage('create', fail=True)
def render_configs(context: ContextT) -> None:
    """
    Render each template in the subtree.
    Each template is rendered in-place. As the framework operates in
    staging dir, this is easily reset by `make clean`, or `rm -fr staging`.
    """
    staging_dir = context.conf['staging_dir']
    for service, conf in context.conf['services'].items():
        for i in range(1, conf.get('docker_instances', 1) + 1):
            instance_dir = f'{staging_dir}/images/{service}{i:02d}'
            context.instance_id = f'{i:02d}'
            context.instance_name = f'{service}{i:02d}'
            for root, _, files in os.walk(instance_dir):
                for basename in files:
                    if not basename.endswith(TEMP_FILE_EXT):
                        _render_file(context, root, basename)
    context.instance_name = None
    context.instance_id = None


def render_template(context: ContextT, text: str) -> str:
    """
    Render template passed as a string.
    """
    template = _environment(context).from_string(text)
    return template.render(context_to_dict(context))


def _render_file(context: ContextT, directory: str, basename: str) -> None:
    path = os.path.join(directory, basename)
    temp_file_path = f'{path}.{TEMP_FILE_EXT}'
    loader = FileSystemLoader(directory)
    environment = _environment(context, loader)
    jinja_context = context_to_dict(context)
    try:
        with open(temp_file_path, 'w', encoding='utf-8') as temp_file:
            template = environment.get_template(basename)
            temp_file.write(template.render(jinja_context))
    except Exception as e:
        raise RuntimeError(f'Failed to render {path}') from e
    os.rename(temp_file_path, path)


def _environment(context: ContextT, loader: BaseLoader = None) -> Environment:
    """
    Create Environment object.
    """
    def _get_file_size(container_name, path):
        container = docker.get_container(context, container_name)
        return docker.get_file_size(container, path)

    def _ch_version_ge(comparing_version):
        return version_ge(context.conf['ch_version'], comparing_version)

    def _ch_version_lt(comparing_version):
        return version_lt(context.conf['ch_version'], comparing_version)

    environment = Environment(autoescape=False,
                              trim_blocks=False,
                              undefined=StrictUndefined,
                              keep_trailing_newline=True,
                              loader=loader)

    environment.filters['increase_on'] = increase_time_str
    environment.filters['decrease_on'] = decrease_time_str

    environment.globals['get_file_size'] = _get_file_size
    environment.globals['ch_version_ge'] = _ch_version_ge
    environment.globals['ch_version_lt'] = _ch_version_lt

    return environment
