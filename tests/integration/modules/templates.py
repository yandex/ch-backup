"""
Module responsible for template rendering.
"""
import os

from jinja2 import Environment, FileSystemLoader, StrictUndefined

from . import compose, utils

TEMP_FILE_EXT = 'temp~'


@utils.env_stage('create', fail=True)
def render_configs(context):
    """
    Render each template in the subtree.
    Each template is rendered in-place. As the framework operates in
    staging dir, this is easily reset by `make clean`, or `rm -fr staging`.
    """
    conf = context.conf
    compose_conf = compose.read_config(conf)
    config_root = '{0}/images'.format(conf['staging_dir'])
    jinja_context = {
        'conf': conf,
        'compose': compose_conf,
    }
    # Render configs only for projects that are
    # present in config file.
    for project in conf['projects']:
        for root, _, files in os.walk('%s/%s' % (config_root, project)):
            for basename in files:
                if basename.endswith(TEMP_FILE_EXT):
                    continue
                render_templates_dir(jinja_context, root, basename)


def render_templates_dir(jinja_context, directory, basename):
    """
    Renders the actual template.
    """
    path = '%s/%s' % (directory, basename)
    temp_file_path = '%s.%s' % (path, TEMP_FILE_EXT)
    loader = FileSystemLoader(directory)
    env = getenv(loader)
    # Various filters, e.g. "password_clear | sha256" yields a hashed password.
    try:
        with open(temp_file_path, 'w') as temp_file:
            temp_file.write(env.get_template(basename).render(jinja_context))
    except Exception as exc:
        raise RuntimeError(
            "'{exc_type}' while rendering '{name}': {exc}".format(
                exc_type=exc.__class__.__name__,
                name=path,
                exc=exc,
            ))
    os.rename(temp_file_path, path)


def getenv(loader=None):
    """
    Create Jinja2 env object.
    """
    return Environment(
        autoescape=False,
        trim_blocks=False,
        undefined=StrictUndefined,
        keep_trailing_newline=True,
        loader=loader)
