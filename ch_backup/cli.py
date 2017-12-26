# -*- coding: utf-8 -*-
"""
Command-line interface.
"""

import logging
import re
from functools import wraps

from click import ParamType, Path, argument, group, option, pass_context

from .backup import ClickhouseBackup
from .clickhouse.control import ClickhouseCTL
from .clickhouse.layout import ClickhouseBackupLayout
from .config import Config
from .util import drop_privileges, setup_environment, setup_logging


@group(context_settings=dict(help_option_names=['-h', '--help']))
@option(
    '-c',
    '--config',
    type=Path(exists=True),
    default='/etc/yandex/ch-backup/ch-backup.conf',
    help='Configuration file path.')
@pass_context
def cli(ctx, config):
    """Tool for managing ClickHouse backups."""
    cfg = Config(config)

    setup_logging(cfg['logging'])
    setup_environment(cfg['main'])

    if not drop_privileges(cfg['main']):
        logging.warning('Drop privileges was disabled in config file.')

    ch_ctl = ClickhouseCTL(cfg['clickhouse'])
    backup_layout = ClickhouseBackupLayout(ch_ctl, cfg)

    ch_backup = ClickhouseBackup(cfg['backup'], ch_ctl, backup_layout)

    ctx.obj = dict(backup=ch_backup)


def command(*args, **kwargs):
    """
    Decorator for ch-backup cli commands.
    """

    def decorator(f):
        @pass_context
        @wraps(f)
        def wrapper(ctx, *args, **kwargs):
            try:
                logging.info('Executing command \'%s\', params: %s, args %s',
                             ctx.command.name,
                             {**
                              ctx.parent.params,
                              **
                              ctx.params}, ctx.args)
                result = ctx.invoke(f, ctx, ctx.obj['backup'], *args, **kwargs)
                logging.info('Command \'%s\' completed', ctx.command.name)
                return result
            except Exception:
                logging.exception('Command \'%s\' failed', ctx.command.name)
                raise

        return cli.command(*args, **kwargs)(wrapper)

    return decorator


class List(ParamType):
    """
    List type for command-line parameters.
    """
    name = 'list'

    def __init__(self, separator=',', regexp=None):
        self.separator = separator
        self.regexp_str = regexp
        self.regexp = re.compile(regexp) if regexp else None

    def convert(self, value, param, ctx):
        """
        Convert input value into list of items.
        """
        try:
            result = value.split(self.separator)

            if self.regexp:
                for item in result:
                    if self.regexp.fullmatch(item) is None:
                        raise ValueError()

            return result

        except ValueError:
            msg = '\'%s\' is not a valid list of items' % value
            if self.regexp:
                msg += ' matching the format: %s' % self.regexp_str

            self.fail(msg, param, ctx)


@command(name='list')
def list_command(_ctx, ch_backup):
    """List existing backups."""
    backups = ch_backup.list()

    print('\n'.join(sorted(backups, reverse=True)))


@command()
@argument('name', metavar='BACKUP')
def show(ctx, ch_backup, name):
    """Show details for a particular backup."""
    if name == 'LAST':
        backups = ch_backup.list()
        if not backups:
            ctx.fail('There are no backups to show.')
        name = max(backups)

    print(ch_backup.show(name))


@command()
@option(
    '-d',
    '--databases',
    type=List(regexp=r'\w+'),
    help='Comma-separated list of databases to backup.')
def backup(_ctx, ch_backup, databases):
    """Perform backup."""
    name = ch_backup.backup(databases)
    print(name)


@command()
@argument('name', metavar='BACKUP')
@option(
    '-d',
    '--databases',
    type=List(regexp=r'\w+'),
    help='Comma-separated list of databases to restore.')
def restore(_ctx, ch_backup, name, databases):
    """Restore data from a particular backup."""
    ch_backup.restore(name, databases)
