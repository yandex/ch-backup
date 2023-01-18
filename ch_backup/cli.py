# -*- coding: utf-8 -*-
"""
Command-line interface.
"""
import json
import re
import signal
import sys
import typing
import uuid
from collections import OrderedDict, defaultdict
from functools import wraps
from typing import Union

from click import Choice, ParamType, Path, argument, pass_context, style
from click.types import StringParamType
from cloup import (Color, Context, HelpFormatter, HelpTheme, Style, group, option, option_group)
from cloup.constraints import constraint, mutually_exclusive
from tabulate import tabulate

from . import logging
from .backup.metadata import BackupState, TableMetadata
from .ch_backup import ClickhouseBackup
from .config import Config
from .util import drop_privileges, setup_environment, utcnow
from .version import get_version

TIMESTAMP = utcnow().strftime('%Y%m%dT%H%M%S')
UUID = str(uuid.uuid4())


# pylint: disable=unused-argument
def signal_handler(signum, frame):
    """
    Logs received signal. Useful for troubleshooting.
    """
    logging.info('Received signal %d', signum)


signal.signal(signal.SIGTERM, signal_handler)
# SIGKILL can't be handled.
# signal.signal(signal.SIGKILL, signal_handler)
signal.signal(signal.SIGHUP, signal_handler)
signal.signal(signal.SIGINT, signal_handler)


@group(context_settings=Context.settings(
    help_option_names=['-h', '--help'],
    terminal_width=100,
    align_option_groups=False,
    align_sections=True,
    show_constraints=True,
    formatter_settings=HelpFormatter.settings(
        row_sep='',  # empty line between definitions
        theme=HelpTheme(
            invoked_command=Style(fg=Color.bright_green),  # type: ignore
            heading=Style(fg=Color.bright_white, bold=True),  # type: ignore
            constraint=Style(fg=Color.bright_red),  # type: ignore
            col1=Style(fg=Color.bright_yellow),  # type: ignore
            section_help=Style(italic=True),  # type: ignore
        ),
    )))
@option('-c',
        '--config',
        type=Path(exists=True),
        default='/etc/yandex/ch-backup/ch-backup.conf',
        help='Configuration file path.')
@option('--protocol', type=Choice(['http', 'https']), help='Protocol used to connect to ClickHouse server.')
@option('--host', type=str, help='Host used to connect to ClickHouse server.')
@option('--port', type=int, help='Port used to connect to ClickHouse server.')
@option('--ca-path', type=str, help='Path to custom CA bundle path for https protocol.')
@option('--insecure', is_flag=True, help='Disable certificate verification for https protocol.')
@option('--zk-hosts', type=str, help='Use specified zk hosts for connection to the ZK')
@pass_context
def cli(ctx: Context, config: str, protocol: str, host: str, port: int, ca_path: Union[str, bool], insecure: bool,
        zk_hosts: str) -> None:
    """Tool for managing ClickHouse backups."""
    if insecure:
        ca_path = False

    cfg = Config(config)
    if host is not None:
        cfg['clickhouse']['host'] = host
    if protocol is not None:
        cfg['clickhouse']['protocol'] = protocol
    if port is not None:
        cfg['clickhouse']['port'] = port
    if ca_path is not None:
        cfg['clickhouse']['ca_path'] = ca_path
    if zk_hosts is not None:
        cfg['zookeeper']['hosts'] = zk_hosts

    logging.configure(cfg['logging'])
    setup_environment(cfg['main'])

    if not drop_privileges(cfg['main']):
        logging.warning('Drop privileges was disabled in config file.')

    ch_backup = ClickhouseBackup(cfg)

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
                logging.info('Executing command \'%s\', params: %s, args: %s, version: %s', ctx.command.name, {
                    **ctx.parent.params,
                    **ctx.params,
                }, ctx.args, get_version())
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
            result = list(map(str.strip, value.split(self.separator)))

            if self.regexp:
                for item in result:
                    if self.regexp.fullmatch(item) is None:
                        raise ValueError()

            return result

        except ValueError:
            msg = f'"{value}" is not a valid list of items'
            if self.regexp:
                msg += f' matching the format: {self.regexp_str}'

            self.fail(msg, param, ctx)


KeyValue = typing.Dict[str, str]


class KeyValueList(List):
    """
    List of key-value type for command-line parameters.
    """
    name = 'kvlist'

    def __init__(self, kv_separator=':', list_separator=','):
        super().__init__(separator=list_separator)
        self.kv_separator = kv_separator

    def convert(self, value, param, ctx):
        """
        Convert input value into list of key-value.
        """
        result: KeyValue = {}

        try:
            kvs = super().convert(value, param, ctx)

            for kv in kvs:
                k, v = list(map(str.strip, kv.split(self.kv_separator)))
                result[k] = v

            return result

        except ValueError:
            self.fail(f'"{value}" is not a valid list of key-value', param, ctx)


KeyValues = typing.Dict[str, typing.List[str]]


class KeyValuesList(KeyValueList):
    """
    List of key-values type for command-line parameters.
    """
    name = 'kvslist'

    def __init__(self, value_separator=',', kv_separator=':', list_separator=';'):
        super().__init__(kv_separator=kv_separator, list_separator=list_separator)
        self.value_separator = value_separator

    def convert(self, value, param, ctx):
        """
        Convert input value into list of key-values.
        """
        result: KeyValues = defaultdict(list)

        try:
            kvs: KeyValue = super().convert(value, param, ctx)

            for k in kvs.keys():
                vs = list(map(str.strip, kvs[k].split(self.value_separator)))
                result[k].extend(vs)

            return dict(result)

        except ValueError:
            self.fail(f'"{value}" is not a valid list of key-values', param, ctx)


class String(StringParamType):
    """
    String type for command-line parameters with support of macros and
    regexp-based validation.
    """
    name = 'string'

    def __init__(self, regexp=None, macros=None):
        self.regexp_str = regexp
        self.regexp = re.compile(regexp) if regexp else None
        self.macros = macros

    def convert(self, value, param, ctx):
        """
        Parse input value.
        """
        if self.macros:
            for macro, replacement in self.macros.items():
                value = value.replace(macro, replacement)

        if self.regexp:
            if self.regexp.fullmatch(value) is None:
                msg = f'"{value}" does not match the format: {self.regexp_str}'
                self.fail(msg, param, ctx)

        return super().convert(value, param, ctx)


@command(name='list')
@option('-a',
        '--all',
        is_flag=True,
        default=False,
        help='List all backups. The default is to show only successfully created backups.')
@option('-v', '--verbose', is_flag=True, default=False, help='Verbose output.')
@option('--format',
        type=Choice(['table', 'json']),
        default='table',
        help='Output format. The default is "table" format.')
def list_command(_ctx: Context, ch_backup: ClickhouseBackup, verbose: bool, **kwargs: dict) -> None:
    """List existing backups."""
    state = None if kwargs['all'] else BackupState.CREATED

    backups = ch_backup.list(state)

    if not verbose:
        print('\n'.join([b.name for b in backups]))
        return

    records = []
    for backup in backups:
        records.append(
            OrderedDict((
                ('name', backup.name),
                ('state', backup.state.value),
                ('start_time', backup.start_time_str),
                ('end_time', backup.end_time_str),
                ('size', backup.size),
                ('real_size', backup.real_size),
                ('ch_version', backup.ch_version),
                ('cloud_disks', backup.cloud_storage.disks),
                ('labels', backup.labels),
            )))

    if kwargs['format'] == 'json':
        json.dump(records, sys.stdout, indent=2)
        print()
    else:
        print(tabulate(records, headers='keys'))


@command(name='show')
@argument('name', metavar='BACKUP')
def show_command(ctx: Context, ch_backup: ClickhouseBackup, name: str) -> None:
    """Show details for a particular backup."""
    name = _validate_name(ctx, ch_backup, name)

    print(ch_backup.get(name))


@command(name='backup')
@option('--name',
        type=String(regexp=r'(?a)[\w-]+', macros={
            '{timestamp}': TIMESTAMP,
            '{uuid}': UUID,
        }),
        help='Name of creating backup. The value can contain macros:'
        f' {{timestamp}} - current time in UTC ({TIMESTAMP}),'
        f' {{uuid}} - randomly generated UUID value ({UUID}).',
        default='{timestamp}')
@option('-d', '--databases', type=List(regexp=r'\w+'), help='Comma-separated list of databases to backup.')
@option('-t', '--tables', type=List(regexp=r'[\w.]+'), help='Comma-separated list of tables to backup.')
@option('-f', '--force', is_flag=True, help='Enables force mode (backup.min_interval is ignored).')
@option('-l', '--label', multiple=True, help='Custom labels as key-value pairs that represents user metadata.')
@option('--schema-only', is_flag=True, help='Backup only databases schemas')
@option('--backup-access-control', is_flag=True, help='Backup users, roles, etc. created by SQL.')
def backup_command(ctx: Context, ch_backup: ClickhouseBackup, name: str, databases: list, tables: list, force: bool,
                   label: list, schema_only: bool, backup_access_control: bool) -> None:
    """Perform backup."""
    # pylint: disable=too-many-arguments
    if databases and tables:
        ctx.fail('Options --databases and --tables are mutually exclusive.')

    labels = {}
    for key_value_str in label:
        key_value = key_value_str.split('=', 1)
        key = key_value.pop(0)
        value = key_value.pop() if key_value else None
        labels[key] = value

    (name, msg) = ch_backup.backup(name,
                                   databases=databases,
                                   tables=tables,
                                   force=force,
                                   labels=labels,
                                   schema_only=schema_only,
                                   backup_access_control=backup_access_control)

    if msg:
        print(msg, file=sys.stderr, flush=True)
    print(name)


@command(name='restore')
@argument('name', metavar='BACKUP')
@option_group(
    'Database',
    f'Example for {style(List.name.upper(), bold=True)}: ' + style('"db1, db2"', fg=Color.cyan),
    mutually_exclusive(
        option('-d',
               '--databases',
               type=List(regexp=r'\w+'),
               help='Comma-separated list of databases to restore. Other databases will be skipped.'),
        option('--exclude-databases',
               type=List(regexp=r'\w+'),
               help='Comma-separated list of databases to exclude for restore. Other databases will be restored.'),
    ),
    option('--schema-only', is_flag=True, help='Restore only database schemas.'),
)
@option_group(
    'Table',
    # pylint: disable=consider-using-f-string
    'Example for {key_values}: {key_values_example}'.format(
        key_values=style(KeyValuesList.name.upper(), bold=True),
        key_values_example=style('"db1: table1, table2; db2: table3"', fg=Color.cyan),
    ),
    option('-t',
           '--tables',
           type=KeyValuesList(),
           help='Semicolon-separated list of db:tables to restore. Other tables will be skipped.'),
    option('--exclude-tables',
           type=KeyValuesList(),
           help='Semicolon-separated list of db:tables to skip on restore. Other tables will be restored.'),
    constraint=mutually_exclusive,
)
@option_group(
    'Replica',
    option('--override-replica-name', type=str, help='Override replica name to value from config.'),
    option(
        '--force-non-replicated',
        is_flag=True,
        # pylint: disable=consider-using-f-string
        help='Override {replicated_mergee_tree} family tables to {merge_tree}.'.format(
            replicated_mergee_tree=style("ReplicatedMergeTree", fg=Color.bright_green),
            merge_tree=style("MergeTree", fg=Color.bright_green),
        ),
    ),
    option('--replica-name',
           type=str,
           help=f'Replica name to be removed from zookeeper. Default - {style("hostname", fg=Color.bright_green)}.'),
)
@option_group(
    'Cloud Storage',
    option('--cloud-storage-source-bucket', type=str, help='Source bucket name to restore cloud storage data.'),
    option('--cloud-storage-source-path', type=str, help='Source path to restore cloud storage data.'),
    option('--cloud-storage-source-endpoint', type=str, help='Endpoint for source bucket.'),
    option('--cloud-storage-latest',
           is_flag=True,
           help=f'Forces to use {style("revision=0", fg=Color.bright_green)} to cloud storage.'),
    option('--skip-cloud-storage', is_flag=True, help='Forces to skip restoring data on cloud storage.'),
)
@option_group(
    'ZooKeeper',
    option('--clean-zookeeper', is_flag=True, help='Remove zookeeper metadata for tables to restore'),
)
@option('--keep-going', is_flag=True, help='Forces to keep going if some tables are failed on restore')
@constraint(mutually_exclusive, ['databases', 'tables'])
@constraint(mutually_exclusive, ['exclude_databases', 'tables'])
@constraint(mutually_exclusive, ['schema_only', 'tables'])
@constraint(mutually_exclusive, ['schema_only', 'exclude_tables'])
def restore_command(
    ctx: Context,
    ch_backup: ClickhouseBackup,
    name: str,
    databases: list,
    exclude_databases: list,
    schema_only: bool,
    tables: typing.Optional[KeyValues] = None,
    exclude_tables: typing.Optional[KeyValues] = None,
    override_replica_name: str = None,
    force_non_replicated: bool = False,
    replica_name: str = None,
    cloud_storage_source_bucket: str = None,
    cloud_storage_source_path: str = None,
    cloud_storage_source_endpoint: str = None,
    cloud_storage_latest: bool = False,
    skip_cloud_storage: bool = False,
    clean_zookeeper: bool = False,
    keep_going: bool = False,
) -> None:
    """Restore data from a particular backup."""
    # pylint: disable=too-many-arguments,too-many-locals
    name = _validate_name(ctx, ch_backup, name)

    specified_databases: typing.List[str] = _list_to_database_names(databases)
    specified_exclude_databases: typing.List[str] = _list_to_database_names(exclude_databases)

    specified_tables: typing.List[TableMetadata] = _key_values_to_tables_metadata(tables)
    specified_exclude_tables: typing.List[TableMetadata] = _key_values_to_tables_metadata(exclude_tables)

    ch_backup.restore(
        backup_name=name,
        databases=specified_databases,
        exclude_databases=specified_exclude_databases,
        schema_only=schema_only,
        tables=specified_tables,
        exclude_tables=specified_exclude_tables,
        override_replica_name=override_replica_name,
        force_non_replicated=force_non_replicated,
        replica_name=replica_name,
        cloud_storage_source_bucket=cloud_storage_source_bucket,
        cloud_storage_source_path=cloud_storage_source_path,
        cloud_storage_source_endpoint=cloud_storage_source_endpoint,
        cloud_storage_latest=cloud_storage_latest,
        skip_cloud_storage=skip_cloud_storage,
        clean_zookeeper=clean_zookeeper,
        keep_going=keep_going,
    )


@command(name='fix-s3-oplog')
@option('--cloud-storage-source-bucket', type=str, help='Source bucket name to restore cloud storage data.')
@option('--cloud-storage-source-path', type=str, help='Source path to restore cloud storage data.')
@option('--source-cluster-id', type=str, help='Source cluster ID.')
@option('--shard', type=str, default='shard1', help='Shard name.')
@option('--dryrun', is_flag=True, default=False, help='Do not perform any actions.')
def fix_s3_oplog_command(ctx: Context,
                         ch_backup: ClickhouseBackup,
                         cloud_storage_source_bucket: str = None,
                         cloud_storage_source_path: str = None,
                         source_cluster_id: str = None,
                         shard: str = 'shard1',
                         dryrun: bool = False) -> None:
    """Fix S3 operations log."""
    ch_backup.fix_s3_oplog(source_cluster_id, shard, cloud_storage_source_bucket, cloud_storage_source_path, dryrun)


@command(name='restore-schema')
@option('--source-host', type=str, help='Host used to connect to source ClickHouse server.')
@option('--source-port', type=int, help='Port used to connect to source ClickHouse server.')
@option('--exclude-dbs', type=List(regexp=r'\w+'), help='Comma-separated of databases to exclude.')
@option('--replica-name', type=str, help='Name of restored replica for zk cleanup. Default - hostname')
@option('--keep-going', is_flag=True, help='Forces to keep going if there are some errors on restoring schema.')
def restore_schema_command(ctx: Context,
                           _ch_backup: ClickhouseBackup,
                           source_host: str,
                           source_port: int,
                           exclude_dbs: list,
                           replica_name: str,
                           keep_going: bool = False) -> None:
    """Restore ClickHouse schema from replica, without s3."""
    if not source_host:
        ctx.fail('Clickhouse source host not specified.')
    _ch_backup.restore_schema(source_host, source_port, exclude_dbs, replica_name, keep_going)


@command(name='restore-access-control')
@argument('name', metavar='BACKUP')
def restore_access_control_command(_ctx: Context, _ch_backup: ClickhouseBackup, name: str) -> None:
    """Restore ClickHouse access control metadata."""
    _ch_backup.restore_access_control(name)


@command(name='delete')
@argument('name', metavar='BACKUP')
@option('--purge-partial', is_flag=True, default=False, help='Also purge all partial deleted backups')
def delete_command(ctx: Context, ch_backup: ClickhouseBackup, name: str, purge_partial: bool) -> None:
    """Delete particular backup."""
    name = _validate_name(ctx, ch_backup, name)

    deleted_backup_name, msg = ch_backup.delete(name, purge_partial)

    if msg:
        print(msg, file=sys.stderr, flush=True)

    if deleted_backup_name:
        print(deleted_backup_name)


@command(name='purge')
def purge_command(_ctx: Context, ch_backup: ClickhouseBackup) -> None:
    """Purge outdated backups."""
    names, msg = ch_backup.purge()

    if msg:
        print(msg, file=sys.stderr, flush=True)

    print('\n'.join(names))


@command(name='version')
def version_command(_ctx: Context, _ch_backup: ClickhouseBackup) -> None:
    """Print ch-backup version."""
    print(get_version())


def _validate_name(ctx: Context, ch_backup: ClickhouseBackup, name: str) -> str:
    backups = ch_backup.list()

    if name == 'LAST':
        if not backups:
            ctx.fail('There are no backups.')
        return backups[0].name

    if name not in (b.name for b in backups):
        ctx.fail(f'No backups with name "{name}" were found.')

    return name


def _list_to_database_names(dbs: typing.Optional[typing.List[str]]) -> typing.List[str]:
    if not dbs:
        return []

    return dbs


def _key_values_to_tables_metadata(kvs: typing.Optional[KeyValues]) -> typing.List[TableMetadata]:
    result: typing.List[TableMetadata] = []

    if not kvs:
        return result

    for k, vs in kvs.items():
        for v in vs:
            result.append(TableMetadata(database=k, name=v, engine='', uuid=''))

    return result
