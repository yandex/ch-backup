# -*- coding: utf-8 -*-
"""
Command-line interface.
"""
import json
import signal
import sys
import typing
import uuid
from collections import OrderedDict
from functools import wraps
from typing import Iterable, Tuple, Union

from click import Choice, Path, argument, pass_context, style
from cloup import (
    Color,
    Context,
    HelpFormatter,
    HelpTheme,
    Style,
    group,
    option,
    option_group,
)
from cloup.constraints import constraint, mutually_exclusive
from humanfriendly import format_timespan
from tabulate import tabulate

from ch_backup.exceptions import TerminatingSignal

from . import logging
from .backup.metadata import BackupState, TableMetadata
from .backup.sources import BackupSources
from .ch_backup import ClickhouseBackup
from .config import DEFAULT_CONFIG, Config
from .params import JsonParamType, KeyValues, KeyValuesList, List, String, TimeSpan
from .profile import profile
from .util import drop_privileges, setup_environment, utcnow
from .version import get_version

TIMESTAMP = utcnow().strftime("%Y%m%dT%H%M%S")
UUID = str(uuid.uuid4())


def signal_handler(signum, _frame):
    """
    Logs received signal. Useful for troubleshooting.
    """
    logging.info(f"Received signal {signum}")
    # If a signal handler raises an exception, the exception will be propagated to the main
    # thread and may be raised after any bytecode instruction.
    # https://docs.python.org/3/library/signal.html#note-on-signal-handlers-and-exceptions
    raise TerminatingSignal(f"Execution was interrupted by the signal {signum}")


signal.signal(signal.SIGTERM, signal_handler)
# SIGKILL can't be handled.
# signal.signal(signal.SIGKILL, signal_handler)
signal.signal(signal.SIGHUP, signal_handler)
signal.signal(signal.SIGINT, signal_handler)


@group(
    context_settings=Context.settings(
        help_option_names=["-h", "--help"],
        terminal_width=100,
        align_option_groups=False,
        align_sections=True,
        show_constraints=True,
        formatter_settings=HelpFormatter.settings(
            row_sep="",  # empty line between definitions
            theme=HelpTheme(
                invoked_command=Style(fg=Color.bright_green),  # type: ignore
                heading=Style(fg=Color.bright_white, bold=True),  # type: ignore
                constraint=Style(fg=Color.bright_red),  # type: ignore
                col1=Style(fg=Color.bright_yellow),  # type: ignore
                section_help=Style(italic=True),  # type: ignore
            ),
        ),
    )
)
@option(
    "-c",
    "--config",
    type=Path(exists=True),
    default="/etc/yandex/ch-backup/ch-backup.conf",
    help="Configuration file path.",
)
@option(
    "--protocol",
    type=Choice(["http", "https"]),
    help="Protocol used to connect to ClickHouse server.",
)
@option("--host", type=str, help="Host used to connect to ClickHouse server.")
@option("--port", type=int, help="Port used to connect to ClickHouse server.")
@option("--ca-path", type=str, help="Path to custom CA bundle path for https protocol.")
@option(
    "--insecure",
    is_flag=True,
    help="Disable certificate verification for https protocol.",
)
@option("--zk-hosts", type=str, help="Use specified zk hosts for connection to the ZK")
@option(
    "--config-parameter",
    "config_parameters",
    multiple=True,
    type=(str, JsonParamType()),
    metavar="PATH VALUE",
    help="Paths and values to override ch-backup config values. "
    'Path should contains a string with dot separated keys (e.g. "backup.min_interval.minutes"). '
    "Value should be json-serializable string or plain value (string, true/false, number). "
    "Can be specified multiple times to override several settings.",
)
@pass_context
# pylint: disable=too-many-positional-arguments
def cli(
    ctx: Context,
    config: str,
    protocol: str,
    host: str,
    port: int,
    ca_path: Union[str, bool],
    insecure: bool,
    zk_hosts: str,
    config_parameters: Iterable[Tuple[str, dict]],
) -> None:
    """Tool for managing ClickHouse backups."""
    if insecure:
        ca_path = False

    cfg = Config(config)
    if host is not None:
        cfg["clickhouse"]["host"] = host
    if protocol is not None:
        cfg["clickhouse"]["protocol"] = protocol
    if port is not None:
        cfg["clickhouse"]["port"] = port
    if ca_path is not None:
        cfg["clickhouse"]["ca_path"] = ca_path
    if zk_hosts is not None:
        cfg["zookeeper"]["hosts"] = zk_hosts

    if config_parameters is not None:
        cli_cfg = _build_cli_cfg_from_config_parameters(config_parameters)
        cfg.merge(cli_cfg)

    logging.configure(cfg["loguru"])
    setup_environment(cfg["main"])

    if not drop_privileges(cfg["main"]):
        logging.warning("Drop privileges was disabled in config file.")

    ch_backup = ClickhouseBackup(cfg)

    ctx.obj = {"backup": ch_backup}


def command(*args, **kwargs):
    """
    Decorator for ch-backup cli commands.
    """

    def decorator(f):
        @pass_context
        @wraps(f)
        @profile(10)
        def wrapper(ctx, *args, **kwargs):
            try:
                logging.info(
                    "Executing command '{}', params: {}, args: {}, version: {}",
                    ctx.command.name,
                    {
                        **ctx.parent.params,
                        **ctx.params,
                    },
                    ctx.args,
                    get_version(),
                )
                result = ctx.invoke(f, ctx, ctx.obj["backup"], *args, **kwargs)
                logging.info("Command '{}' completed", ctx.command.name)
                return result
            except (Exception, TerminatingSignal):
                logging.exception("Command '{}' failed", ctx.command.name)
                raise

        return cli.command(*args, **kwargs)(wrapper)

    return decorator


@command(name="list")
@option(
    "-a",
    "--all",
    "all_",
    is_flag=True,
    default=False,
    help="List all backups. The default is to show only successfully created backups.",
)
@option("-v", "--verbose", is_flag=True, default=False, help="Verbose output.")
@option(
    "--format",
    "format_",
    type=Choice(["table", "json"]),
    default="table",
    help='Output format. The default is "table" format.',
)
def list_command(
    _ctx: Context,
    ch_backup: ClickhouseBackup,
    verbose: bool,
    all_: bool,
    format_: str,
) -> None:
    """List existing backups."""
    state = None if all_ else BackupState.CREATED

    backups = ch_backup.list(state)

    if not verbose:
        print("\n".join([b.name for b in backups]))
        return

    records = []
    for backup in backups:
        record: dict = OrderedDict(
            (
                ("name", backup.name),
                ("state", backup.state.value),
                ("start_time", backup.start_time_str),
                ("end_time", backup.end_time_str),
                ("size", backup.size),
                ("real_size", backup.real_size),
                ("ch_version", backup.ch_version),
            )
        )
        if format_ == "json":
            record["labels"] = backup.labels
            record["cloud_disks"] = backup.cloud_storage.disks
            record["exception"] = backup.exception
        else:
            record["labels"] = "\n".join(
                f"{name}: {value}" for name, value in backup.labels.items()
            )
        records.append(record)

    if format_ == "json":
        json.dump(records, sys.stdout, indent=2)
        print()
    else:
        print(tabulate(records, headers="keys"))


@command(name="show")  # type: ignore
@argument("name", metavar="BACKUP")
def show_command(ctx: Context, ch_backup: ClickhouseBackup, name: str) -> None:
    """Show details for a particular backup."""
    name = _validate_and_resolve_name(ctx, ch_backup, name)

    print(ch_backup.get(name))


@command(name="backup")
@option_group(
    "General",
    option(
        "--name",
        type=String(
            regexp=r"(?a)[\w-]+",
            macros={
                "timestamp": TIMESTAMP,
                "uuid": UUID,
            },
        ),
        help="Name of creating backup. The value can contain macros:"
        f" {{timestamp}} - current time in UTC ({TIMESTAMP}),"
        f" {{uuid}} - randomly generated UUID value ({UUID}).",
        default="{timestamp}",
    ),
    option(
        "-l",
        "--label",
        multiple=True,
        help="Custom labels as key-value pairs that represents user metadata.",
    ),
)
@option_group(
    "Partial backup",
    mutually_exclusive(
        option(
            "-d",
            "--databases",
            type=List(regexp=r"\w+"),
            help="Comma-separated list of databases to backup.",
        ),
        option(
            "-t",
            "--tables",
            type=List(regexp=r"[\w.]+"),
            help="Comma-separated list of tables to backup.",
        ),
    ),
    option("--schema-only", is_flag=True, help="Backup only databases schemas"),
    option(
        "--access",
        is_flag=True,
        help="Perform partial backup of access control entities.",
    ),
    option(
        "--data",
        is_flag=True,
        help="Perform partial backup of database schemas and tables data.",
    ),
    option(
        "--schema",
        is_flag=True,
        help="Perform partial backup of databases and tables schemas.",
    ),
    option(
        "--udf", is_flag=True, help="Perform partial backup of user defined functions."
    ),
    option("--nc", is_flag=True, help="Perform partial backup of named collections."),
)
@option_group(
    "Timeout configuration",
    # pylint: disable=consider-using-f-string
    "Examples for {timespan_slug}: {examples}".format(
        timespan_slug=style(TimeSpan.name.upper(), bold=True),
        examples=style('"10 seconds"', fg=Color.cyan)
        + ", "
        + style('"1.5 hours"', fg=Color.cyan),
    ),
    option(
        "--freeze-timeout",
        type=TimeSpan(),
        help="Timeout for `ALTER FREEZE` command. Default is {d}".format(
            d=style(
                '"{}"'.format(
                    format_timespan(
                        typing.cast(
                            typing.Dict[str, dict], DEFAULT_CONFIG["clickhouse"]
                        )["freeze_timeout"]
                    )
                ),
                fg=Color.cyan,
            )
        ),
    ),
)
@option_group(
    "Util",
    option(
        "-f",
        "--force",
        is_flag=True,
        help="Enables force mode (backup.min_interval is ignored).",
    ),
)
@constraint(mutually_exclusive, ["schema_only", "access"])
@constraint(mutually_exclusive, ["schema_only", "data"])
@constraint(mutually_exclusive, ["schema_only", "schema"])
@constraint(mutually_exclusive, ["schema_only", "udf"])
@constraint(mutually_exclusive, ["schema_only", "nc"])
@constraint(mutually_exclusive, ["data", "schema"])
# pylint: disable=too-many-positional-arguments
def backup_command(
    _ctx: Context,
    ch_backup: ClickhouseBackup,
    name: str,
    databases: list,
    tables: list,
    force: bool,
    label: list,
    schema_only: bool,
    freeze_timeout: int,
    access: bool,
    data: bool,
    schema: bool,
    udf: bool,
    nc: bool,
) -> None:
    """Perform backup."""
    # pylint: disable=too-many-arguments,too-many-locals
    if freeze_timeout:
        logging.info(f"ALTER FREEZE timeout force set to {freeze_timeout} sec.")
        ch_backup.reload_config(
            ch_backup.config.merge({"clickhouse": {"freeze_timeout": freeze_timeout}})
        )

    labels = {}
    for key_value_str in label:
        key_value = key_value_str.split("=", 1)
        key = key_value.pop(0)
        value = key_value.pop() if key_value else None
        labels[key] = value

    sources = BackupSources.for_backup(access, data, schema, udf, nc, schema_only)
    (name, msg) = ch_backup.backup(
        sources, name, db_names=databases, tables=tables, force=force, labels=labels
    )

    if msg:
        print(msg, file=sys.stderr, flush=True)
    print(name)


@command(name="restore")  # type: ignore
@argument("name", metavar="BACKUP")
@option_group(
    "Database",
    f"Example for {style(List.name.upper(), bold=True)}: "
    + style('"db1, db2"', fg=Color.cyan),
    mutually_exclusive(
        option(
            "-d",
            "--databases",
            type=List(regexp=r"\w+"),
            help="Comma-separated list of databases to restore. Other databases will be skipped.",
        ),
        option(
            "--exclude-databases",
            type=List(regexp=r"\w+"),
            help="Comma-separated list of databases to exclude for restore. Other databases will be restored.",
        ),
    ),
    option("--schema-only", is_flag=True, help="Restore only database schemas."),
)
@option_group(
    "Table",
    # pylint: disable=consider-using-f-string
    "Example for {key_values}: {key_values_example}".format(
        key_values=style(KeyValuesList.name.upper(), bold=True),
        key_values_example=style('"db1: table1, table2; db2: table3"', fg=Color.cyan),
    ),
    option(
        "-t",
        "--tables",
        type=KeyValuesList(),
        help="Semicolon-separated list of db:tables to restore. Other tables will be skipped.",
    ),
    option(
        "--exclude-tables",
        type=KeyValuesList(),
        help="Semicolon-separated list of db:tables to skip on restore. Other tables will be restored.",
    ),
    constraint=mutually_exclusive,
)
@option_group(
    "Replica",
    option(
        "--override-replica-name",
        type=str,
        help="Override replica name to value from config.",
    ),
    option(
        "--force-non-replicated",
        is_flag=True,
        # pylint: disable=consider-using-f-string
        help="Override {replicated_mergee_tree} family tables to {merge_tree}.".format(
            replicated_mergee_tree=style("ReplicatedMergeTree", fg=Color.bright_green),
            merge_tree=style("MergeTree", fg=Color.bright_green),
        ),
    ),
    option(
        "--replica-name",
        type=str,
        help=f'Replica name to be removed from zookeeper. Default - {style("hostname", fg=Color.bright_green)}.',
    ),
)
@option_group(
    "Cloud Storage",
    option(
        "--cloud-storage-source-bucket",
        type=str,
        help="Source bucket name to restore cloud storage data.",
    ),
    option(
        "--cloud-storage-source-path",
        type=str,
        help="Source path to restore cloud storage data.",
    ),
    option(
        "--cloud-storage-source-endpoint", type=str, help="Endpoint for source bucket."
    ),
    option(
        "--skip-cloud-storage",
        is_flag=True,
        help="Forces to skip restoring data on cloud storage.",
    ),
)
@option_group(
    "ZooKeeper",
    option(
        "--clean-zookeeper/--no-clean-zookeeper",
        is_flag=True,
        default=True,
        help="Remove zookeeper metadata for tables to restore",
    ),
)
@option_group(
    "Partial restore",
    option(
        "--access",
        is_flag=True,
        help="Perform partial restore of access control entities.",
    ),
    option(
        "--data",
        is_flag=True,
        help="Perform partial restore of database schemas and tables data.",
    ),
    option(
        "--schema",
        is_flag=True,
        help="Perform partial restore of databases and tables schemas.",
    ),
    option(
        "--udf", is_flag=True, help="Perform partial restore of user defined functions."
    ),
    option("--nc", is_flag=True, help="Perform partial restore of named collections."),
)
@option(
    "--keep-going",
    is_flag=True,
    help="Forces to keep going if some tables are failed on restore",
)
@constraint(mutually_exclusive, ["databases", "tables"])
@constraint(mutually_exclusive, ["exclude_databases", "tables"])
@constraint(mutually_exclusive, ["schema_only", "tables"])
@constraint(mutually_exclusive, ["schema_only", "exclude_tables"])
@constraint(mutually_exclusive, ["schema_only", "access"])
@constraint(mutually_exclusive, ["schema_only", "data"])
@constraint(mutually_exclusive, ["schema_only", "schema"])
@constraint(mutually_exclusive, ["schema_only", "udf"])
@constraint(mutually_exclusive, ["schema_only", "nc"])
@constraint(mutually_exclusive, ["data", "schema"])
# pylint: disable=too-many-positional-arguments
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
    skip_cloud_storage: bool = False,
    clean_zookeeper: bool = False,
    keep_going: bool = False,
    access: bool = False,
    data: bool = False,
    schema: bool = False,
    udf: bool = False,
    nc: bool = False,
) -> None:
    """Restore data from a particular backup."""
    # pylint: disable=too-many-arguments,too-many-locals
    name = _validate_and_resolve_name(ctx, ch_backup, name)

    specified_databases: typing.List[str] = _list_to_database_names(databases)
    specified_exclude_databases: typing.List[str] = _list_to_database_names(
        exclude_databases
    )

    specified_tables: typing.List[TableMetadata] = _key_values_to_tables_metadata(
        tables
    )
    specified_exclude_tables: typing.List[TableMetadata] = (
        _key_values_to_tables_metadata(exclude_tables)
    )

    sources = BackupSources.for_restore(access, data, schema, udf, nc, schema_only)
    ch_backup.restore(
        sources=sources,
        backup_name=name,
        databases=specified_databases,
        exclude_databases=specified_exclude_databases,
        tables=specified_tables,
        exclude_tables=specified_exclude_tables,
        override_replica_name=override_replica_name,
        force_non_replicated=force_non_replicated,
        replica_name=replica_name,
        cloud_storage_source_bucket=cloud_storage_source_bucket,
        cloud_storage_source_path=cloud_storage_source_path,
        cloud_storage_source_endpoint=cloud_storage_source_endpoint,
        skip_cloud_storage=skip_cloud_storage,
        clean_zookeeper=clean_zookeeper,
        keep_going=keep_going,
    )


@command(name="restore-access-control")  # type: ignore
@argument("name", metavar="BACKUP")
def restore_access_control_command(
    ctx: Context, ch_backup: ClickhouseBackup, name: str
) -> None:
    """Restore ClickHouse access control metadata."""
    name = _validate_and_resolve_name(ctx, ch_backup, name)

    ch_backup.restore_access_control(name)


@command(name="fix-admin-user")
@option(
    "--dryrun",
    is_flag=True,
    default=False,
    help="Do not perform any destructive actions.",
)
def fix_admin_user_command(
    _ctx: Context, ch_backup: ClickhouseBackup, dryrun: bool = False
) -> None:
    """Check and fix potential duplicates of `admin` user in Keeper."""
    ch_backup.fix_admin_user(dry_run=dryrun)


@command(name="delete")  # type: ignore
@argument("name", metavar="BACKUP")
@option(
    "--purge-partial",
    is_flag=True,
    default=False,
    help="Also purge all partial deleted backups",
)
@option(
    "-f",
    "--force",
    is_flag=True,
    default=False,
    help="Do nothing if backup does not exist",
)
def delete_command(
    ctx: Context,
    ch_backup: ClickhouseBackup,
    name: str,
    purge_partial: bool,
    force: bool,
) -> None:
    """Delete particular backup."""
    try:
        name = _validate_and_resolve_name(ctx, ch_backup, name)
    except Exception:
        if force:
            logging.info(
                f"Attempt to delete a non-existent backup '{name}' was skipped"
            )
            return
        raise

    deleted_backup_name, msg = ch_backup.delete(name, purge_partial)

    if msg:
        print(msg, file=sys.stderr, flush=True)

    if deleted_backup_name:
        print(deleted_backup_name)


@command(name="purge")
def purge_command(_ctx: Context, ch_backup: ClickhouseBackup) -> None:
    """Purge outdated backups."""
    names, msg = ch_backup.purge()

    if msg:
        print(msg, file=sys.stderr, flush=True)

    print("\n".join(names))


@command(name="version")
def version_command(_ctx: Context, _ch_backup: ClickhouseBackup) -> None:
    """Print ch-backup version."""
    print(get_version())


def _validate_and_resolve_name(
    ctx: Context, ch_backup: ClickhouseBackup, name: str
) -> str:
    backups = ch_backup.list()

    if name == "LAST":
        if not backups:
            ctx.fail("There are no backups.")
        return backups[0].name

    if name not in (b.name for b in backups):
        ctx.fail(f'No backups with name "{name}" were found.')

    return name


def _list_to_database_names(dbs: typing.Optional[typing.List[str]]) -> typing.List[str]:
    if not dbs:
        return []

    return dbs


def _key_values_to_tables_metadata(
    kvs: typing.Optional[KeyValues],
) -> typing.List[TableMetadata]:
    result: typing.List[TableMetadata] = []

    if not kvs:
        return result

    for k, vs in kvs.items():
        for v in vs:
            result.append(TableMetadata(database=k, name=v, engine="", uuid=""))

    return result


def _build_cli_cfg_from_config_parameters(values: Iterable[Tuple[str, dict]]) -> dict:
    """
    Build config dict from specified keys and values in plain format.
    Duplicate keys are ignored in favor of the last entry.
    """

    def _split_key(key: str) -> typing.List[str]:
        return key.split(".")

    values_by_uniq_key: dict[str, dict] = {}
    for key, value in values:
        values_by_uniq_key[key] = value

    values_sorted = sorted(
        values_by_uniq_key.items(), key=lambda x: len(_split_key(x[0]))
    )

    result: dict = {}
    for key, value in values_sorted:
        path = _split_key(key)
        if not path:
            continue

        subresult = result
        for i, subkey in enumerate(path):
            if i != len(path) - 1 and not isinstance(subresult, dict):
                continue

            if i == len(path) - 1:
                subresult[subkey] = value
            elif subkey not in subresult:
                subresult[subkey] = {}
            subresult = subresult[subkey]

    return result
