# -*- coding: utf-8 -*-
"""
Command-line interface.
"""

# pylint: disable=too-many-lines

import json
import os
import signal
import sys
import typing
import uuid
from collections import OrderedDict
from functools import wraps
from pathlib import Path as PathlibPath
from typing import Iterable

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

from ch_backup.command_context import command_context
from ch_backup.exceptions import ConfigurationError, TerminatingSignal

from . import logging
from .backup.copy_report import CopyReport, print_copy_report
from .backup.metadata import BackupState, CopyScope, TableMetadata
from .backup.sources import BackupSources
from .ch_backup import CleanZooKeeperMode, ClickhouseBackup
from .config import DEFAULT_CONFIG, Config
from .logic.partial_restore import PartialRestoreFilter
from .params import JsonParamType, KeyValues, List, String, TimeSpan
from .profile import profile
from .util import (
    chown_dir_contents,
    drop_privileges,
    raise_nofile_limit,
    setup_environment,
    utcnow,
)
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
    ca_path: str | bool,
    insecure: bool,
    zk_hosts: str,
    config_parameters: Iterable[tuple[str, dict]],
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

    PathlibPath(cfg["loguru"]["logs_directory"]).mkdir(exist_ok=True)
    chown_dir_contents(
        cfg["main"]["user"],
        cfg["main"]["group"],
        cfg["loguru"]["logs_directory"],
        need_recursion=False,
    )
    os.chdir(cfg["main"]["working_dir"])
    setup_environment(cfg["main"])

    if not drop_privileges(cfg["main"]):
        logging.warning("Drop privileges was disabled in config file.")

    # Creating logger as not privileged user is required to copy logger to child processes
    logging.configure(cfg["loguru"])
    raise_nofile_limit()

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
                ch_backup = ctx.obj["backup"]
                with command_context(ctx.command.name):
                    result = ctx.invoke(f, ctx, ch_backup, *args, **kwargs)
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
    "--include-unreadable",
    is_flag=True,
    help="Include unreadable metadata in verbose listings (-a -v shows it too); exit with code 2 if any is unreadable.",
)
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
    include_unreadable: bool,
    format_: str,
) -> None:
    """List existing backups."""
    if include_unreadable and not verbose:
        _ctx.fail("--include-unreadable requires --verbose")
    state = None if all_ else BackupState.CREATED

    unreadable: dict[str, str] = {}
    backups = ch_backup.list(state, unreadable=unreadable)
    if not (all_ or include_unreadable):
        unreadable.clear()

    if not verbose:
        print("\n".join([b.name for b in backups]))
        return

    records = []
    for backup in backups:
        data_skipped_tables, incomplete_tables = backup.count_skipped_data_tables()
        record: dict = OrderedDict(
            (
                ("name", backup.name),
                ("state", backup.state.value),
                ("start_time", backup.start_time_str),
                ("end_time", backup.end_time_str),
                ("size", backup.size),
                ("real_size", backup.real_size),
                ("ch_version", backup.ch_version),
                ("data_skipped_tables", data_skipped_tables),
                ("incomplete_tables", incomplete_tables),
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
        if all_ or include_unreadable:
            record["metadata_error"] = None
        records.append(record)

    for name, error in sorted(unreadable.items()):
        record = dict.fromkeys(
            (
                "start_time",
                "end_time",
                "size",
                "real_size",
                "ch_version",
                "data_skipped_tables",
                "incomplete_tables",
                "labels",
            )
        )
        record = {"name": name, "state": "unknown", **record}
        if format_ == "json":
            record.update(cloud_disks=None, exception=None)
        record["metadata_error"] = error
        records.append(record)

    if format_ == "json":
        json.dump(records, sys.stdout, indent=2)
        print()
    else:
        print(tabulate(records, headers="keys"))
    if unreadable and include_unreadable:
        print(f"Unreadable metadata: {len(unreadable)}", file=sys.stderr, flush=True)
        _ctx.exit(2)


@command(name="show")  # type: ignore
@argument("name", metavar="BACKUP")
@option("--pretty", is_flag=True, default=False, help="Pretty output.")
@option(
    "-d",
    "--database",
    type=str,
    help="Filter output to show only the specified database.",
)
@option(
    "-t",
    "--table",
    type=str,
    help="Filter output to show only the specified table (format: db.table or table when used with --database).",
)
# pylint: disable=too-many-positional-arguments
def show_command(
    ctx: Context,
    ch_backup: ClickhouseBackup,
    name: str,
    pretty: bool,
    database: str | None,
    table: str | None,
) -> None:
    """Show details for a particular backup."""
    name = _validate_and_resolve_name(ctx, ch_backup, name)

    try:
        print(
            ch_backup.get(name).dump_json(pretty=pretty, database=database, table=table)
        )
    except ValueError as e:
        ctx.fail(str(e))


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
            type=List(regexp=r".+"),
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
    option(
        "--http-handlers",
        is_flag=True,
        help="Perform partial backup of SQL-defined HTTP handlers.",
    ),
    option(
        "--workload", is_flag=True, help="Perform partial backup of workload entities."
    ),
    option(
        "--detached-parts",
        is_flag=True,
        help="Also backup detached parts of selected tables. Overrides backup.detached_parts.enabled.",
    ),
    option(
        "--detached-parts-disk",
        "--detached-parts-disks",
        "detached_parts_disks",
        type=List(),
        help="Comma-separated disk names whose detached directories are backed up. Overrides backup.detached_parts.disks (default: object_storage).",
    ),
    option(
        "--detached-parts-reason",
        "--detached-parts-reasons",
        "detached_parts_reasons",
        type=List(),
        help="Comma-separated detach reasons. By default all reasons are included; pass an empty value to select user-detached parts. Overrides backup.detached_parts.reasons.",
    ),
)
@option_group(
    "Cloud Storage",
    option(
        "--copy-cloud-storage-data",
        is_flag=True,
        help="Copy object storage data into the backup instead of storing "
        "references only. Increases backup size and duration. The data is copied "
        "by ClickHouse as is: unlike the metadata, it is neither compressed nor "
        "encrypted by ch-backup.",
    ),
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
                        typing.cast(dict[str, dict], DEFAULT_CONFIG["clickhouse"])[
                            "freeze_timeout"
                        ]
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
    option(
        "--keep-going",
        is_flag=True,
        help="Leave out a data part, or the data of a table, that cannot be "
        "backed up, instead of failing the whole backup. Overrides backup.keep_going.",
    ),
)
@constraint(mutually_exclusive, ["schema_only", "access"])
@constraint(mutually_exclusive, ["schema_only", "data"])
@constraint(mutually_exclusive, ["schema_only", "schema"])
@constraint(mutually_exclusive, ["schema_only", "udf"])
@constraint(mutually_exclusive, ["schema_only", "nc"])
@constraint(mutually_exclusive, ["schema_only", "workload"])
@constraint(mutually_exclusive, ["schema_only", "http_handlers"])
@constraint(mutually_exclusive, ["data", "schema"])
# pylint: disable=too-many-positional-arguments
def backup_command(
    _ctx: Context,
    ch_backup: ClickhouseBackup,
    name: str,
    databases: list,
    tables: list,
    force: bool,
    keep_going: bool,
    label: list,
    schema_only: bool,
    freeze_timeout: int,
    access: bool,
    data: bool,
    schema: bool,
    udf: bool,
    nc: bool,
    workload: bool,
    copy_cloud_storage_data: bool,
    http_handlers: bool,
    detached_parts: bool,
    detached_parts_disks: list[str] | None,
    detached_parts_reasons: list[str] | None,
) -> None:
    """Perform backup."""
    # pylint: disable=too-many-arguments,too-many-locals
    if copy_cloud_storage_data:
        ch_backup.config.merge({"cloud_storage": {"copy_data": True}})
    if keep_going:
        ch_backup.config.merge({"backup": {"keep_going": True}})
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

    backup_config = ch_backup.config["backup"]
    if any([access, data, schema, udf, nc, workload, schema_only, http_handlers]):
        sources = BackupSources.for_backup(
            access,
            data,
            schema,
            udf,
            nc,
            workload,
            schema_only,
            http_handlers=http_handlers,
        )
    else:
        sources = BackupSources.from_config(backup_config["sources"])

    detached_parts_config = backup_config["detached_parts"]
    if not isinstance(detached_parts_config["enabled"], bool):
        raise ConfigurationError("backup.detached_parts.enabled must be a boolean")
    if detached_parts_disks is None:
        detached_parts_disks = detached_parts_config["disks"]
    if detached_parts_reasons is None:
        detached_parts_reasons = detached_parts_config["reasons"]

    name, msg = ch_backup.backup(
        sources,
        name,
        db_names=databases,
        tables=tables,
        force=force,
        labels=labels,
        include_detached_parts=detached_parts or detached_parts_config["enabled"],
        detached_parts_disks=detached_parts_disks,
        detached_parts_reasons=detached_parts_reasons,
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
            type=List(regexp=r".+"),
            help="Comma-separated list of databases to restore. Other databases will be skipped.",
        ),
        option(
            "--exclude-databases",
            type=List(regexp=r".+"),
            help="Comma-separated list of databases to exclude for restore. Other databases will be restored.",
        ),
    ),
    option("--schema-only", is_flag=True, help="Restore only database schemas."),
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
    option(
        "--parts-from-backup",
        type=str,
        help="Download data parts that all other replicas have from this backup instead "
        "of fetching them from replicas. Only for restore without data.",
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
        "--cloud-storage-source-endpoint",
        type=str,
        help="Endpoint for source bucket: HTTP(S) URL or host[:port]. "
        "Without a scheme, uses the original disk's scheme.",
    ),
    option(
        "--skip-cloud-storage",
        is_flag=True,
        help=(
            "Skip cloud data and adapt storage policies and TTL MOVE rules to the target. "
            "Conflicting Keeper metadata causes an error without resetting replicas."
        ),
    ),
)
@option_group(
    "ZooKeeper",
    option(
        "--clean-zookeeper-mode",
        type=Choice(CleanZooKeeperMode),
        default=CleanZooKeeperMode.REPLICA_ONLY,
        help="Remove zookeeper metadata for tables to restore.",
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
    option(
        "--table-included-patterns",
        type=List(regexp=r"\w+\.[\w*]+"),
        help="Comma-separated list of db.tables to restore. Other tables will be skipped. "
        "Patterns support Unix shell-style wildcards(*) and are supported only for tables."
        "Examples: db1.table1 | db1.table2,db2.* | db1.prefix*,db2.*suffix | db1.*",
    ),
    option(
        "--table-excluded-patterns",
        type=List(regexp=r"\w+\.[\w*]+"),
        help="Comma-separated list of db.tables to skip on restore. Other tables will be restored. "
        "Patterns support Unix shell-style wildcards(*) and are supported only for tables."
        "When used together with option table-included-patterns, it has a higher priority and excludes tables from final result."
        "Examples: db1.table1 | db1.table2,db2.* | db1.prefix*,db2.*suffix | db1.*",
    ),
    option("--nc", is_flag=True, help="Perform partial restore of named collections."),
    option(
        "--http-handlers",
        is_flag=True,
        help="Perform partial restore of SQL-defined HTTP handlers.",
    ),
    option(
        "--workload", is_flag=True, help="Perform partial restore of workload entities."
    ),
)
@option(
    "--restore-detached-parts",
    is_flag=True,
    help="Restore backed-up detached parts without attaching them.",
)
@option(
    "--keep-going",
    is_flag=True,
    help="Forces to keep going if some tables are failed on restore",
)
@option(
    "--restore-tables-in-replicated-database",
    is_flag=True,
    help="Restore tables in Replicated databases",
)
@constraint(mutually_exclusive, ["table_included_patterns", "exclude_databases"])
@constraint(mutually_exclusive, ["schema_only", "access"])
@constraint(mutually_exclusive, ["schema_only", "data"])
@constraint(mutually_exclusive, ["schema_only", "schema"])
@constraint(mutually_exclusive, ["schema_only", "udf"])
@constraint(mutually_exclusive, ["schema_only", "nc"])
@constraint(mutually_exclusive, ["schema_only", "workload"])
@constraint(mutually_exclusive, ["schema_only", "http_handlers"])
@constraint(mutually_exclusive, ["data", "schema"])
# pylint: disable=too-many-positional-arguments
def restore_command(
    ctx: Context,
    ch_backup: ClickhouseBackup,
    name: str,
    databases: list,
    exclude_databases: list,
    schema_only: bool,
    override_replica_name: str = None,
    force_non_replicated: bool = False,
    replica_name: str = None,
    parts_from_backup: str | None = None,
    cloud_storage_source_bucket: str = None,
    cloud_storage_source_path: str = None,
    cloud_storage_source_endpoint: str = None,
    skip_cloud_storage: bool = False,
    restore_detached_parts: bool = False,
    clean_zookeeper_mode: CleanZooKeeperMode = CleanZooKeeperMode.REPLICA_ONLY,
    keep_going: bool = False,
    restore_tables_in_replicated_database: bool = False,
    access: bool = False,
    data: bool = False,
    schema: bool = False,
    udf: bool = False,
    table_included_patterns: list[str] | None = None,
    table_excluded_patterns: list[str] | None = None,
    nc: bool = False,
    workload: bool = False,
    http_handlers: bool = False,
) -> None:
    """Restore data from a particular backup."""
    # pylint: disable=too-many-arguments,too-many-locals
    name = _validate_and_resolve_name(ctx, ch_backup, name)

    specified_databases: list[str] = _list_to_database_names(databases)
    excluded_databases: list[str] = _list_to_database_names(exclude_databases)

    matcher = None
    if table_included_patterns:
        matcher = PartialRestoreFilter(inverted=False, patterns=table_included_patterns)

    if table_excluded_patterns:
        matcher = PartialRestoreFilter(inverted=True, patterns=table_excluded_patterns)

    sources = BackupSources.for_restore(
        access,
        data,
        schema,
        udf,
        nc,
        workload,
        schema_only,
        http_handlers=http_handlers,
    )
    if parts_from_backup:
        parts_from_backup = _validate_and_resolve_name(
            ctx, ch_backup, parts_from_backup, BackupState.CREATED
        )
    ch_backup.restore(
        sources=sources,
        backup_name=name,
        databases=specified_databases,
        exclude_databases=excluded_databases,
        override_replica_name=override_replica_name,
        force_non_replicated=force_non_replicated,
        replica_name=replica_name,
        parts_backup_name=parts_from_backup,
        cloud_storage_source_bucket=cloud_storage_source_bucket,
        cloud_storage_source_path=cloud_storage_source_path,
        cloud_storage_source_endpoint=cloud_storage_source_endpoint,
        skip_cloud_storage=skip_cloud_storage,
        restore_detached_parts=restore_detached_parts,
        clean_zookeeper_mode=clean_zookeeper_mode,
        keep_going=keep_going,
        restore_tables_in_replicated_database=restore_tables_in_replicated_database,
        partial_restore_filter=matcher,
    )


@command(name="restore-from-replica")  # type: ignore
@option(
    "--source-host",
    type=str,
    required=True,
    help="Live replica of the same shard to restore from. It is connected with the "
    "credentials and TLS settings of this host.",
)
@option(
    "--replica-name",
    type=str,
    help=f'Replica name to be removed from zookeeper. Default - {style("{replica}", fg=Color.bright_green)} macro.',
)
@option_group(
    "Partial restore",
    option("--schema", is_flag=True, help="Restore databases and tables schemas."),
    option("--access", is_flag=True, help="Restore access control entities."),
    option("--udf", is_flag=True, help="Restore user defined functions."),
    option("--nc", is_flag=True, help="Restore named collections."),
    option("--workload", is_flag=True, help="Restore workload entities."),
)
# pylint: disable=too-many-positional-arguments
def restore_from_replica_command(
    _ctx: Context,
    ch_backup: ClickhouseBackup,
    source_host: str,
    replica_name: str | None = None,
    schema: bool = False,
    access: bool = False,
    udf: bool = False,
    nc: bool = False,
    workload: bool = False,
) -> None:
    """Restore from a live replica of the shard instead of a backup. All without options."""
    sources = BackupSources(data=False, http_handlers=False)
    if any([schema, access, udf, nc, workload]):
        sources = BackupSources(
            access=access,
            data=False,
            schema=schema,
            udf=udf,
            named_collections=nc,
            workload_entities=workload,
            http_handlers=False,
        )
    ch_backup.restore_from_replica(source_host, sources, replica_name)


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


@command(name="copy")  # type: ignore
@argument("name", metavar="[BACKUP]", required=False)
@option_group(
    "General",
    option(
        "-a",
        "--all",
        "all_",
        is_flag=True,
        default=False,
        help='Copy all backups in the state "created". Older backups are copied first.',
    ),
    option(
        "--keep-going",
        is_flag=True,
        default=False,
        help="Continue copying independent objects and backups after errors.",
    ),
    option(
        "--report-path",
        type=Path(dir_okay=False),
        help="Save the final copy report as JSON, including on failure.",
    ),
    option(
        "--scope",
        "scopes",
        multiple=True,
        type=Choice([scope.value for scope in CopyScope]),
        help='Copy only the specified scope: "files" for files of the backup, '
        '"cloud_storage" for data of object storage disks. Can be specified multiple times, '
        "all scopes are copied by default. The copy is published once every scope is copied. "
        'Without "files", copies are taken from the target storage, so the source storage is '
        'not accessed; --all and LAST select copies with "cloud_storage" not completed.',
    ),
)
@option_group(
    "Target storage options",
    option(
        "--target-config",
        type=Path(exists=True),
        help="Configuration file with settings of the target storage. "
        "It's merged on top of the main configuration, so it's enough to specify only "
        'differing settings (e.g. "storage.credentials" and "backup.path_root").',
    ),
    option(
        "--target-config-parameter",
        "target_config_parameters",
        multiple=True,
        type=(str, JsonParamType()),
        metavar="PATH VALUE",
        help="Paths and values to override target configuration values. "
        "The same as --config-parameter, but for the target configuration.",
    ),
    option(
        "--server-side-copy/--no-server-side-copy",
        default=None,
        help="Copy objects on the server side if source and target buckets belong to the same "
        "S3 service and are accessible with the same credentials. Enabled by default.",
    ),
    option(
        "--resume/--no-resume",
        default=None,
        help="Skip objects confirmed by previous runs of copying and still present in the target "
        "storage. Enabled by default. With --no-resume, all objects are copied again.",
    ),
)
@option_group(
    "Cloud storage options",
    option(
        "--target-cloud-storage-bucket",
        type=str,
        help="Bucket to copy data of object storage disks to.",
    ),
    option(
        "--target-cloud-storage-path",
        type=str,
        help="Path in the bucket to copy data of object storage disks to. "
        'Data of the backup is put under "<path>/<backup name>/".',
    ),
    option(
        "--cloud-storage-source-bucket",
        type=str,
        help="Bucket with data of object storage disks. By default, it's taken from ClickHouse configuration.",
    ),
    option(
        "--cloud-storage-source-path",
        type=str,
        help="Path in the bucket with data of object storage disks. "
        "By default, it's taken from ClickHouse configuration.",
    ),
)
# pylint: disable=too-many-arguments,too-many-positional-arguments,too-many-locals
def copy_command(
    ctx: Context,
    ch_backup: ClickhouseBackup,
    name: str | None,
    all_: bool,
    target_config: str | None,
    target_config_parameters: Iterable[tuple[str, dict]],
    server_side_copy: bool | None,
    resume: bool | None,
    target_cloud_storage_bucket: str | None,
    target_cloud_storage_path: str | None,
    cloud_storage_source_bucket: str | None,
    cloud_storage_source_path: str | None,
    keep_going: bool = False,
    report_path: str | None = None,
    scopes: tuple[str, ...] = (),
) -> None:
    """Copy backup to another storage (another S3 bucket, endpoint or path)."""
    report = CopyReport()
    try:
        if bool(name) == all_:
            ctx.fail("Either BACKUP or --all must be specified.")

        backup_names = None
        if name:
            if name == "LAST" and (not scopes or CopyScope.FILES.value in scopes):
                name = _validate_and_resolve_name(
                    ctx, ch_backup, name, BackupState.CREATED
                )
            backup_names = [name]

        copy_config: dict = {"cloud_storage": {"source": {}, "target": {}}}
        if server_side_copy is not None:
            copy_config["server_side_copy"] = server_side_copy
        if resume is not None:
            copy_config["resume"] = resume
        for section, key, value in (
            ("target", "bucket", target_cloud_storage_bucket),
            ("target", "path", target_cloud_storage_path),
            ("source", "bucket", cloud_storage_source_bucket),
            ("source", "path", cloud_storage_source_path),
        ):
            if value is not None:
                copy_config["cloud_storage"][section][key] = value

        target_cfg = ch_backup.config.derive(
            target_config,
            _build_cli_cfg_from_config_parameters(target_config_parameters),
        )
        target_cfg.merge({"copy": copy_config})
        ch_backup.copy(
            target_cfg,
            backup_names,
            keep_going=keep_going,
            report=report,
            scopes=scopes,
        )
    except BaseException as e:
        report.error = f"{type(e).__name__}: {e}"
        raise
    finally:
        print_copy_report(report, report_path)


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


@command(name="get-cloud-storage-metadata")  # type: ignore
@argument("name", metavar="BACKUP")
@option("--disk", type=str, required=True, help="Disk to get metadata")
@option(
    "--local-path",
    "local_path",
    type=str,
    default="",
    help="Download metadata as tar archive to a given path. Will unpack tar to shadow directory if local_path is not specified",
)
def get_cloud_storage_metadata(
    ctx: Context,
    ch_backup: ClickhouseBackup,
    name: str,
    disk: str,
    local_path: str,
) -> None:
    """Download cloud storage metadata to shadow directory"""
    name = _validate_and_resolve_name(
        ctx, ch_backup, name, backup_state=BackupState.CREATED
    )
    if not ch_backup.get_cloud_storage_metadata(
        backup_name=name, disk_name=disk, local_path=local_path
    ):
        print(f"Metadata for disk {disk} and backup {name} is already present")


def _validate_and_resolve_name(
    ctx: Context,
    ch_backup: ClickhouseBackup,
    name: str,
    backup_state: BackupState = None,
) -> str:
    """Check that the backup exists; LAST is refused while any metadata is unreadable."""
    unreadable: dict[str, str] = {}
    backups = ch_backup.list(backup_state, unreadable=unreadable)

    if name == "LAST":
        if unreadable:
            ctx.fail(
                f"Cannot resolve LAST, metadata is unreadable for: {', '.join(sorted(unreadable))}"
            )
        if not backups:
            ctx.fail("There are no backups.")
        return backups[0].name

    if name not in unreadable and name not in (b.name for b in backups):
        ctx.fail(f'No backups with name "{name}" were found.')

    return name


def _list_to_database_names(dbs: list[str] | None) -> list[str]:
    if not dbs:
        return []

    return dbs


def _key_values_to_tables_metadata(
    kvs: KeyValues | None,
) -> list[TableMetadata]:
    result: list[TableMetadata] = []

    if not kvs:
        return result

    for k, vs in kvs.items():
        for v in vs:
            result.append(TableMetadata(database=k, name=v, engine="", uuid=""))

    return result


def _build_cli_cfg_from_config_parameters(values: Iterable[tuple[str, dict]]) -> dict:
    """
    Build config dict from specified keys and values in plain format.
    Duplicate keys are ignored in favor of the last entry.
    """

    def _split_key(key: str) -> list[str]:
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
