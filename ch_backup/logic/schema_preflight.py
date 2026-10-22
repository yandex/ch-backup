"""Validate restored schemas before destructive database/table restore work."""

import sys
from uuid import uuid4

from ch_backup import logging
from ch_backup.backup_context import BackupContext
from ch_backup.clickhouse.client import ClickhouseError
from ch_backup.clickhouse.masking import mask_sql_literals
from ch_backup.clickhouse.models import Database, Table
from ch_backup.clickhouse.schema import (
    embedded_schema_db_sql,
    rewrite_database_schema,
    to_create_query,
)
from ch_backup.clickhouse.schema_probe import (
    can_create_probe,
    has_local_probe_storage,
    probe_database,
    probe_table,
    schema_engine,
)
from ch_backup.exceptions import ClickhouseBackupError


def validate_database_schemas(
    context: BackupContext, databases: dict[str, Database]
) -> dict[str, str]:
    """Download and check all definitions before the first DROP DATABASE."""
    statements = dict(
        context.backup_layout.get_database_create_statements(
            context.backup_meta,
            [name for name, db in databases.items() if not db.has_embedded_metadata()],
        )
    )
    for name, db in databases.items():
        statement = (
            embedded_schema_db_sql(db)
            if db.has_embedded_metadata()
            else statements[name]
        )
        statement = rewrite_database_schema(
            db,
            to_create_query(statement),
            context.config["force_non_replicated"],
            context.config["override_replica_name"],
        )
        try:
            engine = schema_engine(context.ch_ctl.explain_schema(statement))
            if engine in {"Atomic", "Ordinary"} and not db.has_embedded_metadata():
                probe_name = f"_ch_backup_preflight_{uuid4().hex}"
                context.ch_ctl.create_schema_probe_database(
                    probe_name,
                    probe_database(statement, probe_name),
                    ordinary=engine == "Ordinary",
                )
                _cleanup(context, [probe_name], failed=False)
            elif not db.has_embedded_metadata():
                logging.info(
                    "Schema preflight for database {}: syntax only (database engine {})",
                    name,
                    engine,
                )
        except ClickhouseError as error:
            raise ClickhouseBackupError(
                f"Schema preflight failed for database {name}: {mask_sql_literals(str(error))}"
            ) from None
        statements[name] = statement
    return statements


def validate_table_schemas(context: BackupContext, tables: list[Table]) -> None:
    """Parse every schema; create safe local copies in disposable Atomic databases."""
    # pylint: disable=too-many-locals
    if not tables:
        return
    external_functions = context.ch_ctl.get_user_defined_function_names()
    asts = {}
    for table in tables:
        try:
            asts[table] = context.ch_ctl.explain_schema(table.create_statement)
        except ClickhouseError as error:
            raise ClickhouseBackupError(
                f"Schema preflight failed for {table.database}.{table.name}: {mask_sql_literals(str(error))}"
            ) from None
    # Empty MergeTree tables still write format_version.txt. A remote-backed
    # policy (including a mixed policy or an engine default) is not isolated.
    has_merge_tree = any(
        (schema_engine(ast) or "").endswith("MergeTree") for ast in asts.values()
    )
    policies = context.ch_ctl.get_storage_policies() if has_merge_tree else {}
    disks = context.ch_ctl.get_disks() if has_merge_tree else {}
    candidates = [
        table
        for table in tables
        if has_local_probe_storage(table, policies, disks, context.ch_config.config)
    ]
    # Excluding an external object also excludes views which would access it.
    while True:
        names = {f"{table.database}.{table.name}" for table in candidates}
        remaining = [
            table
            for table in candidates
            if can_create_probe(table, asts[table], names, external_functions)
        ]
        if len(remaining) == len(candidates):
            break
        candidates = remaining
    for table in tables:
        if table not in candidates:
            logging.info(
                "Schema preflight for {}.{}: syntax only (external engine/storage, active query, or dependency outside the isolated schemas)",
                table.database,
                table.name,
            )
    databases = {
        name: f"_ch_backup_preflight_{uuid4().hex}"
        for name in dict.fromkeys(table.database for table in candidates)
    }
    table_names = {(table.database, table.name) for table in candidates}
    defaults = context.ch_config.config.get("replicated_merge_tree")
    policy = defaults.get("storage_policy") if defaults is not None else None
    disk = defaults.get("disk") if defaults is not None else None
    probes = [
        (
            table,
            probe_table(table, databases, table_names, policy, replicated_disk=disk),
        )
        for table in candidates
    ]
    created = []
    try:
        for name in databases.values():
            # No IF NOT EXISTS and no retry: a name collision must never give us
            # ownership of a database belonging to somebody else.
            context.ch_ctl.create_schema_probe_database(name)
            created.append(name)
        while probes:
            failed = []
            errors = []
            for original, probe in probes:
                try:
                    context.ch_ctl.create_schema_probe(probe)
                except ClickhouseError as error:
                    failed.append((original, probe))
                    errors.append(
                        f"{original.database}.{original.name}: {mask_sql_literals(str(error))}"
                    )
            if len(failed) == len(probes):
                raise ClickhouseBackupError(
                    "Schema preflight failed: " + "; ".join(errors)
                )
            # Like restore, retry dependent views after their source tables.
            probes = failed
    finally:
        _cleanup(context, created, failed=sys.exc_info()[0] is not None)


def _cleanup(context: BackupContext, created: list[str], failed: bool) -> None:
    """Try every owned database, preserving an earlier validation failure."""
    cleanup_errors = []
    for name in reversed(created):
        try:
            context.ch_ctl.drop_schema_probe_database(name)
        except Exception:
            cleanup_errors.append(name)
            logging.error("Cannot remove schema preflight database {}", name)
    if cleanup_errors and not failed:
        raise ClickhouseBackupError(
            "Cannot remove schema preflight databases: " + ", ".join(cleanup_errors)
        )
