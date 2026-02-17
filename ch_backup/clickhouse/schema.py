"""
Utilities for schema manipulation.
"""

import re

from ch_backup import logging
from ch_backup.clickhouse.models import Database, Table
from ch_backup.util import escape


def to_attach_query(create_query: str) -> str:
    """
    Convert CREATE table query to ATTACH one.
    """
    return re.sub("^CREATE", "ATTACH", create_query)


def to_create_query(create_query: str) -> str:
    """
    Convert CREATE table query to ATTACH one.
    """
    return re.sub("^ATTACH", "CREATE", create_query)


def rewrite_table_schema(
    table: Table,
    force_non_replicated_engine: bool = False,
    override_replica_name: str = None,
    add_uuid: bool = False,
    inner_uuid: str = None,
) -> None:
    """
    Rewrite table schema.
    """
    # Use repr for logging because schema may contain surrogate escape symbols
    logging.info(f"Going to rewrite table schema: {table.create_statement!r}")
    if force_non_replicated_engine:
        create_statement = table.create_statement
        match = re.search(
            r"(?P<replicated>Replicated)\S{0,20}MergeTree\((?P<params>('[^']+', '[^']+'(,\s*|))|)",
            create_statement,
        )
        if match:
            params = match.group("params")
            if len(params) > 0:
                create_statement = create_statement.replace(params, "").replace(
                    match.group("replicated"), ""
                )
                create_statement = create_statement.replace("MergeTree()", "MergeTree")
                table.create_statement = create_statement
            if table.is_replicated():
                table.engine = table.engine.replace("Replicated", "")

    if override_replica_name:
        create_statement = table.create_statement
        match = re.search(
            r"Replicated\S{0,20}MergeTree\('[^']+', (?P<replica>\'\S+\')",
            create_statement,
        )
        if match:
            table.create_statement = create_statement.replace(
                match.group("replica"), f"'{override_replica_name}'"
            )

    if add_uuid:
        _add_uuid(table, inner_uuid)

    table.create_statement = re.sub(
        f"(?P<create>CREATE|ATTACH)\\s+(?P<type>TABLE|(\\S+\\s+)?VIEW|DICTIONARY)\\s+(_|`?{escape(table.name)}`?)\\s+",
        f"\\g<create> \\g<type> `{escape(table.database)}`.`{escape(table.name)}` ",
        table.create_statement,
    )
    logging.info(f"Resulting table schema: {table.create_statement!r}")


def rewrite_database_schema(
    db: Database,
    db_sql: str,
    force_non_replicated_engine: bool = False,
    override_replica_name: str = None,
) -> str:
    """
    Rewrite database schema
    """
    if force_non_replicated_engine:
        db_sql = re.sub(
            r"ENGINE\s*=\s*Replicated\('[^']*'\s*,\s*'[^']*'\s*,\s*'[^']*'\s*\)",
            r"ENGINE=Atomic",
            db_sql,
        )

    if override_replica_name:
        match = re.search(
            r"Replicated\('[^']*'\s*,\s*'[^']*'\s*,\s*(?P<replica>'[^']*'\s*)\)", db_sql
        )
        if match:
            db_sql = db_sql.replace(
                match.group("replica"), f"'{override_replica_name}'"
            )

    return re.sub(
        r"CREATE\s+DATABASE\s+_\s", f"CREATE DATABASE `{escape(db.name)}` ", db_sql
    )


def _add_uuid(table: Table, inner_uuid: str = None) -> None:
    mach_uuid = re.search(
        r"UUID '\w{8}-\w{4}-\w{4}-\w{4}-\w{12}'", table.create_statement
    )
    if mach_uuid is not None:
        logging.info(
            f"{mach_uuid.group(0)} already present in table schema."
            f" Metadata UUID is {table.uuid}. return without adding UUID"
        )
        return
    if table.is_view():
        inner_uuid_clause = f"TO INNER UUID '{inner_uuid}'" if inner_uuid else ""
        table.create_statement = re.sub(
            f"^(?P<statement>CREATE|ATTACH) (?P<view_type>((MATERIALIZED|LIVE) )?VIEW) "
            f"(?P<view_name>`?{table.database}`?.`?{table.name}`?) ",
            f"\\g<statement> \\g<view_type> \\g<view_name> UUID '{table.uuid}' {inner_uuid_clause} ",
            table.create_statement,
        )
    else:
        # CREATE TABLE <db-name>.<table-name> $ (...)
        # UUID clause is inserted to $ place.
        table.create_statement = re.sub(
            r"(?P<statement>CREATE|ATTACH) (?P<type>\S*) "
            r"(?P<table_name>(`[^`]*`\.`[^`]*`|\S+\.\S+|`[^`]*`\.\S+|\S+\.`[^`]*`))",
            f"\\g<statement> \\g<type> \\g<table_name> UUID '{table.uuid}'",
            table.create_statement,
        )


def embedded_schema_db_sql(db: Database) -> str:
    """
    Returns create statement for db with embedded schema.
    """
    return f"CREATE DATABASE `{db.name}` Engine={db.engine}"
