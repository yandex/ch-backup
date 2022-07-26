"""
Utilities for schema manipulation.
"""
import re

from ch_backup.clickhouse.models import Table


def is_merge_tree(engine: str) -> bool:
    """
    Return True if table engine belongs to merge tree table engine family, or False otherwise.
    """
    return engine.find('MergeTree') != -1


def is_replicated(engine: str) -> bool:
    """
    Return True if table engine belongs to replicated merge tree table engine family, or False otherwise.
    """
    return engine.find('Replicated') != -1


def is_distributed(engine: str) -> bool:
    """
    Return True if it's Distributed table engine, or False otherwise.
    """
    return engine == 'Distributed'


def is_view(engine: str) -> bool:
    """
    Return True if table engine is a view (either View or MaterializedView), or False otherwise.
    """
    return engine in ('View', 'LiveView', 'MaterializedView')


def is_materialized_view(engine: str) -> bool:
    """
    Return True if it's MaterializedView table engine, or False otherwise.
    """
    return engine == 'MaterializedView'


def is_external_db_engine(db_engine: str) -> bool:
    """
    Return True if DB's engine is one of:
    - MySQL
    - MaterializedMySQL
    - PostgreSQL
    - MaterializedPostgreSQL
    or False otherwise.
    """
    return any((
        db_engine == 'MySQL',
        db_engine == 'MaterializedMySQL',
        db_engine == 'PostgreSQL',
        db_engine == 'MaterializedPostgreSQL',
    ))


def is_atomic_db_engine(db_engine: str) -> bool:
    """
    Return True if database engine is Atomic, or False otherwise.
    """
    return db_engine == 'Atomic'


def is_replicated_db_engine(db_sql: str) -> bool:
    """
    Return True if database engine is Replicaed, or False otherwise.
    """
    return db_sql.find('Replicated') != -1


def to_attach_query(create_query: str) -> str:
    """
    Convert CREATE table query to ATTACH one.
    """
    return re.sub('^CREATE', 'ATTACH', create_query)


def rewrite_table_schema(table: Table,
                         force_non_replicated_engine: bool = False,
                         override_replica_name: str = None,
                         add_uuid: bool = False,
                         inner_uuid: str = None) -> None:
    """
    Rewrite table schema.
    """
    if force_non_replicated_engine:
        create_statement = table.create_statement
        match = re.search(r"(?P<replicated>Replicated)\S{0,20}MergeTree\((?P<params>('[^']+', '[^']+'(,\s*|))|)",
                          create_statement)
        if match:
            params = match.group('params')
            if len(params) > 0:
                create_statement = create_statement.replace(params, '').replace(match.group('replicated'), '')
                create_statement = create_statement.replace('MergeTree()', 'MergeTree')
                table.create_statement = create_statement
            if is_replicated(table.engine):
                table.engine = table.engine.replace('Replicated', '')

    if override_replica_name:
        create_statement = table.create_statement
        match = re.search(r"Replicated\S{0,20}MergeTree\('[^']+', (?P<replica>\'\S+\')", create_statement)
        if match:
            table.create_statement = create_statement.replace(match.group('replica'), f"'{override_replica_name}'")

    if add_uuid:
        _add_uuid(table, inner_uuid)


def _add_uuid(table: Table, inner_uuid: str = None) -> None:
    if is_view(table.engine):
        inner_uuid_clause = f"TO INNER UUID '{inner_uuid}'" if inner_uuid else ''
        table.create_statement = re.sub(
            f"^CREATE (?P<view_type>((MATERIALIZED|LIVE) )?VIEW) (?P<view_name>`?{table.database}`?.`?{table.name}`?) ",
            f"CREATE \\g<view_type> \\g<view_name> UUID '{table.uuid}' {inner_uuid_clause} ", table.create_statement)
    else:
        # CREATE TABLE <db-name>.<table-name> $ (...)
        # UUID clause is inserted to $ place.
        create_statement = table.create_statement
        engine_index = create_statement.upper().find('ENGINE')
        brace_index = create_statement.find('(')
        if engine_index != -1 and brace_index != -1:
            index = min(engine_index, brace_index)
        else:
            index = max(engine_index, brace_index)

        assert index != -1, f'Unable to find UUID insertion point for the following create table statement: ' \
                            f'{create_statement} '
        table.create_statement = create_statement[:index] + f"UUID '{table.uuid}' " + create_statement[index:]
