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
    return engine in ('View', 'MaterializedView')


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


def to_attach_query(create_query: str) -> str:
    """
    Convert CREATE table query to ATTACH one.
    """
    return re.sub('^CREATE', 'ATTACH', create_query)


def rewrite_table_schema(table: Table,
                         force_non_replicated_engine: bool = False,
                         override_replica_name: str = None) -> None:
    """
    Rewrite table schema.
    """
    table_schema = table.create_statement
    table_engine = table.engine

    if force_non_replicated_engine:
        match = re.search(r"(?P<replicated>Replicated)\S{0,20}MergeTree\((?P<params>('[^']+', '[^']+'(,\s*|))|)",
                          table_schema)
        if match:
            params = match.group('params')
            if len(params) > 0:
                table_schema = table_schema.replace(params, '').replace(match.group('replicated'), '')
                table_schema = table_schema.replace('MergeTree()', 'MergeTree')
            if is_replicated(table_engine):
                table_engine = table_engine.replace('Replicated', '')

    if override_replica_name:
        match = re.search(r"Replicated\S{0,20}MergeTree\('[^']+', (?P<replica>\'\S+\')", table_schema)
        if match:
            table_schema = table_schema.replace(match.group('replica'), f"'{override_replica_name}'")

    table.create_statement = table_schema
    table.engine = table_engine
