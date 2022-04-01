"""
Utilities for schema manipulation.
"""


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
