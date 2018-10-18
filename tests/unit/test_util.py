"""
Unit test for util module.
"""

from ch_backup.util import strip_query

MULTILINE_SQL = """
    SHOW TABLES
    FROM {db_name}
    FORMAT JSON
"""

MULTILINE_SQL_STRIPPED = 'SHOW TABLES FROM {db_name} FORMAT JSON'


class TestStripQuery:
    """
    Tests for strip_query() function.
    """

    def test_query_without_newlines(self):
        assert (strip_query('SELECT 42 FROM {db_name}.{table_name}') ==
                'SELECT 42 FROM {db_name}.{table_name}')

    def test_query_with_newlines(self):
        assert strip_query(MULTILINE_SQL) == MULTILINE_SQL_STRIPPED
