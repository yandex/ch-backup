"""
utils unit tests
"""

from ch_backup.util import strip_query

MULTILINE_SQL = """
    SHOW TABLES
    FROM {db_name}
    FORMAT JSON
"""

MULTILINE_SQL_STRIPPED = 'SHOW TABLES FROM {db_name} FORMAT JSON'


class Test_strip_query(object):  # pylint: disable=invalid-name
    # pylint: disable=missing-docstring, no-self-use, invalid-name

    def test_return_same_query_for_query_without_endlines(self):
        assert strip_query('SELECT 42 FROM {db_name}.{table_name}') ==\
               'SELECT 42 FROM {db_name}.{table_name}'

    def test_remove_endlines(self):
        assert strip_query(MULTILINE_SQL) == MULTILINE_SQL_STRIPPED
