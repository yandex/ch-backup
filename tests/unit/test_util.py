"""
Unit test for util module.
"""

from ch_backup.util import strip_query


class TestStripQuery:
    """
    Tests for strip_query() function.
    """

    def test_query_without_newlines(self):
        input_query = 'SELECT 42 FROM {db_name}.{table_name}'
        expected = 'SELECT 42 FROM {db_name}.{table_name}'
        assert strip_query(input_query) == expected

    def test_query_with_newlines(self):
        input_query = '''
            SHOW TABLES
            FROM {db_name}
            FORMAT JSON
        '''
        expected = 'SHOW TABLES FROM {db_name} FORMAT JSON'
        assert strip_query(input_query) == expected
