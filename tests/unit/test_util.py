"""
Unit test for util module.
"""
import pytest

from ch_backup.util import retry, strip_query

from . import ExpectedException, UnexpectedException


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


@retry(ExpectedException, max_attempts=3, max_interval=0.1)
def unreliable_function(context):
    context['attempts'] = context.get('attempts', 0) + 1

    if context.get('failure_count', 0) >= context['attempts']:
        raise context.get('exception', ExpectedException)()


class TestRetry:
    """
    Tests for retry() function.
    """

    def test_function_succeeds(self):
        context = {}
        unreliable_function(context)
        assert context['attempts'] == 1

    def test_function_raises_once(self):
        context = {'failure_count': 1}
        unreliable_function(context)
        assert context['attempts'] == 2

    def test_function_always_raises(self):
        context = {'failure_count': 3}
        with pytest.raises(ExpectedException):
            unreliable_function(context)
        assert context['attempts'] == 3

    def test_function_raises_unexpected_exception(self):
        context = {'failure_count': 3, 'exception': UnexpectedException}
        with pytest.raises(UnexpectedException):
            unreliable_function(context)
        assert context['attempts'] == 1
