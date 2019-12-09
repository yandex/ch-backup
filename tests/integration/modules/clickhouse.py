"""
ClickHouse client.
"""

import logging
from datetime import datetime, timedelta
from typing import Any, Sequence, Tuple, Union
from urllib.parse import urljoin

from requests import HTTPError, Session

from . import docker
from .typing import ContextT
from .utils import generate_random_string

DB_COUNT = 2
TABLE_COUNT = 2
ROWS_COUNT = 3


class ClickhouseClient:
    """
    ClickHouse Client.
    """
    def __init__(self, context: ContextT, node_name: str) -> None:
        protocol = 'http'
        port = context.conf['projects']['clickhouse']['expose'][protocol]
        host, exposed_port = docker.get_exposed_port(docker.get_container(context, node_name), port)

        self._session = Session()
        self._url = f'{protocol}://{host}:{exposed_port}'
        self._timeout = 30

    def ping(self) -> None:
        """
        Ping ClickHouse server.
        """
        self._query('GET', url='ping')

    def execute(self, query: str) -> None:
        """
        Execute arbitrary query.
        """
        self._query('POST', query=query)

    def get_response(self, query: str) -> str:
        """
        Execute arbitrary query and return result
        """
        return str(self._query('POST', query=query))

    def get_version(self) -> str:
        """
        Get ClickHouse version.
        """
        return self._query('GET', 'SELECT version()')

    def init_schema(self) -> None:
        """
        Create test schema.
        """
        for db_num in range(1, DB_COUNT + 1):
            db_name = f'test_db_{db_num:02d}'
            self._query('POST', f'CREATE DATABASE IF NOT EXISTS {db_name}')
            for table_num in range(1, TABLE_COUNT + 1):
                table_name = f'test_table_{table_num:02d}'
                query = f"""
                    CREATE TABLE IF NOT EXISTS `{db_name}`.`{table_name}` (
                        date Date,
                        datetime DateTime,
                        int_num UInt32,
                        str String
                    )
                    ENGINE = MergeTree(date, int_num, 8192)
                    """
                self._query('POST', query)

    def init_data(self, mark: str) -> None:
        """
        Fill test schema with data
        """
        for db_num in range(1, DB_COUNT + 1):
            db_name = self._get_test_db_name(db_num)
            for table_num in range(1, TABLE_COUNT + 1):
                rows = []
                table_name = self._get_test_table_name(table_num)
                for row_num in range(1, ROWS_COUNT + 1):
                    rows.append(', '.join(self._gen_record(row_num=row_num, str_prefix=mark)))

                self._query('POST', f'INSERT INTO {db_name}.{table_name} FORMAT CSV', data='\n'.join(rows))

    def get_all_user_data(self) -> Tuple[int, dict]:
        """
        Retrieve all user data.
        """
        user_data = {}
        rows_count = 0
        for db_name, table_name, columns in self._get_all_user_tables():
            query = f"""
                SELECT *
                FROM `{db_name}`.`{table_name}`
                ORDER BY {','.join(columns)}
                FORMAT JSONCompact
                """
            table_data = self._query('GET', query)
            user_data['.'.join([db_name, table_name])] = table_data['data']
            rows_count += table_data['rows']
        return rows_count, user_data

    def get_all_user_schemas(self) -> dict:
        """
        Retrieve DDL for user schemas.
        """
        all_tables_desc = {}
        for db_name, table_name, _ in self._get_all_user_tables():
            query = f"""
                DESCRIBE `{db_name}`.`{table_name}`
                FORMAT JSONCompact
                """
            table_data = self._query('GET', query)
            all_tables_desc[(db_name, table_name)] = table_data['data']
        return all_tables_desc

    def get_all_user_databases(self) -> Sequence[str]:
        """
        Get user databases.
        """
        query = """
            SELECT name
            FROM system.databases
            WHERE name NOT IN ('system')
            FORMAT JSONCompact
            """

        databases = self._query('GET', query)['data']
        return [db[0] for db in databases]

    def drop_database(self, db_name: str) -> None:
        """
        Drop database.
        """
        self._query('POST', f'DROP DATABASE {db_name}')

    def drop_test_table(self, db_num: int, table_num: int) -> None:
        """
        Drop test table.
        """
        db_name = self._get_test_db_name(db_num)
        table_name = self._get_test_table_name(table_num)
        self._query('POST', f'DROP TABLE {db_name}.{table_name}')

    def _get_all_user_tables(self) -> dict:
        query = """
            SELECT
                database,
                table,
                groupArray(name) AS columns
            FROM system.columns
            WHERE database NOT IN ('system')
            GROUP BY database, table
            ORDER BY database, table
            FORMAT JSONCompact
            """
        return self._query('GET', query)['data']

    def _query(self, method: str, query: str = None, url: str = None, data: Union[bytes, str] = None) -> Any:
        if url:
            url = urljoin(self._url, url)
        else:
            url = self._url

        if isinstance(data, str):
            data = data.encode()

        params = {}
        if query:
            params['query'] = query

        try:
            logging.debug('Executing ClickHouse query: %s', query)
            response = self._session.request(method, url, params=params, data=data, timeout=self._timeout)

            response.raise_for_status()
        except HTTPError as e:
            logging.critical('Error while performing request: %s', e.response.text)
            raise

        try:
            return response.json()
        except ValueError:
            return str.strip(response.text)

    @staticmethod
    def _get_test_db_name(db_num: int) -> str:
        """
        Get test database name
        """
        return f'test_db_{db_num:02d}'

    @staticmethod
    def _get_test_table_name(table_num: int) -> str:
        """
        Get test table name
        """
        return f'test_table_{table_num:02d}'

    @staticmethod
    def _gen_record(row_num=0, day_diff=None, str_len=5, str_prefix=None):
        """
        Generate test record.
        """
        if day_diff is None:
            day_diff = {'days': 0}
        if str_prefix is None:
            str_prefix = ''
        else:
            str_prefix = f'{str_prefix}_'

        rand_str = generate_random_string(str_len)

        dt_now = datetime.utcnow() - timedelta(**day_diff)
        row = (dt_now.strftime('%Y-%m-%d'), dt_now.strftime('%Y-%m-%d %H:%M:%S'), str(row_num),
               f'{str_prefix}{rand_str}')

        return row
