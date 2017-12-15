"""
Clickhouse-control classes module
"""

import logging
import os
import shutil

import requests

from ch_backup.exceptions import ClickHouseBackupError
from ch_backup.util import chown_dir_contents, strip_query

GET_ALL_DB_TABLES_ORDERED_SQL = strip_query("""
    SELECT name
    FROM system.tables
    WHERE engine like '%MergeTree%' and database = '{db_name}'
    ORDER BY metadata_modification_time
    FORMAT JSON
""")

PART_ATTACH_SQL = strip_query("""
    ALTER TABLE {db_name}.{table_name}
    ATTACH PART '{part_name}'
""")

TABLE_FREEZE_SQL = strip_query("""
    ALTER TABLE {db_name}.{table_name}
    FREEZE PARTITION ''
""")

SHOW_DATABASES_SQL = strip_query("""
    SHOW DATABASES
    FORMAT JSON
""")

SHOW_TABLES_SQL = strip_query("""
    SHOW TABLES
    FROM {db_name}
    FORMAT JSON
""")

GET_ALL_TABLE_PARTS_INFO_SQL = strip_query("""
    SELECT *
    FROM system.parts
    WHERE active AND database == '{db_name}'
    AND table == '{table_name}'
    FORMAT JSON;
""")


class ClickhouseCTL:
    """
    Clickhouse control tool
    """

    def __init__(self, config):
        self._config = config
        self._ch_client = ClickhouseClient(config)

        self.data_path = config['data_path']
        self.metadata_path = self._get_metadata_abs_path(self.data_path)
        self.shadow_data_path = self._get_shadow_data_abs_path(self.data_path)
        self.shadow_data_path_inc = os.path.join(self.shadow_data_path, '1')

    def chown_attach_part(self, db_name, table_name, part_name):
        """
        Chown detached part files
        """

        part_path = self.get_detached_part_abs_path(db_name, table_name,
                                                    part_name)
        self.chown_dir_contents(part_path)
        self.attach_part(db_name, table_name, part_name)

    def chown_dettached_table_parts(self, db_name, table_name):
        """
        Chown detached table files
        """

        dettached_path = self.get_detached_abs_path(db_name, table_name)
        self.chown_dir_contents(dettached_path)

    def attach_part(self, db_name, table_name, part_name):
        """
        Attach part to database.table from dettached dir
        """

        query_sql = PART_ATTACH_SQL\
            .format(db_name=db_name,
                    table_name=table_name,
                    part_name=part_name)

        logging.debug('Attaching partition: %s', query_sql)
        self._ch_client.query(query_sql)

    def chown_dir_contents(self, dir_path):
        """
        Chown directory contents to configured owner:group
        """

        if not dir_path.startswith(self._config['data_path']):
            raise ClickHouseBackupError(
                'Trying to chown directory outside clickhouse data path')
        chown_dir_contents(self._config['user'], self._config['group'],
                           dir_path)

    def freeze_table(self, db_name, table_name):
        """
        Freeze all partitions in specified database.table
        """

        query_sql = TABLE_FREEZE_SQL.format(
            db_name=db_name, table_name=table_name)
        logging.debug('Freezing partition: %s', query_sql)

        return self._ch_client.query(query_sql)

    def remove_shadow_data(self):
        """
        Recursively delete shadow data path
        """

        if not self.shadow_data_path.startswith(self._config['data_path']):
            raise ClickHouseBackupError(
                'Trying to drop directory outside clickhouse data path')

        logging.debug('Removing shadow data path: %s', self.shadow_data_path)
        shutil.rmtree(self.shadow_data_path, ignore_errors=True)

    def get_all_databases(self, exclude_dbs=None):
        """
        Get list of all databases
        """

        result = []
        ch_resp = self._ch_client.query(SHOW_DATABASES_SQL)
        if 'data' in ch_resp:
            result = [
                row['name'] for row in ch_resp['data']
                if row['name'] not in exclude_dbs
            ]
        return result

    def get_all_db_tables(self, db_name):
        """
        Get unordered list of all database tables
        """

        result = []
        query_sql = SHOW_TABLES_SQL.format(db_name=db_name)
        logging.debug('Fetching all %s tables: %s', db_name, query_sql)
        ch_resp = self._ch_client.query(query_sql)
        if 'data' in ch_resp:
            result = [row['name'] for row in ch_resp['data']]
        return result

    def get_all_db_tables_ordered(self, db_name):
        """
        Get ordered by mtime list of all database tables
        """

        result = []
        query_sql = GET_ALL_DB_TABLES_ORDERED_SQL.format(db_name=db_name)
        logging.debug('Fetching all %s tables ordered: %s', db_name, query_sql)
        ch_resp = self._ch_client.query(query_sql)
        if 'data' in ch_resp:
            result = [row['name'] for row in ch_resp['data']]
        return result

    def get_all_table_parts_info(self, db_name, table_name):
        """
        Get dict with all table parts
        """

        query_sql = GET_ALL_TABLE_PARTS_INFO_SQL.format(
            db_name=db_name, table_name=table_name)
        logging.debug('Fetching all %s table parts: %s', db_name, query_sql)

        return self._ch_client.query(query_sql)['data']

    def restore_meta(self, query_sql):
        """
        Restore database or table meta sql
        """

        logging.debug('Restoring meta sql: %s', query_sql)
        return self._ch_client.query(query_sql)

    def get_detached_part_abs_path(self, db_name, table_name, part_name):
        """
        Get filesystem absolute path of detached part
        """

        return os.path.join(self._config['data_path'], 'data', db_name,
                            table_name, 'detached', part_name)

    def get_detached_abs_path(self, db_name, table_name):
        """
        Get filesystem absolute path of detached table parts
        """

        return os.path.join(self._config['data_path'], 'data', db_name,
                            table_name, 'detached')

    def get_db_sql_abs_path(self, db_name):
        """
        Get filesystem absolute path of database meta sql
        """

        return os.path.join(self.data_path, self.get_db_sql_rel_path(db_name))

    def get_table_sql_abs_path(self, db_name, table_name):
        """
        Get filesystem absolute path of database.table meta sql
        """

        return os.path.join(self.data_path,
                            self.get_table_sql_rel_path(db_name, table_name))

    def get_shadow_part_abs_path(self, db_name, table_name, part_name):
        """
        Get freezed part absolute path
        """

        return os.path.join(self.shadow_data_path_inc, 'data', db_name,
                            table_name, part_name)

    @staticmethod
    def get_db_sql_rel_path(db_name):
        """
        Get filesystem relative path of database meta sql
        """

        return os.path.join(
            'metadata', '{db_name}.sql'.format(db_name=db_name))

    @staticmethod
    def get_table_sql_rel_path(db_name, table_name):
        """
        Get filesystem relative path of database.table meta sql
        """

        return os.path.join(
            'metadata',
            '{db_name}'.format(db_name=db_name),
            '{table_name}.sql'.format(table_name=table_name))

    @staticmethod
    def _get_metadata_abs_path(data_path):
        """
        Get filesystem metadata dir abs path
        """

        return os.path.join(data_path, 'metadata')

    @staticmethod
    def _get_shadow_data_abs_path(data_path):
        """
        Get filesystem metadata dir abs path
        """

        return os.path.join(data_path, 'shadow')


class ClickhouseClient:
    """
    Simple clickhouse client
    """

    def __init__(self, config):
        self._config = config
        self._timeout = int(config.get('timeout', 3))
        self._query_url = '{proto}://{host}:{port}/?query={query}'. \
            format(
                proto=config.get('proto', 'http'),
                host=config.get('host', 'localhost'),
                port=config.get('port', 8123),
                query='{query}')

    def query(self, query_str, post_data=None, timeout=None):
        """
        Perform query to configured clickhouse endpoint
        """

        if timeout is None:
            timeout = self._timeout

        query_url = self._query_url.format(query=query_str)
        logging.debug('Clickhouse request url: %s', query_url)
        http_response = requests.post(
            query_url, data=post_data, timeout=timeout)

        try:
            http_response.raise_for_status()
        except requests.HTTPError:
            logging.critical('Error while performing request: %s',
                             http_response.text)
            raise

        try:
            return http_response.json()
        except ValueError:
            return {}
