"""
Clickhouse-control classes module
"""

import os
import shutil
from hashlib import md5
from types import SimpleNamespace
from typing import List, Optional, Sequence
from urllib.parse import quote

from pkg_resources import parse_version

from ch_backup import logging
from ch_backup.clickhouse.client import ClickhouseClient
from ch_backup.exceptions import ClickhouseBackupError
from ch_backup.util import chown_dir_contents, retry, strip_query

GET_TABLES_ORDERED_SQL = strip_query("""
    SELECT name
    FROM system.tables
    WHERE engine like '%MergeTree%'
      AND database = '{db_name}'
      AND (empty({tables}) OR has(cast({tables}, 'Array(String)'), name))
    ORDER BY metadata_modification_time
    FORMAT JSON
""")

CHECK_TABLE_SQL = strip_query("""
    SELECT countIf(database = '{db_name}' AND name = '{table_name}')
    FROM system.tables
    FORMAT TSVRaw
""")

PART_ATTACH_SQL = strip_query("""
    ALTER TABLE `{db_name}`.`{table_name}`
    ATTACH PART '{part_name}'
""")

FREEZE_TABLE_SQL = strip_query("""
    ALTER TABLE `{db_name}`.`{table_name}`
    FREEZE
""")

FREEZE_PARTITION_SQL = strip_query("""
    ALTER TABLE `{db_name}`.`{table_name}`
    FREEZE PARTITION {partition_name}
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

SHOW_CREATE_DATABASE_SQL = strip_query("""
    SHOW CREATE DATABASE `{db_name}`
    FORMAT TSVRaw
""")

SHOW_CREATE_TABLE_SQL = strip_query("""
    SHOW CREATE TABLE `{db_name}`.`{table_name}`
    FORMAT TSVRaw
""")

GET_TABLE_PARTITIONS_SQL = strip_query("""
    SELECT DISTINCT partition
    FROM system.parts
    WHERE active
      AND database = '{db_name}'
      AND table = '{table_name}'
    FORMAT JSON
""")

GET_VERSION_SQL = strip_query("""
    SELECT version()
    FORMAT TSVRaw
""")


class Partition(SimpleNamespace):
    """
    Table partition.
    """

    def __init__(self, database: str, table: str, name: str) -> None:
        super().__init__()
        self.database = database
        self.table = table
        self.name = name


class FreezedPart(SimpleNamespace):
    """
    Freezed data part.
    """

    def __init__(self, database: str, table: str, name: str, path: str,
                 checksum: str, size: int):
        super().__init__()
        self.database = database
        self.table = table
        self.name = name
        self.path = path
        self.checksum = checksum
        self.size = size


class ClickhouseCTL:
    """
    ClickHouse control tool.
    """

    def __init__(self, config: dict) -> None:
        self._config = config
        self._ch_client = ClickhouseClient(config)
        root_data_path = config['data_path']
        self._data_path = os.path.join(root_data_path, 'data')
        self._shadow_data_path = os.path.join(root_data_path, 'shadow')
        self._ch_version = self._ch_client.query(GET_VERSION_SQL)

    def chown_attach_part(self, db_name: str, table_name: str,
                          part_name: str) -> None:
        """
        Chown detached part files
        """
        part_path = self.get_detached_part_abs_path(db_name, table_name,
                                                    part_name)
        self.chown_dir_contents(part_path)
        self.attach_part(db_name, table_name, part_name)

    def chown_dettached_table_parts(self, db_name: str,
                                    table_name: str) -> None:
        """
        Chown detached table files
        """
        dettached_path = self.get_detached_abs_path(db_name, table_name)
        self.chown_dir_contents(dettached_path)

    def attach_part(self, db_name: str, table_name: str,
                    part_name: str) -> None:
        """
        Attach part to database.table from dettached dir
        """
        query_sql = PART_ATTACH_SQL.format(
            db_name=db_name, table_name=table_name, part_name=part_name)

        logging.debug('Attaching partition: %s', query_sql)
        self._ch_client.query(query_sql)

    def chown_dir_contents(self, dir_path: str) -> None:
        """
        Chown directory contents to configured owner:group
        """
        if not dir_path.startswith(self._config['data_path']):
            raise ClickhouseBackupError(
                'Trying to chown directory outside clickhouse data path')
        chown_dir_contents(self._config['user'], self._config['group'],
                           dir_path)

    def freeze_table(self, db_name: str,
                     table_name: str) -> Sequence[FreezedPart]:
        """
        Make snapshot of the specified table.
        """
        if self._match_ch_version(min_version='18.16.0'):
            return self._freeze_table(db_name, table_name)

        return self._freeze_table_compat(db_name, table_name)

    def remove_freezed_data(self) -> None:
        """
        Remove all freezed partitions.
        """
        self._remove_shadow_data(self._shadow_data_path)

    def remove_freezed_part(self, part: FreezedPart) -> None:
        """
        Remove the freezed part.
        """
        self._remove_shadow_data(part.path)

    def get_all_databases(self, exclude_dbs: Optional[Sequence[str]] = None) \
            -> Sequence[str]:
        """
        Get list of all databases
        """
        if not exclude_dbs:
            exclude_dbs = []

        result: List[str] = []
        ch_resp = self._ch_client.query(SHOW_DATABASES_SQL)
        if 'data' in ch_resp:
            result = [
                row['name'] for row in ch_resp['data']
                if row['name'] not in exclude_dbs
            ]

        return result

    def get_all_db_tables(self, db_name: str) -> Sequence[str]:
        """
        Get unordered list of all database tables
        """
        query_sql = SHOW_TABLES_SQL.format(db_name=db_name)
        logging.debug('Fetching all %s tables: %s', db_name, query_sql)
        ch_resp = self._ch_client.query(query_sql)
        return [row['name'] for row in ch_resp.get('data', [])]

    def does_table_exist(self, db_name: str, table_name: str) -> bool:
        """
        Return True if the specified table exists.
        """
        query_sql = CHECK_TABLE_SQL.format(
            db_name=db_name, table_name=table_name)
        return bool(int(self._ch_client.query(query_sql)))

    def get_database_schema(self, db_name: str) -> str:
        """
        Return database schema (CREATE DATABASE query).
        """
        query_sql = SHOW_CREATE_DATABASE_SQL.format(db_name=db_name)
        return self._ch_client.query(query_sql)

    def get_table_schema(self, db_name: str, table_name: str) -> str:
        """
        Return table schema (CREATE TABLE query).
        """
        query_sql = SHOW_CREATE_TABLE_SQL.format(
            db_name=db_name, table_name=table_name)
        return self._ch_client.query(query_sql)

    def get_tables_ordered(self,
                           db_name: str,
                           tables: Optional[Sequence[str]] = None) \
            -> Sequence[str]:
        """
        Get ordered by mtime list of all database tables
        """
        result: List[str] = []
        query_sql = GET_TABLES_ORDERED_SQL.format(
            db_name=db_name, tables=tables or [])
        logging.debug('Fetching all %s tables ordered: %s', db_name, query_sql)
        ch_resp = self._ch_client.query(query_sql)
        if 'data' in ch_resp:
            result = [row['name'] for row in ch_resp['data']]
        return result

    def get_partitions(self, database: str, table: str) -> Sequence[Partition]:
        """
        Get dict with all table parts
        """
        query_sql = GET_TABLE_PARTITIONS_SQL.format(
            db_name=database, table_name=table)
        logging.debug('Fetching all %s table parts: %s', database, query_sql)

        data = self._ch_client.query(query_sql)['data']
        return [Partition(database, table, item['partition']) for item in data]

    def restore_meta(self, query_sql: str) -> None:
        """
        Restore database or table meta sql
        """
        logging.debug('Restoring meta sql: %s', query_sql)
        self._ch_client.query(query_sql)

    def get_detached_part_abs_path(self, db_name: str, table_name: str,
                                   part_name: str) -> str:
        """
        Get filesystem absolute path of detached part
        """
        return os.path.join(self._data_path, self._quote(db_name),
                            self._quote(table_name), 'detached', part_name)

    def get_detached_abs_path(self, db_name: str, table_name: str) -> str:
        """
        Get filesystem absolute path of detached table parts
        """
        return os.path.join(self._data_path, self._quote(db_name),
                            self._quote(table_name), 'detached')

    def get_version(self) -> str:
        """
        Get ClickHouse version
        """
        return self._ch_version

    @classmethod
    def get_db_sql_rel_path(cls, db_name: str) -> str:
        """
        Get filesystem relative path of database meta sql
        """
        return os.path.join('metadata', cls._quote(db_name) + '.sql')

    @classmethod
    def get_table_sql_rel_path(cls, db_name: str, table_name: str) -> str:
        """
        Get filesystem relative path of database.table meta sql
        """
        return os.path.join('metadata', cls._quote(db_name),
                            cls._quote(table_name) + '.sql')

    def _freeze_table(self, db_name: str,
                      table_name: str) -> Sequence[FreezedPart]:
        """
        Implementation of freeze_table function using FREEZE command syntax for
        the whole table that is available starting from the version 18.16.
        """
        query_sql = FREEZE_TABLE_SQL.format(
            db_name=db_name, table_name=table_name)

        self._ch_client.query(query_sql)

        return self._get_freezed_parts(db_name, table_name)

    def _freeze_table_compat(self, db_name: str,
                             table_name: str) -> Sequence[FreezedPart]:
        """
        Implementation of freeze_table function for versions prior to 18.16.
        """
        freezed_parts: List[FreezedPart] = []
        for partition in self.get_partitions(db_name, table_name):
            query_sql = FREEZE_PARTITION_SQL.format(
                db_name=db_name,
                table_name=table_name,
                partition_name=partition.name)

            self._ch_client.query(query_sql)

            freezed_parts.extend(self._get_freezed_parts(db_name, table_name))

        return freezed_parts

    def _get_freezed_parts(self, db_name: str,
                           table_name: str) -> Sequence[FreezedPart]:
        path = os.path.join(self._shadow_data_path,
                            self._get_shadow_increment(), 'data',
                            self._quote(db_name), self._quote(table_name))

        if not os.path.exists(path):
            logging.debug('Shadow path %s is empty', path)
            return []

        freezed_parts: List[FreezedPart] = []
        for part in os.listdir(path):
            part_path = os.path.join(path, part)
            checksum = self._get_part_checksum(part_path)
            size = self._get_part_size(part_path)
            freezed_parts.append(
                FreezedPart(db_name, table_name, part, part_path, checksum,
                            size))

        return freezed_parts

    @retry(OSError)
    def _remove_shadow_data(self, path: str) -> None:
        assert path.startswith(self._shadow_data_path)

        logging.debug('Removing shadow data: %s', path)
        try:
            shutil.rmtree(path)
        except FileNotFoundError:
            pass

    def _match_ch_version(self, min_version: str) -> bool:
        return parse_version(self._ch_version) >= parse_version(min_version)

    @staticmethod
    def _quote(value: str) -> str:
        return quote(
            value, safe='').translate({
                ord('.'): '%2E',
                ord('-'): '%2D',
            })

    def _get_shadow_increment(self) -> str:
        file_path = os.path.join(self._shadow_data_path, 'increment.txt')
        with open(file_path, 'r') as file:
            return file.read().strip()

    @staticmethod
    def _get_part_checksum(part_path: str) -> str:
        with open(os.path.join(part_path, 'checksums.txt'), 'rb') as f:
            return md5(f.read()).hexdigest()  # nosec

    @staticmethod
    def _get_part_size(part_path: str) -> int:
        size = 0
        for file in os.listdir(part_path):
            size += os.path.getsize(os.path.join(part_path, file))
        return size
