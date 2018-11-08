"""
Clickhouse-control classes module
"""

import logging
import os
import shutil
from hashlib import md5
from types import SimpleNamespace
from typing import List, Optional
from urllib.parse import quote

from ch_backup.clickhouse.client import ClickhouseClient
from ch_backup.exceptions import ClickhouseBackupError
from ch_backup.util import chown_dir_contents, strip_query

GET_TABLES_ORDERED_SQL = strip_query("""
    SELECT name
    FROM system.tables
    WHERE engine like '%MergeTree%'
      AND database = '{db_name}'
      AND (empty({tables}) OR has(cast({tables}, 'Array(String)'), name))
    ORDER BY metadata_modification_time
    FORMAT JSON
""")

PART_ATTACH_SQL = strip_query("""
    ALTER TABLE `{db_name}`.`{table_name}`
    ATTACH PART '{part_name}'
""")

PARTITION_FREEZE_SQL = strip_query("""
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

SHOW_CREATE_TABLE_SQL = strip_query("""
    SHOW CREATE TABLE `{db_name}`.`{table_name}`
    FORMAT TSVRaw
""")

GET_TABLE_PARTITIONS_SQL = strip_query("""
    SELECT DISTINCT partition
    FROM system.parts
    WHERE active
      AND database == '{db_name}'
      AND table == '{table_name}'
    FORMAT JSON
""")

GET_VERSION_SQL = strip_query("""
    SELECT value
    FROM system.build_options
    WHERE name = 'VERSION_DESCRIBE'
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


class FreezedPartition(SimpleNamespace):
    """
    Freezed table partition.
    """

    def __init__(self, partition: Partition, shadow_increment: str) -> None:
        super().__init__()
        self.database = partition.database
        self.table = partition.table
        self.name = partition.name
        self.shadow_increment = shadow_increment


class FreezedPart(SimpleNamespace):
    """
    Part of freezed table partition.
    """

    def __init__(self, fpartition: FreezedPartition, name: str, path: str,
                 checksum: str, size: int):
        super().__init__()
        self.database = fpartition.database
        self.table = fpartition.table
        self.partition = fpartition.name
        self.name = name
        self.path = path
        self.checksum = checksum
        self.size = size


class ClickhouseCTL:
    """
    Clickhouse control tool
    """

    def __init__(self, config: dict) -> None:
        self._config = config
        self._ch_client = ClickhouseClient(config)

        self.root_data_path = config['data_path']
        self.data_path = os.path.join(self.root_data_path, 'data')
        self.metadata_path = os.path.join(self.root_data_path, 'metadata')
        self.shadow_data_path = os.path.join(self.root_data_path, 'shadow')

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
        query_sql = PART_ATTACH_SQL\
            .format(db_name=db_name,
                    table_name=table_name,
                    part_name=part_name)

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

    def freeze_partition(self, partition: Partition) -> FreezedPartition:
        """
        Freeze the specified partition.
        """
        query_sql = PARTITION_FREEZE_SQL.format(
            db_name=partition.database,
            table_name=partition.table,
            partition_name=partition.name)

        self._ch_client.query(query_sql)

        return FreezedPartition(partition, self._get_shadow_increment())

    def get_freezed_parts(self,
                          fpartition: FreezedPartition) -> List[FreezedPart]:
        """
        Get parts of freezed partition.
        """
        path = os.path.join(self.shadow_data_path, fpartition.shadow_increment,
                            'data', self._quote(fpartition.database),
                            self._quote(fpartition.table))

        if not os.path.exists(path):
            logging.debug('Freezed partition %s is empty', fpartition.name)
            return []

        fparts = []  # type: List[FreezedPart]
        for part in os.listdir(path):
            part_path = os.path.join(path, part)
            checksum = self._get_part_checksum(part_path)
            size = self._get_part_size(part_path)
            fparts.append(
                FreezedPart(fpartition, part, part_path, checksum, size))

        return fparts

    def remove_freezed_data(self) -> None:
        """
        Remove all freezed partitions.
        """
        if not self.shadow_data_path.startswith(self._config['data_path']):
            raise ClickhouseBackupError(
                'Trying to drop directory outside clickhouse data path')

        logging.debug('Removing shadow data path: %s', self.shadow_data_path)
        shutil.rmtree(self.shadow_data_path, ignore_errors=True)

    def get_all_databases(
            self, exclude_dbs: Optional[List[str]] = None) -> List[str]:
        """
        Get list of all databases
        """
        if not exclude_dbs:
            exclude_dbs = []

        result = []  # type: List[str]
        ch_resp = self._ch_client.query(SHOW_DATABASES_SQL)
        if 'data' in ch_resp:
            result = [
                row['name'] for row in ch_resp['data']
                if row['name'] not in exclude_dbs
            ]

        return result

    def get_all_db_tables(self, db_name: str) -> List[str]:
        """
        Get unordered list of all database tables
        """
        query_sql = SHOW_TABLES_SQL.format(db_name=db_name)
        logging.debug('Fetching all %s tables: %s', db_name, query_sql)
        ch_resp = self._ch_client.query(query_sql)
        return [row['name'] for row in ch_resp.get('data', [])]

    def get_table_schema(self, db_name: str, table_name: str) -> str:
        """
        Return table schema (CREATE TABLE query)
        """
        query_sql = SHOW_CREATE_TABLE_SQL.format(
            db_name=db_name, table_name=table_name)
        return self._ch_client.query(query_sql)

    def get_tables_ordered(self,
                           db_name: str,
                           tables: Optional[List[str]] = None) -> List[str]:
        """
        Get ordered by mtime list of all database tables
        """
        result = []  # type: List[str]
        query_sql = GET_TABLES_ORDERED_SQL.format(
            db_name=db_name, tables=tables or [])
        logging.debug('Fetching all %s tables ordered: %s', db_name, query_sql)
        ch_resp = self._ch_client.query(query_sql)
        if 'data' in ch_resp:
            result = [row['name'] for row in ch_resp['data']]
        return result

    def get_partitions(self, database: str, table: str) -> List[Partition]:
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
        return os.path.join(self.data_path, self._quote(db_name),
                            self._quote(table_name), 'detached', part_name)

    def get_detached_abs_path(self, db_name: str, table_name: str) -> str:
        """
        Get filesystem absolute path of detached table parts
        """
        return os.path.join(self.data_path, self._quote(db_name),
                            self._quote(table_name), 'detached')

    def get_db_sql_abs_path(self, db_name: str) -> str:
        """
        Get filesystem absolute path of database meta sql
        """
        return os.path.join(self.root_data_path,
                            self.get_db_sql_rel_path(db_name))

    def get_version(self) -> str:
        """
        Get ClickHouse version
        """
        return self._ch_client.query(GET_VERSION_SQL)

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

    @staticmethod
    def _quote(value: str) -> str:
        return quote(
            value, safe='').translate({
                ord('.'): '%2E',
                ord('-'): '%2D',
            })

    def _get_shadow_increment(self) -> str:
        file_path = os.path.join(self.shadow_data_path, 'increment.txt')
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
