"""
Clickhouse-control classes module
"""

import os
import shutil
from hashlib import md5
from tarfile import BLOCKSIZE  # type: ignore
from typing import Any, Dict, List, Optional, Sequence, Union

from pkg_resources import parse_version

from ch_backup import logging
from ch_backup.backup.metadata import TableMetadata
from ch_backup.backup.restore_context import RestoreContext
from ch_backup.clickhouse.client import ClickhouseClient
from ch_backup.clickhouse.models import Disk, FreezedPart, Table
from ch_backup.clickhouse.schema import (is_distributed, is_external_engine, is_replicated, is_view, to_attach_query)
from ch_backup.util import (chown_dir_contents, escape, list_dir_files, retry, strip_query)

GET_TABLES_SQL = strip_query("""
    SELECT
        database,
        name,
        engine,
        engine_full,
        create_table_query,
        data_paths,
        uuid
    FROM system.tables
    WHERE (empty('{db_name}') OR database = '{db_name}')
      AND (empty({table_names}) OR has(cast({table_names}, 'Array(String)'), name))
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

TABLE_ATTACH_SQL = strip_query("""
    ATTACH TABLE `{db_name}`.`{table_name}`
""")

FREEZE_TABLE_SQL = strip_query("""
    ALTER TABLE `{db_name}`.`{table_name}`
    FREEZE WITH NAME '{backup_name}'
""")

SYSTEM_UNFREEZE_SQL = strip_query("""
    SYSTEM UNFREEZE WITH NAME '{backup_name}'
""")

DROP_TABLE_IF_EXISTS_SQL = strip_query("""
    DROP TABLE IF EXISTS `{db_name}`.`{table_name}` NO DELAY
""")

DROP_DATABASE_IF_EXISTS_SQL = strip_query("""
    DROP DATABASE IF EXISTS `{db_name}`
""")

DROP_UDF_SQL = strip_query("""
    DROP FUNCTION `{udf_name}`
""")

RESTORE_REPLICA_SQL = strip_query("""
    SYSTEM RESTORE REPLICA `{db_name}`.`{table_name}`
""")

GET_DATABASES_SQL = strip_query("""
    SELECT
        name
    FROM system.databases
    WHERE name NOT IN ('system', '_temporary_and_external_tables', 'information_schema', 'INFORMATION_SCHEMA')
    FORMAT JSON
""")

SHOW_CREATE_DATABASE_SQL = strip_query("""
    SHOW CREATE DATABASE `{db_name}`
    FORMAT TSVRaw
""")

GET_DATABASE_ENGINE = strip_query("""
    SELECT engine FROM system.databases WHERE name='{db_name}'
    FORMAT TSVRaw
""")

GET_DATABASE_METADATA_PATH = strip_query("""
    SELECT metadata_path FROM system.databases WHERE name='{db_name}'
    FORMAT JSON
""")

GET_VERSION_SQL = strip_query("""
    SELECT version()
    FORMAT TSVRaw
""")

GET_MACROS_SQL = strip_query("""
    SELECT macro, substitution FROM system.macros
    FORMAT JSON
""")

GET_ACCESS_CONTROL_OBJECTS_SQL = strip_query("""
    SELECT id, name FROM system.{type} WHERE storage='disk' OR storage='local directory' OR storage='replicated'
    FORMAT JSON
""")

GET_DISK_SQL = strip_query("""
    SELECT name, path, type, cache_path FROM system.disks
    WHERE name = '{disk_name}'
    FORMAT JSON
""")

GET_DISKS_SQL = strip_query("""
    SELECT name, path, type FROM system.disks
    ORDER BY length(path) DESC
    FORMAT JSON
""")

GET_DISKS_SQL_22_8 = strip_query("""
    SELECT name, path, type, cache_path FROM system.disks
    ORDER BY length(path) DESC
    FORMAT JSON
""")

GET_UDF_QUERY_SQL = strip_query("""
    SELECT name, create_query FROM system.functions WHERE origin == 'SQLUserDefined'
    FORMAT JSON
""")

RESTART_DISK_SQL = strip_query("""
    SYSTEM RESTART DISK {disk_name}
""")

RELOAD_CONFIG_SQL = strip_query("""
    SYSTEM RELOAD CONFIG
""")


# pylint: disable=too-many-public-methods
class ClickhouseCTL:
    """
    ClickHouse control tool.
    """

    # pylint: disable=too-many-instance-attributes

    def __init__(self, ch_ctl_config: dict, main_config: dict) -> None:
        self._ch_ctl_config = ch_ctl_config
        self._main_config = main_config
        self._root_data_path = self._ch_ctl_config['data_path']
        self._shadow_data_path = os.path.join(self._root_data_path, 'shadow')
        self._timeout = self._ch_ctl_config['timeout']
        self._freeze_timeout = self._ch_ctl_config['freeze_timeout']
        self._system_unfreeze_timeout = self._ch_ctl_config['system_unfreeze_timeout']
        self._restart_disk_timeout = self._ch_ctl_config['restart_disk_timeout']
        self._ch_client = ClickhouseClient(self._ch_ctl_config)
        self._ch_version = self._ch_client.query(GET_VERSION_SQL)
        self._disks = self.get_disks()
        settings = {
            'allow_experimental_live_view': 1,
        }
        if self.ch_version_ge('21.3'):
            settings.update({
                'allow_experimental_database_replicated': 1,
            })
        if self.ch_version_ge('22.7'):
            settings.update({
                'allow_deprecated_database_ordinary': 1,
                'allow_deprecated_syntax_for_merge_tree': 1,
            })
        if self.ch_version_ge('22.8.5'):
            settings.update({
                'allow_experimental_object_type': 1,
                'kafka_disable_num_consumers_limit': 1,
            })
        self._ch_client.settings.update(settings)

    def chown_detached_table_parts(self, table: Table, context: RestoreContext) -> None:
        """
        Change permissions (owner and group) of detached data parts for the
        specified table. New values for permissions are taken from the config.
        """
        for _, disk in table.paths_with_disks:
            detached_path = self._get_table_detached_path(table, disk.name)
            try:
                self.chown_dir(detached_path)
            except FileNotFoundError:
                logging.warning(f'table {table.database}.{table.name} path {detached_path} not found')
                context.add_failed_chown(
                    TableMetadata(escape(table.database), escape(table.name), table.engine, table.uuid), detached_path)

    def attach_part(self, table: Table, part_name: str) -> None:
        """
        Attach data part to the specified table.
        """
        query_sql = PART_ATTACH_SQL.format(db_name=escape(table.database),
                                           table_name=escape(table.name),
                                           part_name=part_name)

        self._ch_client.query(query_sql)

    def attach_table(self, table: Union[TableMetadata, Table]) -> None:
        """
        Attach data part to the specified table.
        """
        query_sql = TABLE_ATTACH_SQL.format(db_name=escape(table.database), table_name=escape(table.name))

        self._ch_client.query(query_sql)

    def freeze_table(self, backup_name: str, table: Table) -> None:
        """
        Make snapshot of the specified table.
        """
        query_sql = FREEZE_TABLE_SQL.format(db_name=escape(table.database),
                                            table_name=escape(table.name),
                                            backup_name=backup_name)
        self._ch_client.query(query_sql, timeout=self._freeze_timeout)

    def system_unfreeze(self, backup_name: str) -> None:
        """
        Unfreeze all partitions from all disks
        """
        if self.ch_version_ge('22.6'):
            query_sql = SYSTEM_UNFREEZE_SQL.format(backup_name=backup_name)
            self._ch_client.query(query_sql, timeout=self._system_unfreeze_timeout)

    def remove_freezed_data(self) -> None:
        """
        Remove all freezed partitions from all local disks.
        """
        for disk in self._disks.values():
            if disk.type == 'local':
                shadow_path = os.path.join(disk.path, 'shadow')
                logging.debug('Removing shadow data: %s', shadow_path)
                self._remove_shadow_data(shadow_path)

    def remove_freezed_part(self, part: FreezedPart) -> None:
        """
        Remove the freezed part.
        """
        logging.debug('Removing freezed part: %s', part.path)
        self._remove_shadow_data(part.path)

    def get_databases(self, exclude_dbs: Optional[Sequence[str]] = None) -> Sequence[str]:
        """
        Get list of all databases
        """
        if not exclude_dbs:
            exclude_dbs = []

        result: List[str] = []
        ch_resp = self._ch_client.query(GET_DATABASES_SQL)
        if 'data' in ch_resp:
            result = [row['name'] for row in ch_resp['data'] if row['name'] not in exclude_dbs]

        return result

    def get_database_schema(self, db_name: str) -> str:
        """
        Return database schema (CREATE DATABASE query).
        """
        query_sql = SHOW_CREATE_DATABASE_SQL.format(db_name=escape(db_name))
        return self._ch_client.query(query_sql)

    def get_database_engine(self, db_name: str) -> str:
        """
        Return database engine.
        """
        query_sql = GET_DATABASE_ENGINE.format(db_name=escape(db_name))
        return self._ch_client.query(query_sql)

    def get_tables(self, db_name: str = None, tables: Optional[Sequence[str]] = None) -> Sequence[Table]:
        """
        Get database tables.
        """
        query_sql = GET_TABLES_SQL.format(
            db_name=escape(db_name) if db_name is not None else '',
            table_names=list(map(escape, tables)) if tables is not None else [])  # type: ignore
        result: List[Table] = []
        for row in self._ch_client.query(query_sql)['data']:
            result.append(self._make_table(row))

        return result

    def get_table(self, db_name: str, table_name: str) -> Optional[Table]:
        """
        Get table by name, returns None if no table has found.
        """
        query_sql = GET_TABLES_SQL.format(db_name=escape(db_name), table_names=[escape(table_name)])
        tables_raw = self._ch_client.query(query_sql)['data']

        if tables_raw:
            return self._make_table(tables_raw[0])

        return None

    def does_table_exist(self, db_name: str, table_name: str) -> bool:
        """
        Return True if the specified table exists.
        """
        query_sql = CHECK_TABLE_SQL.format(db_name=escape(db_name), table_name=escape(table_name))
        return bool(int(self._ch_client.query(query_sql)))

    def restore_database(self, database_schema: str) -> None:
        """
        Restore database.
        """
        self._ch_client.query(database_schema)

    def restore_udf(self, udf_statement):
        """
        Restore user defined function
        """
        self._ch_client.query(udf_statement)

    def restore_table(self, db_name: str, table_name: str, table_engine: str, table_schema: str) -> None:
        """
        Restore table.
        """
        if is_view(table_engine) or is_distributed(table_engine) or is_external_engine(table_engine):
            # Restore views and distributed tables using ATTACH.
            self._ch_client.query(to_attach_query(table_schema))
        elif is_replicated(table_engine) and self.ch_version_ge('21.8'):
            # Restore replicated tables using ATTACH + SYSTEM RESTORE REPLICA.
            # Conditional logic for differential versions is required because:
            # - Starting with 22.4 it's forbidden to specify UUID in non-distributed CREATE queries. As a result, we
            #   cannot use CREATE in new versions.
            # - SYSTEM RESTORE REPLICA query is supported only for versions 21.8 and newer. As a result, we
            #   cannot use SYSTEM RESTORE REPLICA in old versions.
            try:
                self._ch_client.query(to_attach_query(table_schema))
                self._ch_client.query(
                    RESTORE_REPLICA_SQL.format(db_name=escape(db_name), table_name=escape(table_name)))
            except Exception:
                self.drop_table_if_exists(db_name, escape(table_name))
                raise
        else:
            # Use CREATE for restoring all other types of tables.
            self._ch_client.query(table_schema)

    def drop_table_if_exists(self, db_name: str, table_name: str) -> None:
        """
        Drop table. If the specified table doesn't exist, do nothing.
        """
        self._ch_client.query(DROP_TABLE_IF_EXISTS_SQL.format(db_name=escape(db_name), table_name=escape(table_name)))

    def drop_database_if_exists(self, db_name: str) -> None:
        """
        Drop database. If the specified database doesn't exist, do nothing.
        """
        self._ch_client.query(DROP_DATABASE_IF_EXISTS_SQL.format(db_name=escape(db_name)))

    def drop_udf(self, udf_name: str) -> None:
        """
        Drop user defined function.
        """
        self._ch_client.query(DROP_UDF_SQL.format(udf_name=escape(udf_name)))

    def get_database_metadata_path(self, database: str) -> str:
        """
        Get filesystem absolute path to database metadata.
        """
        data = self._ch_client.query(GET_DATABASE_METADATA_PATH.format(db_name=database))['data']
        assert len(data) == 1
        return data[0]['metadata_path']

    def get_detached_part_path(self, table: Table, disk_name: str, part_name: str) -> str:
        """
        Get filesystem absolute path to detached data part.
        """
        return os.path.join(self._get_table_detached_path(table, disk_name), part_name)

    def get_version(self) -> str:
        """
        Get ClickHouse version.
        """
        return self._ch_version

    def get_access_control_objects(self) -> Sequence[Dict[str, Any]]:
        """
        Returns all access control objects.
        """
        result: List[Dict[str, Any]] = []

        for obj_type in ['users', 'roles', 'quotas', 'row_policies', 'settings_profiles']:
            ch_resp = self._ch_client.query(GET_ACCESS_CONTROL_OBJECTS_SQL.format(type=obj_type))
            obj_result = ch_resp.get('data', [])
            for row in obj_result:
                row.update({'type': obj_type})
            result.extend(obj_result)

        return result

    @staticmethod
    def list_freezed_parts(table: Table, disk: Disk, data_path: str, backup_name: str) -> Sequence[FreezedPart]:
        """
        List freezed parts from specific disk and path.
        """
        table_relative_path = os.path.relpath(data_path, disk.path)
        path = os.path.join(disk.path, 'shadow', backup_name, table_relative_path)

        if not os.path.exists(path):
            logging.debug('Shadow path %s is empty', path)
            return []

        freezed_parts: List[FreezedPart] = []
        for part in os.listdir(path):
            part_path = os.path.join(path, part)
            checksum = _get_part_checksum(part_path)
            files = list_dir_files(part_path)
            size = _get_part_size(part_path, files)
            logging.debug(f"list_freezed_parts: {table.name} -> {escape(table.name)} \n {part}")
            freezed_parts.append(
                FreezedPart(table.database, table.name, part, disk.name, part_path, checksum, size, files))

        return freezed_parts

    @staticmethod
    def _get_table_detached_path(table: Table, disk_name: str) -> str:
        for data_path, disk in table.paths_with_disks:
            if disk.name == disk_name:
                return os.path.join(data_path, 'detached')
        raise Exception("Disk not found: " + disk_name)

    def chown_dir(self, dir_path: str) -> None:
        """
        Change owner and group for all files in folder.
        """
        need_recursion = not self._main_config['drop_privileges']
        chown_dir_contents(self._ch_ctl_config['user'], self._ch_ctl_config['group'], dir_path, need_recursion)

    @retry(OSError)
    def _remove_shadow_data(self, path: str) -> None:
        assert path.find('/shadow') != -1, path

        if os.path.exists(path):
            shutil.rmtree(path)

    def ch_version_ge(self, comparing_version: str) -> bool:
        """
        Returns True if ClickHouse version >= comparing_version.
        """
        return parse_version(self.get_version()) >= parse_version(comparing_version)  # type: ignore

    def get_macros(self) -> Dict:
        """
        Get ClickHouse macros.
        """
        ch_resp = self._ch_client.query(GET_MACROS_SQL)
        return {row['macro']: row['substitution'] for row in ch_resp.get('data', [])}

    def get_udf_query(self) -> Dict[str, str]:
        """
        Get udf query from system table
        """
        resp = self._ch_client.query(GET_UDF_QUERY_SQL)
        return {row['name']: row['create_query'] for row in resp.get('data', [])}

    def get_disk(self, disk_name: str) -> Disk:
        """
        Get disk by name.
        """
        resp = self._ch_client.query(GET_DISK_SQL.format(disk_name=disk_name)).get('data')
        assert resp, f"disk '{disk_name}' not found"
        resp = resp[0]
        return Disk(resp['name'], resp['path'], resp['type'], resp['cache_path'])

    def get_disks(self) -> Dict[str, Disk]:
        """
        Get all configured disks.
        """
        if self.ch_version_ge('22.8'):
            disks_resp = self._ch_client.query(GET_DISKS_SQL_22_8)
            return {
                row['name']: Disk(row['name'], row['path'], row['type'], row['cache_path'])
                for row in disks_resp.get('data', [])
            }
        disks_resp = self._ch_client.query(GET_DISKS_SQL)
        return {row['name']: Disk(row['name'], row['path'], row['type']) for row in disks_resp.get('data', [])}

    def _make_table(self, record: dict) -> Table:
        return Table(database=record['database'],
                     name=record['name'],
                     engine=record['engine'],
                     disks=list(self._disks.values()),
                     data_paths=record['data_paths'] if record['engine'].find('MergeTree') != -1 else [],
                     create_statement=record['create_table_query'],
                     uuid=record.get('uuid', None))

    def read_s3_disk_revision(self, disk_name: str, backup_name: str) -> Optional[int]:
        """
        Reads S3 disk revision counter.
        """
        file_path = os.path.join(self._disks[disk_name].path, 'shadow', backup_name, 'revision.txt')
        if not os.path.exists(file_path):
            return None

        with open(file_path, 'r', encoding='utf-8') as file:
            return int(file.read().strip())

    def create_s3_disk_restore_file(self, disk_name: str, revision: int, source_bucket: str, source_path: str) -> None:
        """
        Creates file with restore information for S3 disk.
        """
        file_path = os.path.join(self._disks[disk_name].path, 'restore')
        with open(file_path, 'w', encoding='utf-8') as file:
            file.write(f'revision={revision}{os.linesep}')
            file.write(f'source_bucket={source_bucket}{os.linesep}')
            file.write(f'source_path={source_path}{os.linesep}')
            file.write(f'detached=true{os.linesep}')

    def restart_disk(self, disk_name: str, context: RestoreContext) -> None:
        """
        Restarts ClickHouse and waits till it can process queries.
        """
        self._ch_client.query(RESTART_DISK_SQL.format(disk_name=disk_name), timeout=self._restart_disk_timeout)
        context.add_restarted_disk(disk_name)

    def reload_config(self):
        """
        Reload ClickHouse configuration query.
        """
        self._ch_client.query(RELOAD_CONFIG_SQL, timeout=self._timeout)


def _get_part_checksum(part_path: str) -> str:
    with open(os.path.join(part_path, 'checksums.txt'), 'rb') as f:
        return md5(f.read()).hexdigest()  # nosec


def _get_part_size(part_path: str, files: List[str]) -> int:
    size = 0
    for file in files:
        filesize = os.path.getsize(os.path.join(part_path, file))
        remainder = filesize % BLOCKSIZE
        if remainder > 0:
            filesize += BLOCKSIZE - remainder
        size += filesize
    return size
