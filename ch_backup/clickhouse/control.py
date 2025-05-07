"""
Clickhouse-control classes module
"""

# pylint: disable=too-many-lines

import os
import shutil
from contextlib import contextmanager, suppress
from hashlib import md5
from pathlib import Path
from tarfile import BLOCKSIZE  # type: ignore
from typing import Any, Dict, Iterable, List, Optional, Sequence, Union

from packaging.version import parse as parse_version

from ch_backup import logging
from ch_backup.backup.metadata import TableMetadata
from ch_backup.backup.restore_context import RestoreContext
from ch_backup.calculators import calc_aligned_files_size
from ch_backup.clickhouse.client import ClickhouseClient
from ch_backup.clickhouse.models import Database, Disk, FrozenPart, Table
from ch_backup.exceptions import ClickhouseBackupError
from ch_backup.storage.async_pipeline.base_pipeline.exec_pool import ThreadExecPool
from ch_backup.util import (
    chown_dir_contents,
    chown_file,
    escape,
    list_dir_files,
    retry,
    strip_query,
)

ACCESS_ENTITY_CHAR = {
    "users": "U",
    "roles": "R",
    "quotas": "Q",
    "row_policies": "P",
    "settings_profiles": "S",
}

GET_TABLES_SQL = strip_query(
    """
    SELECT
        database,
        name,
        engine,
        engine_full,
        create_table_query,
        data_paths,
        metadata_path,
        uuid
    FROM system.tables
    WHERE ({db_condition})
      AND ({tables_condition})
      AND engine != 'StorageProxy'

    UNION ALL

    SELECT
        database,
        name,
        engine,
        engine_full,
        create_table_query,
        [],
        metadata_path,
        uuid
    FROM system.tables
    WHERE ({db_condition})
      AND ({tables_condition})
      AND engine = 'StorageProxy'

    ORDER BY metadata_modification_time
    FORMAT JSON
"""
)

# 'WHERE engine != StorageProxy' in first SELECT still
# throws exception if StorageProxy table is not responding,
# only filter by database and name works
GET_TABLES_SQL_23_8 = strip_query(
    """
    SELECT
        database,
        name,
        engine,
        engine_full,
        create_table_query,
        data_paths,
        metadata_path,
        uuid
    FROM system.tables
    WHERE ({db_condition})
      AND ({tables_condition})
      AND name
        NOT IN (SELECT name FROM system.tables
            WHERE ({db_condition}) AND engine = 'StorageProxy')

    UNION ALL

    SELECT
        database,
        name,
        engine,
        engine_full,
        create_table_query,
        [],
        metadata_path,
        uuid
    FROM system.tables
    WHERE ({db_condition})
      AND ({tables_condition})
      AND engine = 'StorageProxy'

    ORDER BY metadata_modification_time
    FORMAT JSON
"""
)

GET_TABLES_SHORT_SQL = strip_query(
    """
    SELECT
        database,
        name,
        engine,
        create_table_query
    FROM system.tables
    WHERE ({db_condition})
      AND ({tables_condition})
    ORDER BY metadata_modification_time
    FORMAT JSON
"""
)

CHECK_TABLE_SQL = strip_query(
    """
    SELECT count()
    FROM system.tables
    WHERE database = '{db_name}' AND name = '{table_name}'
    FORMAT TSVRaw
"""
)

DATABASE_ATTACH_SQL = strip_query(
    """
    ATTACH DATABASE `{db_name}`
"""
)

PART_ATTACH_SQL = strip_query(
    """
    ALTER TABLE `{db_name}`.`{table_name}`
    ATTACH PART '{part_name}'
"""
)

TABLE_ATTACH_SQL = strip_query(
    """
    ATTACH TABLE `{db_name}`.`{table_name}`
"""
)

FREEZE_TABLE_SQL = strip_query(
    """
    ALTER TABLE `{db_name}`.`{table_name}`
    FREEZE WITH NAME '{backup_name}'
"""
)

FREEZE_PARTITION_SQL = strip_query(
    """
    ALTER TABLE `{db_name}`.`{table_name}`
    FREEZE PARTITION ID '{partition}' WITH NAME '{backup_name}'
"""
)

SYSTEM_UNFREEZE_SQL = strip_query(
    """
    SYSTEM UNFREEZE WITH NAME '{backup_name}'
"""
)

DROP_TABLE_IF_EXISTS_SQL = strip_query(
    """
    DROP TABLE IF EXISTS `{db_name}`.`{table_name}` NO DELAY
"""
)

DROP_DICTIONARY_IF_EXISTS_SQL = strip_query(
    """
    DROP DICTIONARY IF EXISTS `{db_name}`.`{table_name}` NO DELAY
"""
)

DROP_DATABASE_IF_EXISTS_SQL = strip_query(
    """
    DROP DATABASE IF EXISTS `{db_name}` NO DELAY
"""
)

DROP_UDF_SQL = strip_query(
    """
    DROP FUNCTION `{udf_name}`
"""
)

DROP_NAMED_COLLECTION_SQL = strip_query(
    """
    DROP NAMED COLLECTION `{nc_name}`
"""
)

TRUNCATE_TABLE_IF_EXISTS_SQL = strip_query(
    """
    TRUNCATE TABLE IF EXISTS `{db_name}`.`{table_name}`
"""
)

RESTORE_REPLICA_SQL = strip_query(
    """
    SYSTEM RESTORE REPLICA `{db_name}`.`{table_name}`
"""
)

DROP_REPLICA_BY_ZK_PATH_SQL = strip_query(
    """
    SYSTEM DROP REPLICA '{replica_name}' FROM ZKPATH '{zk_path}'
"""
)

DROP_DATABASE_REPLICA_BY_ZK_PATH_SQL = strip_query(
    """
    SYSTEM DROP DATABASE REPLICA '{replica_name}' FROM ZKPATH '{zk_path}'
"""
)

GET_DATABASES_SQL = strip_query(
    """
    SELECT
        name,
        engine,
        metadata_path,
        uuid,
        engine_full
    FROM system.databases
    WHERE name NOT IN ('system', '_temporary_and_external_tables', 'information_schema', 'INFORMATION_SCHEMA', '{system_db}')
    FORMAT JSON
"""
)

GET_DATABASES_SQL_22_8 = strip_query(
    """
    SELECT
        name,
        engine,
        metadata_path,
        uuid
    FROM system.databases
    WHERE name NOT IN ('system', '_temporary_and_external_tables', 'information_schema', 'INFORMATION_SCHEMA', '{system_db}')
    FORMAT JSON
"""
)

CREATE_IF_NOT_EXISTS_SYSTEM_DB_SQL = strip_query(
    "CREATE DATABASE IF NOT EXISTS `{system_db}`"
)
CREATE_IF_NOT_EXISTS_DEDUP_TABLE_SQL = strip_query(
    """
    CREATE TABLE IF NOT EXISTS `{system_db}`._deduplication_info (
        database String,
        table String,
        name String,
        backup_path String,
        checksum String,
        size Int64,
        files Array(String),
        tarball Bool,
        disk_name String,
        verified Bool,
        encrypted Bool
    )
    ENGINE = MergeTree()
    ORDER BY (database, table, name, checksum)
"""
)
CREATE_IF_NOT_EXISTS_DEDUP_TABLE_CURRENT_SQL = strip_query(
    """
    CREATE TABLE IF NOT EXISTS `{system_db}`._deduplication_info_current (
        name String,
        checksum String,
    )
    ENGINE = MergeTree()
    ORDER BY (name, checksum)
"""
)

INSERT_DEDUP_INFO_BATCH_SQL = strip_query(
    "INSERT INTO `{system_db}`.`{table}` VALUES {batch}"
)

GET_DEDUPLICATED_PARTS_SQL = strip_query(
    """
    SELECT `{system_db}`._deduplication_info.* FROM `{system_db}`._deduplication_info
    JOIN `{system_db}`._deduplication_info_current
    ON _deduplication_info.name = _deduplication_info_current.name
        AND _deduplication_info.checksum = _deduplication_info_current.checksum
    WHERE database='{database}' AND table='{table}'
    FORMAT JSON
"""
)

SHOW_CREATE_DATABASE_SQL = strip_query(
    """
    SHOW CREATE DATABASE `{db_name}`
    FORMAT TSVRaw
"""
)

GET_DATABASE_ENGINE = strip_query(
    """
    SELECT engine FROM system.databases WHERE name='{db_name}'
    FORMAT TSVRaw
"""
)

GET_DATABASE_METADATA_PATH = strip_query(
    """
    SELECT metadata_path FROM system.databases WHERE name='{db_name}'
    FORMAT JSON
"""
)

GET_VERSION_SQL = strip_query(
    """
    SELECT version()
    FORMAT TSVRaw
"""
)

GET_MACROS_SQL = strip_query(
    """
    SELECT macro, substitution FROM system.macros
    FORMAT JSON
"""
)

GET_ACCESS_CONTROL_OBJECTS_SQL = strip_query(
    """
    SELECT id, name
    FROM system.{type}
    WHERE storage IN ('disk', 'local directory', 'local_directory', 'replicated')
    FORMAT JSON
"""
)

GET_DISK_SQL = strip_query(
    """
    SELECT name, path, type, cache_path FROM system.disks
    WHERE name = '{disk_name}'
    FORMAT JSON
"""
)

GET_DISK_SQL_24_3 = strip_query(
    """
    SELECT name, path, type, object_storage_type, metadata_type, cache_path FROM system.disks
    WHERE name = '{disk_name}'
    FORMAT JSON
"""
)

GET_DISKS_SQL = strip_query(
    """
    SELECT name, path, type FROM system.disks
    ORDER BY length(path) DESC
    FORMAT JSON
"""
)

GET_DISKS_SQL_22_8 = strip_query(
    """
    SELECT name, path, type, cache_path FROM system.disks
    ORDER BY length(path) DESC
    FORMAT JSON
"""
)

GET_DISKS_SQL_24_3 = strip_query(
    """
    SELECT name, path, type, object_storage_type, metadata_type, cache_path FROM system.disks
    ORDER BY length(path) DESC
    FORMAT JSON
"""
)

GET_UDF_QUERY_SQL = strip_query(
    """
    SELECT name, create_query FROM system.functions WHERE origin == 'SQLUserDefined'
    FORMAT JSON
"""
)

GET_NAMED_COLLECTIONS_QUERY_SQL = strip_query(
    """
    SELECT name FROM system.named_collections
    FORMAT JSON
"""
)

DECRYPT_AES_CTR_QUERY_SQL = strip_query(
    """
    SELECT decrypt('aes-{key_size}-ctr', unhex('{data_hex}'), unhex('{key_hex}'), unhex('{iv_hex}')) AS data
    FORMAT JSON
"""
)

RELOAD_CONFIG_SQL = strip_query(
    """
    SYSTEM RELOAD CONFIG
"""
)

GET_ZOOKEEPER_ADMIN_USER = strip_query(
    """
    SELECT
        name,
        value
    FROM system.zookeeper
    WHERE (path = '/clickhouse/access/U') AND (name = 'admin')
    FORMAT JSON
"""
)

GET_ZOOKEEPER_ADMIN_UUID = strip_query(
    """
    SELECT
        name,
        value
    FROM system.zookeeper
    WHERE (path = '/clickhouse/access/uuid') AND (value LIKE 'ATTACH USER admin%')
    FORMAT JSON
"""
)

GET_PARTITIONS = strip_query(
    """
    SELECT DISTINCT partition_id
    FROM system.parts
    WHERE database = '{db_name}' AND table = '{table_name}' AND active = 1
    FORMAT JSONCompact
"""
)


# pylint: disable=too-many-public-methods
class ClickhouseCTL:
    """
    ClickHouse control tool.
    """

    # pylint: disable=too-many-instance-attributes

    def __init__(
        self, ch_ctl_config: dict, main_config: dict, backup_config: dict
    ) -> None:
        self._ch_ctl_config = ch_ctl_config
        self._main_config = main_config
        self._backup_config = backup_config
        self._root_data_path = self._ch_ctl_config["data_path"]
        self._shadow_data_path = os.path.join(self._root_data_path, "shadow")
        self._timeout = self._ch_ctl_config["timeout"]
        self._freeze_timeout = self._ch_ctl_config["freeze_timeout"]
        self._unfreeze_timeout = self._ch_ctl_config["unfreeze_timeout"]
        self._restore_replica_timeout = self._ch_ctl_config["restore_replica_timeout"]
        self._drop_replica_timeout = self._ch_ctl_config["drop_replica_timeout"]
        self._ch_client = ClickhouseClient(self._ch_ctl_config)
        self._ch_version = self._ch_client.query(GET_VERSION_SQL)
        self._disks = self.get_disks()
        settings = {
            "allow_deprecated_database_ordinary": 1,
            "allow_deprecated_syntax_for_merge_tree": 1,
            "allow_experimental_database_materialized_postgresql": 1,
            "allow_experimental_database_materialized_mysql": 1,
            "allow_experimental_database_replicated": 1,
            "allow_experimental_funnel_functions": 1,
            "allow_experimental_hash_functions": 1,
            "allow_experimental_live_view": 1,
            "allow_experimental_window_view": 1,
            "allow_experimental_object_type": 1,
            "allow_suspicious_codecs": 1,
            "allow_suspicious_low_cardinality_types": 1,
            "check_table_dependencies": 0,
            "kafka_disable_num_consumers_limit": 1,
        }
        if self.ch_version_ge("22.9"):
            settings.update(
                {
                    "allow_experimental_annoy_index": 1,
                    "allow_suspicious_fixed_string_types": 1,
                }
            )
        if self.ch_version_ge("23.1"):
            settings.update(
                {
                    "allow_experimental_inverted_index": 1,
                }
            )
        if self.ch_version_ge("23.12"):
            settings.update(
                {
                    "allow_experimental_refreshable_materialized_view": 1,
                    "allow_suspicious_ttl_expressions": 1,
                }
            )
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
                logging.warning(
                    f"table {table.database}.{table.name} path {detached_path} not found"
                )
                context.add_failed_chown(table.database, table.database, detached_path)

    def attach_part(self, table: Table, part_name: str) -> None:
        """
        Attach data part to the specified table.
        """
        query_sql = PART_ATTACH_SQL.format(
            db_name=escape(table.database),
            table_name=escape(table.name),
            part_name=part_name,
        )

        self._ch_client.query(query_sql)

    def attach_table(self, table: Union[TableMetadata, Table]) -> None:
        """
        Attach data part to the specified table.
        """
        query_sql = TABLE_ATTACH_SQL.format(
            db_name=escape(table.database), table_name=escape(table.name)
        )

        self._ch_client.query(query_sql)

    def freeze_table(
        self,
        backup_name: str,
        table: Table,
        threads: int,
    ) -> None:
        """
        Make snapshot of the specified table.
        """
        # Table has no partitions or created with deprecated syntax.
        # FREEZE PARTITION ID with deprecated syntax throws segmentation fault in CH.
        freeze_by_partitions = threads > 0 and "PARTITION BY" in table.create_statement
        if freeze_by_partitions:
            with ThreadExecPool(max(1, threads)) as pool:
                if freeze_by_partitions:
                    partitions_to_freeze = self.list_partitions(table)
                    for partition in partitions_to_freeze:
                        query_sql = FREEZE_PARTITION_SQL.format(
                            db_name=escape(table.database),
                            table_name=escape(table.name),
                            backup_name=backup_name,
                            partition=partition,
                        )
                        pool.submit(
                            f'Freeze partition "{partition}"',
                            self._ch_client.query,
                            query_sql,
                            timeout=self._freeze_timeout,
                            should_retry=False,
                            new_session=True,
                        )
                pool.wait_all(
                    keep_going=False,
                    timeout=self._freeze_timeout,
                )
        else:
            query_sql = FREEZE_TABLE_SQL.format(
                db_name=escape(table.database),
                table_name=escape(table.name),
                backup_name=backup_name,
            )
            self._ch_client.query(
                query_sql,
                timeout=self._freeze_timeout,
                should_retry=False,
                new_session=True,
            )

    def list_partitions(self, table: Table) -> List[str]:
        """
        Get list of active partitions for table.
        """
        query_sql = GET_PARTITIONS.format(
            db_name=escape(table.database),
            table_name=escape(table.name),
        )
        result = self._ch_client.query(query_sql, should_retry=False)["data"]
        return [partition[0] for partition in result]

    def system_unfreeze(self, backup_name: str) -> None:
        """
        Unfreeze all partitions from all disks.
        """
        if self.ch_version_ge("22.6"):
            query_sql = SYSTEM_UNFREEZE_SQL.format(backup_name=backup_name)
            self._ch_client.query(query_sql, timeout=self._unfreeze_timeout)

    def remove_freezed_data(
        self, backup_name: Optional[str] = None, table: Optional[Table] = None
    ) -> None:
        """
        Remove all freezed partitions from all local disks.
        """
        if not (backup_name is None) == (table is None):
            raise RuntimeError(
                "Both backup_name and table should be None or not None at the same time"
            )

        if backup_name and table:
            for table_data_path, disk in table.paths_with_disks:
                if disk.type == "local":
                    table_relative_path = os.path.relpath(table_data_path, disk.path)
                    shadow_path = os.path.join(
                        disk.path, "shadow", backup_name, table_relative_path
                    )
                    logging.debug("Removing shadow data: {}", shadow_path)
                    self._remove_shadow_data(shadow_path)
        else:
            for disk in self._disks.values():
                if disk.type == "local":
                    shadow_path = os.path.join(disk.path, "shadow")
                    logging.debug("Removing shadow data: {}", shadow_path)
                    self._remove_shadow_data(shadow_path)

    def remove_freezed_part(self, part: FrozenPart) -> None:
        """
        Remove the freezed part.
        """
        logging.debug("Removing freezed part: {}", part.path)
        self._remove_shadow_data(part.path)

    def get_databases(
        self, exclude_dbs: Optional[Sequence[str]] = None
    ) -> Sequence[Database]:
        """
        Get list of all databases.
        """
        if not exclude_dbs:
            exclude_dbs = []

        result: List[Database] = []
        system_database = self._backup_config["system_database"]

        query = (
            GET_DATABASES_SQL.format(system_db=system_database)
            if self.ch_version_ge("23.3")
            else GET_DATABASES_SQL_22_8.format(system_db=system_database)
        )

        ch_resp = self._ch_client.query(query)
        if "data" in ch_resp:
            result = [
                Database(
                    row["name"],
                    row["engine"],
                    row["metadata_path"],
                    row["uuid"],
                    row.get("engine_full"),
                )
                for row in ch_resp["data"]
                if row["name"] not in exclude_dbs
            ]

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

    def get_tables(
        self,
        db_name: str = None,
        tables: Optional[Sequence[str]] = None,
        short_query: bool = False,
    ) -> Sequence[Table]:
        """
        Get database tables.

        A short query does not access the source of table if it was built from an external source.
        Example: CREATE ... AS postgresql() or CREATE ... AS s3().
        """
        db_condition = f"database = '{escape(db_name)}'" if db_name else "1"
        tables_condition = (
            f"has(cast({_format_string_array(tables)}, 'Array(String)'), name)"
            if tables
            else "1"
        )

        base_query_sql = ""
        if short_query:
            base_query_sql = GET_TABLES_SHORT_SQL
        elif self.ch_version_ge("24.3"):
            base_query_sql = GET_TABLES_SQL
        else:
            base_query_sql = GET_TABLES_SQL_23_8

        query_sql = base_query_sql.format(
            db_condition=db_condition,
            tables_condition=tables_condition,
        )  # type: ignore
        result: List[Table] = []
        for row in self._ch_client.query(query_sql)["data"]:
            result.append(self._make_table(row))

        return result

    def get_table(
        self, db_name: str, table_name: str, short_query: bool = False
    ) -> Optional[Table]:
        """
        Get table by name, returns None if no table has found.
        """
        tables = self.get_tables(db_name, [table_name], short_query)

        if len(tables) > 1:
            raise RuntimeError(
                f"Found several tables, when expected to find single table: database {db_name}, table {table_name}"
            )

        return tables[0] if len(tables) == 1 else None

    def does_table_exist(self, db_name: str, table_name: str) -> bool:
        """
        Return True if the specified table exists.
        """
        query_sql = CHECK_TABLE_SQL.format(
            db_name=escape(db_name), table_name=escape(table_name)
        )
        return bool(int(self._ch_client.query(query_sql)))

    def attach_database(self, db: Database) -> None:
        """
        Restore database.
        """
        self._ch_client.query(DATABASE_ATTACH_SQL.format(db_name=escape(db.name)))

    def restore_database(self, database_schema: str) -> None:
        """
        Restore database.
        """
        self._ch_client.query(database_schema)

    def restore_udf(self, udf_statement):
        """
        Restore user defined function.
        """
        self._ch_client.query(udf_statement)

    def restore_named_collection(self, nc_statement):
        """
        Restore named collection.
        """
        self._ch_client.query(nc_statement)

    def create_table(self, table: Table) -> None:
        """
        Restore table.
        """
        self._ch_client.query(table.create_statement)

    def restore_replica(self, table: Table) -> None:
        """
        Call SYSTEM RESTORE REPLICA for table.
        """
        assert table.is_replicated()
        self._ch_client.query(
            RESTORE_REPLICA_SQL.format(
                db_name=escape(table.database), table_name=escape(table.name)
            ),
            timeout=self._restore_replica_timeout,
        )

    def drop_table_if_exists(self, table: Table) -> None:
        """
        Drop table. If the specified table doesn't exist, do nothing.
        """
        with self._force_drop_table():
            self._ch_client.query(
                DROP_TABLE_IF_EXISTS_SQL.format(
                    db_name=escape(table.database), table_name=escape(table.name)
                )
            )

    def drop_dictionary_if_exists(self, table: Table) -> None:
        """
        Drop dictionary. If the specified dictionary doesn't exist, do nothing.
        """
        self._ch_client.query(
            DROP_DICTIONARY_IF_EXISTS_SQL.format(
                db_name=escape(table.database), table_name=escape(table.name)
            )
        )

    def drop_database_if_exists(self, db_name: str) -> None:
        """
        Drop database. If the specified database doesn't exist, do nothing.
        """
        with self._force_drop_table():
            self._ch_client.query(
                DROP_DATABASE_IF_EXISTS_SQL.format(db_name=escape(db_name))
            )

    def drop_udf(self, udf_name: str) -> None:
        """
        Drop user defined function.
        """
        self._ch_client.query(DROP_UDF_SQL.format(udf_name=escape(udf_name)))

    def drop_named_collection(self, nc_name: str) -> None:
        """
        Drop named collection.
        """
        self._ch_client.query(DROP_NAMED_COLLECTION_SQL.format(nc_name=escape(nc_name)))

    def system_drop_replica(self, replica: str, zookeeper_path: str) -> None:
        """
        System drop replica query.
        """
        self._ch_client.query(
            DROP_REPLICA_BY_ZK_PATH_SQL.format(
                replica_name=replica, zk_path=zookeeper_path
            ),
            timeout=self._drop_replica_timeout,
        )

    def system_drop_database_replica(self, replica: str, zookeeper_path: str) -> None:
        """
        System drop database replica query.
        """
        self._ch_client.query(
            DROP_DATABASE_REPLICA_BY_ZK_PATH_SQL.format(
                replica_name=replica, zk_path=zookeeper_path
            ),
            timeout=self._drop_replica_timeout,
        )

    def get_database_metadata_path(self, database: str) -> str:
        """
        Get filesystem absolute path to database metadata.
        """
        data = self._ch_client.query(
            GET_DATABASE_METADATA_PATH.format(db_name=database)
        )["data"]
        assert len(data) == 1
        return data[0]["metadata_path"]

    def get_detached_part_path(
        self, table: Table, disk_name: str, part_name: str
    ) -> str:
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

        for obj_type, obj_char in ACCESS_ENTITY_CHAR.items():
            ch_resp = self._ch_client.query(
                GET_ACCESS_CONTROL_OBJECTS_SQL.format(type=obj_type)
            )
            obj_result = ch_resp.get("data", [])
            for row in obj_result:
                row.update({"char": obj_char})
            result.extend(obj_result)

        return result

    def get_zookeeper_admin_id(self) -> str:
        """
        Returns linked admin's UUID from zookeeper.
        """
        result = self._ch_client.query(GET_ZOOKEEPER_ADMIN_USER).get("data", [])
        assert len(result) == 1
        return result[0]["value"]

    def get_zookeeper_admin_uuid(self) -> Dict[str, str]:
        """
        Returns all UUIDs associated with admin user.
        """
        result = self._ch_client.query(GET_ZOOKEEPER_ADMIN_UUID).get("data", [])
        return {item["name"]: item["value"] for item in result}

    @staticmethod
    def scan_frozen_parts(
        table: Table,
        disk: Disk,
        data_path: str,
        backup_name: str,
    ) -> Iterable[FrozenPart]:
        """
        Yield frozen parts from specific disk and path.
        """
        table_relative_path = os.path.relpath(data_path, disk.path)
        path = os.path.join(disk.path, "shadow", backup_name, table_relative_path)

        if not os.path.exists(path):
            logging.debug("Shadow path {} is empty", path)
            return []

        for dir_entry in os.scandir(path):
            part = dir_entry.name
            part_path = dir_entry.path
            checksum = _get_part_checksum(part_path)
            rel_paths = list_dir_files(part_path)
            abs_paths = [Path(part_path) / file for file in rel_paths]

            size = calc_aligned_files_size(abs_paths, alignment=BLOCKSIZE)
            logging.debug(
                f"scan_freezed_parts: {table.name} -> {escape(table.name)} \n {part}"
            )

            yield FrozenPart(
                table.database,
                table.name,
                part,
                disk.name,
                part_path,
                checksum,
                size,
                rel_paths,
            )

    @staticmethod
    def _get_table_detached_path(table: Table, disk_name: str) -> str:
        for data_path, disk in table.paths_with_disks:
            if disk.name == disk_name:
                return os.path.join(data_path, "detached")
        raise RuntimeError(f"Disk not found: {disk_name}")

    def chown_dir(self, dir_path: str) -> None:
        """
        Change owner and group for all files in folder.
        """
        need_recursion = not self._main_config["drop_privileges"]
        chown_dir_contents(
            self._ch_ctl_config["user"],
            self._ch_ctl_config["group"],
            dir_path,
            need_recursion,
        )

    def create_shadow_increment(self) -> None:
        """
        Create shadow/increment.txt to fix race condition with parallel freeze.
        Must be used before freezing more than one table at once.
        https://github.com/ClickHouse/ClickHouse/blob/597a72fd9afd88984abc10b284624c6b4d08368b/src/Common/Increment.h#L20
        """
        default_shadow_path = Path(self._root_data_path) / "shadow"
        increment_path = default_shadow_path / "increment.txt"
        if os.path.exists(increment_path):
            return
        if not os.path.exists(default_shadow_path):
            os.mkdir(default_shadow_path)
            self.chown_dir(str(default_shadow_path))
        with open(increment_path, "w", encoding="utf-8") as file:
            file.write("0")
            chown_file(
                self._ch_ctl_config["user"],
                self._ch_ctl_config["group"],
                str(increment_path),
            )

    @retry(OSError)
    def _remove_shadow_data(self, path: str) -> None:
        if path.find("/shadow") == -1:
            raise ClickhouseBackupError(
                f"Path '{path}' is incompatible for shadow data"
            )

        if os.path.exists(path):
            logging.debug(f"Path {path} exists. Trying to remove it.")

            shutil.rmtree(path)

            msg = "shutil.rmtree has done working"
            if os.path.exists(path):
                msg += ", but path still exists"
            logging.debug(msg)
        else:
            logging.debug(f"Path {path} does not exist. There is nothing to remove.")

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
        return {row["macro"]: row["substitution"] for row in ch_resp.get("data", [])}

    def get_udf_query(self) -> Dict[str, str]:
        """
        Get udf query from system table.
        """
        resp = self._ch_client.query(GET_UDF_QUERY_SQL)
        return {row["name"]: row["create_query"] for row in resp.get("data", [])}

    def get_named_collections_query(self) -> List[str]:
        """
        Get named collections query from system table.
        """
        resp = self._ch_client.query(GET_NAMED_COLLECTIONS_QUERY_SQL)
        return [row["name"] for row in resp.get("data", [])]

    def decrypt_aes_ctr(
        self, data_hex: str, key_hex: str, key_size: int, iv_hex: str
    ) -> str:
        """
        Decode data with aes ctr algorithm.

        Provided :key_size: should be one of:
            - 128
            - 192
            - 256
        """
        resp = self._ch_client.query(
            DECRYPT_AES_CTR_QUERY_SQL.format(
                key_size=key_size,
                data_hex=data_hex,
                key_hex=key_hex,
                iv_hex=iv_hex,
            )
        )

        first_row = resp.get("data")[0]

        assert first_row, "could not decrypt data"

        return first_row.get("data")

    def get_disk(self, disk_name: str) -> Disk:
        """
        Get disk by name.
        """
        if self.ch_version_ge("24.3"):
            resp = resp = self._ch_client.query(
                GET_DISK_SQL_24_3.format(disk_name=disk_name)
            ).get("data")
        else:
            resp = self._ch_client.query(GET_DISK_SQL.format(disk_name=disk_name)).get(
                "data"
            )

        assert resp, f"disk '{disk_name}' not found"
        resp = resp[0]
        return Disk(
            resp.get("name"),
            resp.get("path"),
            resp.get("type"),
            resp.get("object_storage_type"),
            resp.get("metadata_type"),
            resp.get("cache_path"),
        )

    def get_disks(self) -> Dict[str, Disk]:
        """
        Get all configured disks.
        """
        if self.ch_version_ge("24.3"):
            disks_resp = self._ch_client.query(GET_DISKS_SQL_24_3)
            return {
                row["name"]: Disk(
                    row["name"],
                    row["path"],
                    row["type"],
                    row["object_storage_type"],
                    row["metadata_type"],
                    row["cache_path"],
                )
                for row in disks_resp.get("data", [])
            }
        if self.ch_version_ge("22.8"):
            disks_resp = self._ch_client.query(GET_DISKS_SQL_22_8)
            return {
                row["name"]: Disk(
                    row["name"], row["path"], row["type"], cache_path=row["cache_path"]
                )
                for row in disks_resp.get("data", [])
            }
        disks_resp = self._ch_client.query(GET_DISKS_SQL)
        return {
            row["name"]: Disk(row["name"], row["path"], row["type"])
            for row in disks_resp.get("data", [])
        }

    def _make_table(self, record: dict) -> Table:
        return Table(
            database=record["database"],
            name=record["name"],
            engine=record.get("engine", None),
            disks=list(self._disks.values()),
            data_paths=(
                record.get("data_paths", [])
                if "MergeTree" in record.get("engine", "")
                else []
            ),
            metadata_path=self._get_metadata_path(record),
            create_statement=record["create_table_query"],
            uuid=record.get("uuid", None),
        )

    def _get_metadata_path(self, record: dict) -> str:
        metadata_path = record.get("metadata_path", None)
        if metadata_path is None:
            return ""

        if not self.ch_version_ge("25.1"):
            return metadata_path

        default_disk = self._disks.get("default")

        if default_disk is not None:
            metadata_path = default_disk.path + metadata_path

        return metadata_path

    def read_s3_disk_revision(self, disk_name: str, backup_name: str) -> Optional[int]:
        """
        Reads S3 disk revision counter.
        """
        file_path = os.path.join(
            self._disks[disk_name].path, "shadow", backup_name, "revision.txt"
        )
        if not os.path.exists(file_path):
            return None

        with open(file_path, "r", encoding="utf-8") as file:
            return int(file.read().strip())

    def reload_config(self):
        """
        Reload ClickHouse configuration query.
        """
        self._ch_client.query(RELOAD_CONFIG_SQL, timeout=self._timeout)

    def create_deduplication_table(self):
        """
        Create ClickHouse table for deduplication info
        """
        self._ch_client.query(
            CREATE_IF_NOT_EXISTS_SYSTEM_DB_SQL.format(
                system_db=escape(self._backup_config["system_database"])
            )
        )
        self._ch_client.query(
            DROP_TABLE_IF_EXISTS_SQL.format(
                db_name=escape(self._backup_config["system_database"]),
                table_name="_deduplication_info",
            )
        )
        self._ch_client.query(
            CREATE_IF_NOT_EXISTS_DEDUP_TABLE_SQL.format(
                system_db=escape(self._backup_config["system_database"])
            )
        )

    def insert_deduplication_info(self, batch: List[str]) -> None:
        """
        Insert deduplication info in batch
        """
        self._ch_client.query(
            INSERT_DEDUP_INFO_BATCH_SQL.format(
                system_db=escape(self._backup_config["system_database"]),
                table="_deduplication_info",
                batch=",".join(batch),
            ),
        )

    def get_deduplication_info(
        self, database: str, table: str, frozen_parts: Dict[str, FrozenPart]
    ) -> List[Dict]:
        """
        Get deduplication info for given frozen parts of a table
        """
        self._ch_client.query(
            TRUNCATE_TABLE_IF_EXISTS_SQL.format(
                db_name=escape(self._backup_config["system_database"]),
                table_name="_deduplication_info_current",
            )
        )
        self._ch_client.query(
            CREATE_IF_NOT_EXISTS_DEDUP_TABLE_CURRENT_SQL.format(
                system_db=escape(self._backup_config["system_database"])
            )
        )

        batch = [f"('{part.name}','{part.checksum}')" for part in frozen_parts.values()]
        self._ch_client.query(
            INSERT_DEDUP_INFO_BATCH_SQL.format(
                system_db=escape(self._backup_config["system_database"]),
                table="_deduplication_info_current",
                batch=",".join(batch),
            ),
        )
        result_json = self._ch_client.query(
            GET_DEDUPLICATED_PARTS_SQL.format(
                system_db=escape(self._backup_config["system_database"]),
                database=escape(database),
                table=escape(table),
            )
        )

        return result_json["data"]

    @staticmethod
    @contextmanager
    def _force_drop_table():
        """
        Set and clear on exit force_drop_table flag.

        If it has been set before then don't touch it.
        This flag allows to overcome TABLE_SIZE_EXCEEDS_MAX_DROP_SIZE_LIMIT ClickHouse error.
        """
        flag_path = Path("/var/lib/clickhouse/flags/force_drop_table")

        flag_path.touch()
        try:
            flag_path.chmod(0o666)
            yield
        finally:
            with suppress(FileNotFoundError):
                flag_path.unlink()


def _get_part_checksum(part_path: str) -> str:
    with open(os.path.join(part_path, "checksums.txt"), "rb") as f:
        return md5(f.read()).hexdigest()  # nosec


def _format_string_array(value: Sequence[str]) -> str:
    return "[" + ",".join(f"'{escape(v)}'" for v in value) + "]"
