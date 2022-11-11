"""
Clickhouse backup logic for databases
"""
from typing import Any

from ch_backup import logging
from ch_backup.backup.layout import BackupLayout
from ch_backup.backup.metadata import BackupMetadata
from ch_backup.clickhouse.control import ClickhouseCTL
from ch_backup.clickhouse.schema import is_replicated_db_engine
from ch_backup.config import Config
from ch_backup.logic.backup_manager import BackupManager
from ch_backup.util import get_database_zookeeper_paths
from ch_backup.zookeeper.zookeeper import ZookeeperCTL


class DatabaseBackup(BackupManager):
    """
    Database backup class
    """
    def __init__(self, ch_ctl: ClickhouseCTL, backup_layout: BackupLayout, config: Config) -> None:
        super().__init__(ch_ctl, backup_layout)
        self._config = config['backup']
        self._zk_config = config.get('zookeeper')

    def backup(self, **kwargs: Any) -> None:
        for db_name in kwargs['databases']:
            self._backup_database(kwargs['backup_meta'], db_name)

    def restore(self, backup_meta: BackupMetadata, **kwargs: Any) -> None:
        present_databases = self._ch_ctl.get_databases()

        for db_name in kwargs['databases']:
            if not _has_embedded_metadata(db_name) and db_name not in present_databases:
                db_sql = self._backup_layout.get_database_create_statement(backup_meta, db_name)
                self._ch_ctl.restore_database(db_sql)

    def restore_schema(self, **kwargs: Any) -> None:
        """
        Restore schema
        """
        present_databases = self._ch_ctl.get_databases()
        databases_to_create = []
        databases = kwargs['databases']
        source_ch_ctl = kwargs['source_ch_ctl']
        for database in databases:
            logging.debug('Restoring database "%s"', database)
            if not _has_embedded_metadata(database) and database not in present_databases:
                db_sql = source_ch_ctl.get_database_schema(database)
                databases_to_create.append(db_sql)
                if is_replicated_db_engine(db_sql):  # do not check tables in replicated database
                    continue

        if databases_to_create:
            if len(self._zk_config.get('hosts')) > 0:
                logging.info("Cleaning up replicated database metadata")
                macros = self._ch_ctl.get_macros()
                zk_ctl = ZookeeperCTL(self._zk_config)
                zk_ctl.delete_replicated_database_metadata(get_database_zookeeper_paths(databases_to_create),
                                                           kwargs['replica_name'], macros)
            for db_sql in databases_to_create:
                self._ch_ctl.restore_database(db_sql)

    def _backup_database(self, backup_meta: BackupMetadata, db_name: str) -> None:
        """
        Backup database.
        """
        logging.debug('Performing database backup for "%s"', db_name)

        if not _has_embedded_metadata(db_name):
            schema = self._ch_ctl.get_database_schema(db_name)
            self._backup_layout.upload_database_create_statement(backup_meta.name, db_name, schema)

        backup_meta.add_database(db_name)

        self._backup_layout.upload_backup_metadata(backup_meta)


def _has_embedded_metadata(db_name: str) -> bool:
    """
    Return True if db create statement shouldn't be uploaded and applied with restore.
    """
    return db_name in [
        'default',
        'system',
        '_temporary_and_external_tables',
        'information_schema',
        'INFORMATION_SCHEMA',
    ]
