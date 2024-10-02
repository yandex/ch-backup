"""
Clickhouse backup logic for databases
"""

from typing import Dict, Optional, Sequence

from ch_backup import logging
from ch_backup.backup_context import BackupContext
from ch_backup.clickhouse.metadata_cleaner import MetadataCleaner
from ch_backup.clickhouse.models import Database
from ch_backup.clickhouse.schema import (
    embedded_schema_db_sql,
    rewrite_database_schema,
    to_attach_query,
    to_create_query,
)
from ch_backup.logic.backup_manager import BackupManager


class DatabaseBackup(BackupManager):
    """
    Database backup class
    """

    def backup(self, context: BackupContext, databases: Sequence[Database]) -> None:
        """
        Backup database objects metadata.
        """
        for db in databases:
            self._backup_database(context, db)

        context.backup_layout.wait()

    @staticmethod
    def restore(
        context: BackupContext,
        databases: Dict[str, Database],
        keep_going: bool,
        metadata_cleaner: Optional[MetadataCleaner],
    ) -> None:
        """
        Restore database objects.
        """
        logging.debug("Retrieving list of databases")
        present_databases = {db.name: db for db in context.ch_ctl.get_databases()}

        databases_to_restore: Dict[str, Database] = {}
        for name, db in databases.items():
            if (
                name in present_databases
                and db.engine != present_databases[name].engine
            ):
                logging.debug(
                    f"Database engine mismatch({db.engine}!={present_databases[name].engine}), deleting"
                )
                context.ch_ctl.drop_database_if_exists(name)
                del present_databases[name]

            if name not in present_databases:
                databases_to_restore[name] = db
                continue

        if metadata_cleaner:
            replicated_databases = [
                database
                for database in databases_to_restore.values()
                if database.is_replicated_db_engine()
            ]
            metadata_cleaner.clean_database_metadata(replicated_databases)

        logging.info("Restoring databases: {}", ", ".join(databases_to_restore.keys()))
        for db in databases_to_restore.values():
            if db.has_embedded_metadata():
                db_sql = embedded_schema_db_sql(db)
            else:
                db_sql = context.backup_layout.get_database_create_statement(
                    context.backup_meta, db.name
                )
            try:
                if db.is_atomic() or db.has_embedded_metadata():
                    logging.debug(f"Going to restore database `{db.name}` using CREATE")
                    db_sql = to_create_query(db_sql)
                    db_sql = rewrite_database_schema(
                        db,
                        db_sql,
                        context.config["force_non_replicated"],
                        context.config["override_replica_name"],
                    )
                    logging.debug(f"Creating database `{db.name}`")
                    context.ch_ctl.restore_database(db_sql)
                else:
                    logging.debug(f"Going to restore database `{db.name}` using ATTACH")
                    db_sql = to_attach_query(db_sql)
                    context.backup_layout.write_database_metadata(db, db_sql)
                    logging.debug(f"Attaching database `{db.name}`")
                    context.ch_ctl.attach_database(db)
            except Exception as e:
                if keep_going:
                    logging.exception(
                        f"Restore of database {db.name} failed, skipping due to --keep-going flag. Reason {e}"
                    )
                else:
                    raise

        logging.info("All databases restored")

    @staticmethod
    def _backup_database(context: BackupContext, db: Database) -> None:
        """
        Backup database.
        """
        logging.debug('Performing database backup for "{}"', db.name)

        if not db.has_embedded_metadata():
            context.backup_layout.upload_database_create_statement(
                context.backup_meta, db
            )

        context.backup_meta.add_database(db)
        context.backup_layout.upload_backup_metadata(context.backup_meta)
