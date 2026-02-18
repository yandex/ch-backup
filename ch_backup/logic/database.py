"""
Clickhouse backup logic for databases
"""

import time
from typing import Dict, Iterable, List, Optional, Sequence

from tenacity import (
    retry,
    retry_if_exception_type,
    retry_if_not_exception_message,
    stop_after_attempt,
    stop_after_delay,
    wait_random_exponential,
)

from ch_backup import logging
from ch_backup.backup_context import BackupContext
from ch_backup.clickhouse.client import ClickhouseError
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

    def backup(
        self, context: BackupContext, databases: Sequence[Database]
    ) -> list[Database]:
        """
        Backup database objects metadata.
        """
        backed_up_databases = [
            db for db in databases if self._backup_database(context, db)
        ]
        context.backup_layout.wait()
        return backed_up_databases

    @staticmethod
    def restore(
        context: BackupContext,
        databases: Dict[str, Database],
        keep_going: bool,
        metadata_cleaner: Optional[MetadataCleaner],
    ) -> List[Database]:
        """
        Restore database objects.
        """
        # pylint: disable=too-many-branches
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
        return list(databases_to_restore.values())

    @staticmethod
    def wait_sync_replicated_databases(
        context: BackupContext, databases: Iterable[Database], keep_going: bool
    ) -> None:
        """
        Call SYNC DATABASE REPLICA for replicated databases.
        """
        if context.config["force_non_replicated"]:
            logging.info("Skipping synchronizing replicated database replicas.")
            return

        logging.info("Synchronizing replicated database replicas")

        # Common deadline for all databases
        deadline = time.time() + context.ch_ctl_conf["sync_database_replica_timeout"]
        max_retries = context.ch_ctl_conf["sync_database_replica_max_retries"]
        max_backoff = context.ch_ctl_conf["sync_database_replica_max_backoff"]

        for db in databases:
            if db.is_replicated_db_engine():
                try:
                    logging.info(f"Synchronizing replicated database: {db.name}")
                    DatabaseBackup._sync_replicated_database_with_retries(
                        context, db.name, deadline, max_retries, max_backoff
                    )
                except Exception as e:
                    if keep_going:
                        logging.exception(
                            f"Sync of replicated database {db.name} failed, skipping due to --keep-going flag. Reason {e}"
                        )
                    else:
                        raise

    @staticmethod
    def _backup_database(context: BackupContext, db: Database) -> bool:
        """
        Backup database.
        """
        logging.debug('Performing database backup for "{}"', db.name)

        if not db.has_embedded_metadata():
            if not context.backup_layout.upload_database_create_statement(
                context.backup_meta, db
            ):
                return False

        context.backup_meta.add_database(db)
        context.backup_layout.upload_backup_metadata(context.backup_meta)
        return True

    @staticmethod
    def _sync_replicated_database_with_retries(
        context: BackupContext,
        db_name: str,
        deadline: float,
        max_retries: int,
        max_backoff: int,
    ) -> None:
        """
        Synchronize Replicated Database replica with retries.
        """
        timeout_exceeded_exception_pattern = r".*Timeout exceeded.*"
        time_left = max(deadline - time.time(), 1)

        retry_decorator = retry(
            retry=(
                retry_if_exception_type(ClickhouseError)
                & retry_if_not_exception_message(
                    match=timeout_exceeded_exception_pattern
                )
            ),
            stop=(stop_after_attempt(max_retries) | stop_after_delay(time_left)),
            wait=wait_random_exponential(max=max_backoff),
            reraise=True,
        )

        @retry_decorator
        def execute_sync():
            current_time_left = max(deadline - time.time(), 1)
            settings = {"receive_timeout": current_time_left}
            context.ch_ctl.system_sync_database_replica(
                db_name, timeout=int(current_time_left), settings=settings
            )

        execute_sync()
