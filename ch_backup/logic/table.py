"""
Clickhouse backup logic for tables
"""

# pylint: disable=too-many-lines

import os
import shutil
from collections import deque
from contextlib import AbstractContextManager, ExitStack, nullcontext
from dataclasses import dataclass
from functools import partial
from itertools import chain, count
from pathlib import Path
from random import choices
from string import ascii_lowercase
from typing import Iterable, Sequence
from uuid import uuid4

from ch_backup import logging
from ch_backup.backup.deduplication import StoredFiles, deduplicate_parts
from ch_backup.backup.metadata import DetachedPartMetadata, PartMetadata, TableMetadata
from ch_backup.backup.metadata.table_metadata import DataSkippedReason
from ch_backup.backup.restore_context import PartState
from ch_backup.backup_context import BackupContext
from ch_backup.clickhouse.client import ClickhouseError
from ch_backup.clickhouse.disks import ClickHouseBackupDisks, ClickHouseTemporaryDisks
from ch_backup.clickhouse.metadata_cleaner import MetadataCleaner
from ch_backup.clickhouse.models import Database, Disk, FrozenPart, Table
from ch_backup.clickhouse.part_checksums import check_part_files
from ch_backup.clickhouse.schema import (
    rewrite_table_schema,
    to_attach_query,
    to_create_query,
)
from ch_backup.exceptions import ClickhouseBackupError, is_broken_executor
from ch_backup.logic.backup_manager import BackupManager
from ch_backup.logic.storage_schema_restore import (
    StorageSchemaRestore,
    restore_disk_name,
)
from ch_backup.logic.upload_part_observer import UploadPartObserver
from ch_backup.storage.async_pipeline.base_pipeline.exec_pool import (
    ThreadExecPool,
)
from ch_backup.util import compare_schema

RANDOM_TABLE_NAME_LENGTH = 16


@dataclass
class TableMetadataChangeTime:
    """
    Class contains timestamps of table metadata last modification.
    """

    metadata_path: str
    mtime_ns: int  # Time of most recent content modification expressed in nanoseconds.
    ctime_ns: int  # Time of most recent filesystem metadata(hardlinks) change expressed in nanoseconds.


@dataclass
class PreparedTableRestore:
    """Selected schemas and held locks shared by database and table restore."""

    tables_meta: list[TableMetadata]
    filtered_tables_meta: list[TableMetadata]
    tables: list[Table]
    duplicate_uuid_skipped: int
    missing_create_statement_skipped: int
    storage_restore: StorageSchemaRestore | None
    schema_locks: ExitStack


class TableBackup(BackupManager):
    """
    Table backup class
    """

    # pylint: disable=too-many-positional-arguments,too-many-arguments
    def backup(
        self,
        context: BackupContext,
        databases: Sequence[Database],
        db_tables: dict[str, list],
        schema_only: bool,
        multiprocessing_config: dict,
        include_detached_parts: bool = False,
        detached_parts_disks: Sequence[str] = None,
        detached_parts_reasons: Sequence[str] = None,
    ) -> None:
        """
        Backup tables metadata, MergeTree data and Cloud storage metadata.

        Frozen data holds the objects of cloud storage disks. Once the data is
        copied into the backup it is unfrozen, so that those objects are not
        kept in the bucket of the disk for as long as the backup lives. This
        happens only after the temporary disks are gone: their frozen data
        refers to the copies, and unfreezing it would delete them.
        """

        backup_name = context.backup_meta.get_sanitized_name()

        encrypted = context.cloud_conf.get("encryption", False)
        if encrypted:
            logging.debug('Cloud storage "shadow" backup will be encrypted')
        context.backup_meta.cloud_storage.encrypted = encrypted

        compressed = context.cloud_conf.get("compression", True)
        if compressed:
            logging.debug('Cloud storage "shadow" backup will be compressed')
        context.backup_meta.cloud_storage.compressed = compressed

        data_copied = context.cloud_conf.get("copy_data", False) and not schema_only
        if data_copied:
            logging.debug("Cloud storage data will be copied into the backup")
        context.backup_meta.cloud_storage.data_copied = data_copied

        # Since https://github.com/ClickHouse/ClickHouse/pull/75016
        if (
            context.ch_ctl.ch_version_ge("25.2")
            and context.config["kill_old_freeze_queries"]
        ):
            context.ch_ctl.kill_old_freeze_queries()

        # Collect modification timestamps of table metadata files for all databases
        # for optimistic concurrency control of backup creation.
        # To ensure consistency between metadata and data backups in case of EXCHANGE TABLES
        # within a single backup run.
        # See https://en.wikipedia.org/wiki/Optimistic_concurrency_control
        change_times = self._collect_local_metadata_change_times(
            context, databases, db_tables
        )

        copy_workers = multiprocessing_config.get("cloud_storage_backup_workers", 1)
        if (
            data_copied
            and copy_workers > 1
            and not context.ch_ctl.ch_version_ge("23.3")
        ):
            raise ClickhouseBackupError(
                "It is unsafe to use cloud_storage_backup_workers > 1 with clickhouse version < 23.3"
                f" (cloud_storage_backup_workers: {copy_workers}, ch_version: {context.ch_ctl.get_version()})"
            )

        disks_context: AbstractContextManager[ClickHouseBackupDisks | None] = (
            nullcontext()
        )
        if data_copied:
            disks_context = ClickHouseBackupDisks(
                context.ch_ctl,
                context.backup_layout,
                context.config_root,
                context.backup_meta,
                context.ch_config,
            )
        with disks_context as backup_disks:
            for db in databases:
                self._backup(
                    context,
                    db,
                    db_tables[db.name],
                    backup_name,
                    schema_only,
                    multiprocessing_config,
                    change_times,
                    backup_disks,
                    include_detached_parts,
                    detached_parts_disks,
                    detached_parts_reasons,
                )
        if data_copied:
            context.ch_ctl.system_unfreeze(backup_name)

    def _collect_local_metadata_change_times(
        self,
        context: BackupContext,
        databases: Sequence[Database],
        db_tables: dict[str, list],
    ) -> dict[Table, TableMetadataChangeTime]:
        """
        Collect modification timestamps of table metadata files.
        """
        logging.debug("Collecting local metadata modification times")
        res: dict[Table, TableMetadataChangeTime] = {}

        for db in databases:
            if db.is_external_db_engine():
                continue
            for table in context.ch_ctl.get_tables(db.name, db_tables[db.name]):
                change_time = self._get_change_time(table.metadata_path)
                if change_time is None:
                    logging.warning(
                        'Cannot get metadata change time for table "{}"."{}". Skipping it',
                        table.database,
                        table.name,
                    )
                    continue

                res[table] = change_time

        return res

    # pylint: disable=too-many-positional-arguments
    def _backup(
        self,
        context: BackupContext,
        db: Database,
        tables: Sequence[str],
        backup_name: str,
        schema_only: bool,
        multiprocessing_config: dict,
        change_times: dict[Table, TableMetadataChangeTime],
        backup_disks: ClickHouseBackupDisks | None,
        include_detached_parts: bool,
        detached_parts_disks: Sequence[str] = None,
        detached_parts_reasons: Sequence[str] = None,
    ) -> None:
        """
        Backup single database tables.
        """
        if not db.is_external_db_engine():
            tables_ = [
                table
                for table in context.ch_ctl.get_tables(db.name, tables)
                if table in change_times
            ]

            create_statements_to_backup = []

            # Create shadow/increment.txt if not exists manually to avoid
            # race condition with parallel freeze
            context.ch_ctl.create_shadow_increment()
            try:
                with (
                    ThreadExecPool(
                        multiprocessing_config.get("cloud_storage_backup_workers", 1)
                    ) as copy_pool,
                    ThreadExecPool(
                        multiprocessing_config.get("freeze_threads", 1)
                    ) as pool,
                ):
                    for table in tables_:
                        pool.submit(
                            f'Freeze table "{table.database}"."{table.name}"',
                            TableBackup._freeze_table,
                            context,
                            db,
                            table,
                            backup_name,
                            schema_only,
                            change_times[table],
                            multiprocessing_config.get(
                                "parallelize_freeze_in_clickhouse", False
                            ),
                            multiprocessing_config.get("freeze_partition_threads", 0),
                            multiprocessing_config.get(
                                "freeze_table_query_max_threads", 0
                            ),
                        )

                    for freezed_table in pool.as_completed(keep_going=False):
                        if freezed_table is not None:
                            if self._check_metadata_change_time(
                                context,
                                freezed_table,
                                backup_name,
                                change_times[freezed_table],
                            ):
                                create_statements_to_backup.append(
                                    (freezed_table.name, freezed_table.create_statement)
                                )
                                context.backup_meta.add_table(
                                    TableMetadata(
                                        freezed_table.database,
                                        freezed_table.name,
                                        freezed_table.engine,
                                        freezed_table.uuid,
                                        freezed_table.data_skipped_reason,
                                    )
                                )
                                if (
                                    not schema_only
                                    and not freezed_table.data_skipped_reason
                                ):
                                    self._backup_table_data(
                                        context,
                                        freezed_table,
                                        backup_name,
                                        backup_disks,
                                        copy_pool,
                                    )
                                if include_detached_parts and not schema_only:
                                    self._backup_detached_table_data(
                                        context,
                                        freezed_table,
                                        detached_parts_disks,
                                        detached_parts_reasons,
                                    )

                    self._upload_cloud_storage_metadata(context, copy_pool)
            finally:
                if create_statements_to_backup:
                    context.backup_layout.upload_create_statements(
                        context.backup_meta, db, create_statements_to_backup
                    )

            context.backup_layout.wait()
            context.ch_ctl.remove_freezed_data()

        context.backup_layout.upload_backup_metadata(context.backup_meta)

    # pylint: disable=too-many-positional-arguments
    @staticmethod
    def _freeze_table(
        context: BackupContext,
        db: Database,
        table: Table,
        backup_name: str,
        schema_only: bool,
        expected_change_time: TableMetadataChangeTime,
        parallelize_freeze_in_ch: bool,
        freeze_partition_threads: int,
        freeze_table_query_max_threads: int,
    ) -> Table | None:
        """
        Freeze table and return it's create statement
        """
        logging.debug('Trying to freeze "{}"."{}"', table.database, table.name)
        create_statement = TableBackup._load_create_statement_from_disk(table)
        table.create_statement = create_statement or ""
        if not create_statement:
            logging.warning(
                'Skipping table backup for "{}"."{}". Local metadata is empty or absent',
                db.name,
                table.name,
            )
            return None

        if not schema_only and table.has_unique_key():
            logging.warning(
                'Backing up schema only for "{}"."{}". ClickHouse rejects '
                "ALTER ... PARTITION on UNIQUE KEY tables, so their data cannot be frozen",
                table.database,
                table.name,
            )
            table.data_skipped_reason = DataSkippedReason.UNIQUE_KEY
            return table

        # Freeze only MergeTree tables
        if not schema_only and table.is_merge_tree():
            try:
                context.ch_ctl.freeze_table(
                    backup_name,
                    table,
                    parallelize_freeze_in_ch,
                    freeze_partition_threads,
                    freeze_table_query_max_threads,
                )
            except ClickhouseError:
                if TableBackup._check_metadata_change_time(
                    context,
                    table,
                    backup_name,
                    expected_change_time,
                    check_table_exists=True,
                ):
                    logging.error(
                        'Cannot freeze table "{}"."{}"',
                        table.database,
                        table.name,
                        exc_info=True,
                    )
                    if not context.config["keep_going"]:
                        raise
                    table.data_skipped_reason = DataSkippedReason.BACKUP_FAILED
                    return table

                return None

        return table

    @staticmethod
    def _load_create_statement_from_disk(table: Table) -> str | None:
        """
        Load a create statement of the table from a metadata file on the disk.
        """
        if not table.metadata_path:
            logging.debug(
                'Cannot load a create statement of the table "{}"."{}". Metadata is empty',
                table.database,
                table.name,
            )
            return None
        try:
            return Path(table.metadata_path).read_text(
                "utf-8", errors="surrogateescape"
            )
        except OSError as e:
            logging.debug(
                'Cannot load a create statement of the table "{}"."{}": {}',
                table.database,
                table.name,
                str(e),
            )
            return None

    def _backup_table_data(
        self,
        context: BackupContext,
        table: Table,
        backup_name: str,
        backup_disks: ClickHouseBackupDisks | None,
        copy_pool: ThreadExecPool,
    ) -> None:
        """
        Backup data of a table, scheduling the copy of its cloud storage data.

        With keep_going, a part on a local disk that cannot be read or uploaded is
        left out of the backup, and any other failure leaves out the data of the
        whole table. A broken upload pool fails the backup all the same, and so
        do a failed check of uploaded parts and a failure to delete data left out
        after it was stored.
        """
        try:
            uploaded_parts = self._backup_frozen_table_data(
                context, table, backup_name, backup_disks
            )
        except Exception as e:
            if not context.config["keep_going"] or is_broken_executor(e):
                raise
            self._skip_failed_table_data(context, table)
            return

        if uploaded_parts is not None:
            context.backup_layout.wait(keep_going=context.config["keep_going"])
            self._validate_uploaded_parts(context, uploaded_parts)
            context.ch_ctl.remove_freezed_data(backup_name, table)

        try:
            self._backup_cloud_storage_data(context, copy_pool, table, backup_disks)
        except Exception as e:
            if not context.config["keep_going"] or is_broken_executor(e):
                raise
            self._skip_failed_table_data(context, table)

    @staticmethod
    def _skip_failed_table_data(context: BackupContext, table: Table) -> None:
        """
        Leave data of a table out of the backup after a failure to back it up.

        Uploads of the table already started are awaited first, so that none of
        its parts is added to the backup metadata afterwards.
        """
        logging.error(
            'Backing up data of table "{}"."{}" failed, backing up its schema only',
            table.database,
            table.name,
            exc_info=True,
        )
        context.backup_layout.wait(keep_going=True)
        TableBackup._drop_table_data(context, table)

    @staticmethod
    def _drop_table_data(context: BackupContext, table: Table) -> None:
        """
        Drop data of a table from the backup, deleting the parts uploaded so far.

        Once out of the metadata, those parts would not be found by deletion of
        a backup whose other parts are reused by later backups. A table dropped
        already is left as it is.
        """
        uploaded_disks = {
            disk.name for _, disk in table.paths_with_disks if disk.type != "s3"
        }
        table_meta = context.backup_meta.get_table(table.database, table.name)
        if table_meta.data_skipped_reason == DataSkippedReason.BACKUP_FAILED:
            return
        context.backup_layout.delete_data_parts(
            context.backup_meta,
            [
                part
                for part in table_meta.get_parts()
                if part.disk_name in uploaded_disks and not part.link
            ],
            is_async=False,
        )
        table.data_skipped_reason = DataSkippedReason.BACKUP_FAILED
        context.backup_meta.skip_table_data(
            table.database, table.name, DataSkippedReason.BACKUP_FAILED
        )

    @staticmethod
    def _skip_failed_part(
        context: BackupContext,
        table: Table,
        part_name: str,
        error: BaseException,
        uploaded: PartMetadata | None = None,
    ) -> bool:
        """
        Leave a data part that cannot be read or uploaded out of the backup.

        It is called while the error is handled, so the traceback is logged. An
        upload may fail after its data is stored, so that data is deleted. A
        broken pool or a terminating signal fails the backup, so it is ignored.
        Return True if the part is left out.
        """
        if not isinstance(error, Exception) or is_broken_executor(error):
            return False
        logging.error(
            'Backing up part "{}" of table "{}"."{}" failed, leaving it out',
            part_name,
            table.database,
            table.name,
            exc_info=True,
        )
        if uploaded:
            context.backup_layout.delete_data_parts(
                context.backup_meta, [uploaded], is_async=False
            )
        context.backup_meta.skip_part(table.database, table.name, part_name)
        return True

    @staticmethod
    def _backup_cloud_storage_data(
        context: BackupContext,
        pool: ThreadExecPool,
        table: Table,
        backup_disks: ClickHouseBackupDisks | None,
    ) -> None:
        """
        Schedule backup of cloud storage data of a table: a copy of the data
        when backup disks are given, otherwise its metadata files alone.

        Data of every disk is copied on its own, so that copies of different
        tables and disks go in parallel.
        """
        logging.debug(
            'Backing up Cloud Storage disks "shadow" directory of "{}"."{}"',
            table.database,
            table.name,
        )
        for _, disk in table.paths_with_disks:
            if disk.type != "s3" or disk.cache_path:
                continue

            if not context.backup_layout.has_frozen_cloud_storage_data(
                context.backup_meta, disk, table
            ):
                logging.debug(f'No data frozen on disk "{disk.name}", skipping')
                continue

            pool.submit(
                f'Backup of disk "{disk.name}" of "{table.database}"."{table.name}"',
                TableBackup._copy_cloud_storage_data,
                context,
                table,
                disk,
                backup_disks,
            )

    @staticmethod
    def _copy_cloud_storage_data(
        context: BackupContext,
        table: Table,
        disk: Disk,
        backup_disks: ClickHouseBackupDisks | None,
    ) -> tuple[Table, Disk, Disk | None]:
        """
        Copy data of a table on a cloud storage disk into the backup.

        The result is checked explicitly, since clickhouse-disks reports its
        errors with a zero exit code. A failure is reported on the table, as
        the backup metadata is not meant to be used from several threads.
        """
        if not backup_disks:
            return table, disk, None

        try:
            source_disk = backup_disks.copy_table_data(disk.name, table)
            if not context.backup_layout.has_frozen_cloud_storage_data(
                context.backup_meta, source_disk, table
            ):
                raise ClickhouseBackupError(
                    f'Copying data of disk "{disk.name}" of table '
                    f"`{table.database}`.`{table.name}` produced no metadata"
                )
        except Exception:
            if not context.config["keep_going"]:
                raise
            logging.error(
                'Copying data of disk "{}" of table "{}"."{}" failed, backing up'
                " its schema only",
                disk.name,
                table.database,
                table.name,
                exc_info=True,
            )
            table.data_skipped_reason = DataSkippedReason.BACKUP_FAILED
            return table, disk, None

        return table, disk, source_disk

    @staticmethod
    def _upload_cloud_storage_metadata(
        context: BackupContext, pool: ThreadExecPool
    ) -> None:
        """
        Upload cloud storage metadata files of tables as their data is copied.

        Uploading stays in the calling thread: both the upload pipeline and the
        backup metadata are not meant to be used from several threads.
        """
        for table, disk, source_disk in pool.as_completed(keep_going=False):
            if table.data_skipped_reason == DataSkippedReason.BACKUP_FAILED:
                TableBackup._drop_table_data(context, table)
                continue

            context.backup_layout.upload_cloud_storage_metadata(
                context.backup_meta, disk, table, source_disk=source_disk
            )
            context.backup_meta.cloud_storage.add_disk(disk.name)

    # pylint: disable=too-many-positional-arguments,too-many-arguments,too-many-locals
    def prepare_restore(
        self,
        context: BackupContext,
        databases: dict[str, Database],
        tables: list[TableMetadata],
        keep_going: bool,
        skip_cloud_storage: bool,
        restore_tables_in_replicated_database: bool,
        schema_locks: ExitStack,
    ) -> PreparedTableRestore:
        """Prepare and check schemas before any database or table can be dropped."""
        logging.debug("Retrieving tables metadata")
        tables_meta: list[TableMetadata] = list(
            chain(
                *[
                    context.backup_meta.get_tables(db.name)
                    for db in databases.values()
                    if not db.is_external_db_engine()
                ]
            )
        )

        if tables:
            db_tables = [(table.database, table.name) for table in tables_meta]

            logging.debug("Checking for presence of required tables")

            missed_tables = [
                table
                for table in tables
                if (table.database, table.name) not in db_tables
            ]
            if missed_tables:
                missed_tables_names = ", ".join(
                    f"{t.database}.{t.name}" for t in missed_tables
                )
                if not keep_going:
                    raise ClickhouseBackupError(
                        f"Required tables {missed_tables_names} were not found in backup metadata"
                    )
                logging.warning(
                    "Required tables {} were not found in backup metadata, skipping due to --keep-going flag",
                    missed_tables_names,
                )
            else:
                logging.debug("All required tables are present")

            logging.debug("Leaving only required tables metadata")
            required_tables = [(table.database, table.name) for table in tables]
            tables_meta = list(
                filter(lambda t: (t.database, t.name) in required_tables, tables_meta)
            )

            if not tables_meta:
                logging.warning(
                    "No tables remain to restore after filtering missing required tables"
                )
                return PreparedTableRestore([], [], [], 0, 0, None, schema_locks)

        filtered_tables_meta, duplicate_uuid_skipped = self._filter_duplicate_uuid(
            tables_meta, keep_going
        )

        logging.debug("Retrieving tables from tables metadata")
        (
            tables_to_preprocess,
            missing_create_statement_skipped,
        ) = self._get_tables_from_meta(context, filtered_tables_meta, keep_going)
        tables_with_create_statements = {
            (table.database, table.name) for table in tables_to_preprocess
        }
        tables_meta = [
            table
            for table in tables_meta
            if (table.database, table.name) in tables_with_create_statements
        ]
        if len(tables_meta) == 0:
            logging.warning("No tables remain to restore after loading table schemas")
            return PreparedTableRestore(
                [],
                filtered_tables_meta,
                [],
                duplicate_uuid_skipped,
                missing_create_statement_skipped,
                None,
                schema_locks,
            )

        storage_restore = StorageSchemaRestore(context) if skip_cloud_storage else None
        if storage_restore is not None:
            tables_to_preprocess = self._adapt_storage_schemas(
                context,
                databases,
                tables_to_preprocess,
                storage_restore,
                keep_going,
                restore_tables_in_replicated_database,
            )
            storage_restore.lock_tables(tables_to_preprocess, schema_locks)
            storage_restore.check_existing_tables(tables_to_preprocess)
        return PreparedTableRestore(
            tables_meta,
            filtered_tables_meta,
            tables_to_preprocess,
            duplicate_uuid_skipped,
            missing_create_statement_skipped,
            storage_restore,
            schema_locks,
        )

    # pylint: disable=too-many-arguments,too-many-locals,too-many-positional-arguments
    def restore(
        self,
        context: BackupContext,
        databases: dict[str, Database],
        schema_only: bool,
        tables: list[TableMetadata],
        metadata_cleaner: MetadataCleaner | None,
        cloud_storage_source_bucket: str | None,
        cloud_storage_source_path: str | None,
        cloud_storage_source_endpoint: str | None,
        skip_cloud_storage: bool,
        restore_detached_parts: bool,
        keep_going: bool,
        restore_tables_in_replicated_database: bool,
        prepared_restore: PreparedTableRestore | None = None,
    ) -> None:
        """
        Restore tables and MergeTree data.
        """
        with ExitStack() as schema_locks:
            prepared = prepared_restore
            if prepared is None:
                prepared = self.prepare_restore(
                    context,
                    databases,
                    tables,
                    keep_going,
                    skip_cloud_storage,
                    restore_tables_in_replicated_database,
                    schema_locks,
                )
            else:
                # The caller acquired these locks before database restore. Release
                # them after CREATE, without holding up other replicas during data loading.
                schema_locks.enter_context(prepared.schema_locks)
            if len(prepared.tables_meta) == 0:
                return
            tables_meta = prepared.tables_meta
            filtered_tables_meta = prepared.filtered_tables_meta
            tables_to_preprocess = prepared.tables
            storage_restore = prepared.storage_restore
            (
                tables_to_restore,
                preprocessing_failed_skipped,
                preprocessing_failed_tables,
            ) = self._preprocess_tables_to_restore(
                context,
                databases,
                tables_to_preprocess,
                keep_going,
                restore_tables_in_replicated_database,
                metadata_cleaner,
                storage_restore=storage_restore,
            )
            self._log_restore_preprocess_summary(
                duplicate_uuid_skipped=prepared.duplicate_uuid_skipped,
                missing_create_statement_skipped=prepared.missing_create_statement_skipped,
                preprocessing_failed_skipped=preprocessing_failed_skipped,
            )

            failed_tables = self._restore_tables(
                context,
                databases,
                tables_to_restore,
                keep_going,
            )

        if storage_restore is not None:
            # A valid rewrite can still fail during preprocessing or CREATE. Do
            # not report success with keep-going merely because parsing succeeded.
            for key in preprocessing_failed_tables | {
                (table.database, table.name) for table in failed_tables
            }:
                storage_restore.errors.setdefault(key, "Table schema restore failed")

        if schema_only:
            logging.debug(
                "Skipping restoring of table data as --schema-only flag passed"
            )
            if storage_restore is not None:
                # Keep-going may have restored other schemas successfully; it
                # must still report any deferred adaptation errors to the caller.
                storage_restore.raise_errors()
            return

        failed_tables_names = [f"`{t.database}`.`{t.name}`" for t in failed_tables]
        # Never load parts into a table whose schema preparation or creation failed.
        tables_to_restore_data = list(
            filter(
                lambda t: f"`{t.database}`.`{t.name}`" not in failed_tables_names
                and (t.database, t.name) not in preprocessing_failed_tables
                and (
                    storage_restore is None
                    or (t.database, t.name) not in storage_restore.errors
                ),
                tables_meta,
            )
        )

        tables_with_create_statements = {
            (table.database, table.name) for table in tables_to_preprocess
        }
        tables_to_restore_detached = [
            table
            for table in filtered_tables_meta
            if (table.database, table.name) in tables_with_create_statements
            and (table.database, table.name) not in preprocessing_failed_tables
            and f"`{table.database}`.`{table.name}`" not in failed_tables_names
        ]

        use_inplace_cloud_restore = context.config_root["restore"][
            "use_inplace_cloud_restore"
        ]

        with ClickHouseTemporaryDisks(
            context.ch_ctl,
            context.backup_layout,
            context.config_root,
            context.backup_meta,
            cloud_storage_source_bucket,
            cloud_storage_source_path,
            cloud_storage_source_endpoint,
            context.ch_config,
            desired_tables=tables_meta,
            use_local_copy=use_inplace_cloud_restore,
            skip_cloud_storage=skip_cloud_storage,
        ) as disks:
            self._restore_data(
                context,
                tables=tables_to_restore_data,
                disks=disks,
                skip_cloud_storage=skip_cloud_storage,
                keep_going=keep_going,
                storage_disks=(
                    storage_restore.storage_disks
                    if storage_restore is not None
                    else None
                ),
            )

        if restore_detached_parts and any(
            table.get_detached_parts() for table in tables_to_restore_detached
        ):
            self._restore_detached_data(
                context,
                tables=tables_to_restore_detached,
                skip_cloud_storage=skip_cloud_storage,
                keep_going=keep_going,
                storage_disks=(
                    storage_restore.storage_disks
                    if storage_restore is not None
                    else None
                ),
            )
        if storage_restore is not None:
            # Report deferred errors only after independent tables and their parts
            # have been restored, so keep-going preserves useful completed work.
            storage_restore.raise_errors()

    @staticmethod
    def _check_metadata_change_time(
        context: BackupContext,
        table: Table,
        backup_name: str,
        expected_change_time: TableMetadataChangeTime,
        *,
        check_table_exists: bool = False,
    ) -> bool:
        """
        Check table metadata and clean up frozen data if the table changed or disappeared.
        Check existence in ClickHouse only when handling a freeze error.
        """
        current_change_time = TableBackup._get_change_time(table.metadata_path)
        if (
            current_change_time is not None
            and current_change_time == expected_change_time
            and (
                not check_table_exists
                or context.ch_ctl.does_table_exist(table.database, table.name)
            )
        ):
            return True

        logging.warning(
            'Skipping table backup for "{}"."{}". The metadata file was updated or removed, or the table was removed during backup',
            table.database,
            table.name,
        )
        context.ch_ctl.remove_freezed_data(backup_name, table)
        return False

    # pylint: disable=too-many-statements
    def _backup_frozen_table_data(
        self,
        context: BackupContext,
        table: Table,
        backup_name: str,
        backup_disks: ClickHouseBackupDisks | None,
    ) -> list[PartMetadata] | None:
        """
        Backup table with data opposed to schema only.

        Return the list the uploaded parts are added to as the caller awaits
        their uploads, or None for a table that keeps no data parts. Without
        upload workers a failed upload raises at once, after its handler ran.
        """

        def deduplicate_parts_in_batch(
            context: BackupContext,
            upload_observer: UploadPartObserver,
            frozen_parts: dict[str, FrozenPart],
        ) -> None:
            logging.debug(
                "Working on deduplication of {} frozen parts", len(frozen_parts)
            )
            deduplicated_parts = deduplicate_parts(
                context, table.database, table.name, frozen_parts, stored_files
            )
            logging.debug(
                "{} out of {} parts are deduplicated",
                len(deduplicated_parts),
                len(frozen_parts),
            )

            for part_name in frozen_parts:
                if part_name in deduplicated_parts:
                    context.ch_ctl.remove_freezed_part(frozen_parts[part_name])
                    context.backup_meta.add_part(deduplicated_parts[part_name])
                else:
                    part = PartMetadata.from_frozen_part(
                        frozen_parts[part_name], context.backup_meta.encrypted
                    )
                    try:
                        context.backup_layout.upload_data_part(
                            context.backup_meta,
                            frozen_parts[part_name],
                            partial(upload_observer, part),
                            on_error=(
                                partial(skip_failed_upload, part)
                                if skip_failed_part
                                else None
                            ),
                        )
                    except Exception:
                        if part_name not in left_out_parts:
                            raise
            frozen_parts.clear()

        def deduplicate_cloud_parts_in_batch(
            context: BackupContext,
            frozen_parts: dict[str, FrozenPart],
        ) -> None:
            """
            Deduplicate parts stored on cloud storage disks.

            Frozen data of a deduplicated part is moved aside, so that the part
            is left out of the copy of the table data.
            """
            assert backup_disks, "Cloud storage parts are deduplicated without disks"
            matched_parts = deduplicate_parts(
                context,
                table.database,
                table.name,
                frozen_parts,
                stored_files,
                cloud_storage=True,
            )
            deduplicated_parts = {
                name: part
                for name, part in matched_parts.items()
                if part.disk_name == frozen_parts[name].disk_name
            }
            logging.debug(
                "{} out of {} cloud storage parts are deduplicated",
                len(deduplicated_parts),
                len(frozen_parts),
            )

            for part_name, frozen_part in frozen_parts.items():
                context.backup_meta.add_part(
                    deduplicated_parts.get(part_name)
                    or PartMetadata.from_frozen_part(
                        frozen_part, context.backup_meta.encrypted
                    )
                )

            backup_disks.exclude_frozen_parts(
                {disk.name: disk for _, disk in table.paths_with_disks},
                [frozen_parts[name] for name in deduplicated_parts],
            )
            frozen_parts.clear()

        logging.debug(
            'Performing table backup for "{}"."{}"', table.database, table.name
        )

        if not table.is_merge_tree():
            logging.info(
                'Skipping table data backup for non MergeTree table "{}"."{}"',
                table.database,
                table.name,
            )
            return None

        logging.debug('Uploading table data for "{}"."{}"', table.database, table.name)

        upload_observer = UploadPartObserver(context)
        stored_files: StoredFiles = {}
        skip_failed_part = (
            partial(TableBackup._skip_failed_part, context, table)
            if context.config["keep_going"]
            else None
        )
        left_out_parts: set[str] = set()

        def skip_failed_upload(part: PartMetadata, error: BaseException) -> None:
            """
            Leave out a part whose upload failed, noting that it is left out.
            """
            if TableBackup._skip_failed_part(
                context, table, part.name, error, uploaded=part
            ):
                left_out_parts.add(part.name)

        frozen_parts_batch: dict[str, FrozenPart] = {}
        cloud_parts_batch: dict[str, FrozenPart] = {}
        dedup_batch_size = context.config["deduplication_batch_size"]
        for data_path, disk in table.paths_with_disks:
            for fpart in context.ch_ctl.scan_frozen_parts(
                table,
                disk,
                data_path,
                backup_name,
                backup_disks is not None,
                context.config["fail_on_unparsable_part_metadata"],
                on_error=skip_failed_part if disk.type == "local" else None,
            ):
                logging.debug("Working on {}", fpart)
                if disk.type == "s3":
                    if disk.cache_path or backup_disks is None:
                        context.backup_meta.add_part(
                            PartMetadata.from_frozen_part(
                                fpart, context.backup_meta.encrypted
                            )
                        )
                        continue

                    context.backup_meta.cloud_storage.add_disk(disk.name)
                    cloud_parts_batch[fpart.name] = fpart
                    if len(cloud_parts_batch) >= dedup_batch_size:
                        deduplicate_cloud_parts_in_batch(context, cloud_parts_batch)
                    continue

                frozen_parts_batch[fpart.name] = fpart
                if len(frozen_parts_batch) >= dedup_batch_size:
                    deduplicate_parts_in_batch(
                        context,
                        upload_observer,
                        frozen_parts_batch,
                    )
        if frozen_parts_batch:
            deduplicate_parts_in_batch(
                context,
                upload_observer,
                frozen_parts_batch,
            )
        if cloud_parts_batch:
            deduplicate_cloud_parts_in_batch(context, cloud_parts_batch)

        return upload_observer.uploaded_parts

    def _backup_detached_table_data(
        self,
        context: BackupContext,
        table: Table,
        disk_names: Sequence[str] = None,
        reasons: Sequence[str] = None,
    ) -> None:
        """Back up matching detached parts while preserving directory names."""
        if not table.is_merge_tree():
            return

        disk_filter = set(disk_names or [])
        reason_filter = set(reasons or [])
        uploaded_parts: list[DetachedPartMetadata] = []
        warned_metadata_only = False

        def observe_uploaded_part(part: DetachedPartMetadata) -> None:
            uploaded_parts.append(part)
            context.backup_meta.add_detached_part(part)

        for detached_part in context.ch_ctl.scan_detached_parts(
            table, disk_names=disk_names, reasons=reasons
        ):
            if disk_filter and detached_part.disk_name not in disk_filter:
                continue
            if reason_filter and detached_part.reason not in reason_filter:
                continue

            part_meta = DetachedPartMetadata.from_detached_part(
                detached_part, context.backup_meta.encrypted
            )
            if (
                detached_part.object_storage
                and context.backup_meta.cloud_storage.data_copied
                and not warned_metadata_only
            ):
                logging.warning(
                    "Detached parts of {}.{} on object storage are metadata-only: "
                    "--copy-cloud-storage-data does not copy their referenced "
                    "objects; only local metadata files will be backed up",
                    detached_part.database,
                    detached_part.table,
                )
                warned_metadata_only = True
            try:
                context.backup_layout.upload_detached_part(
                    context.backup_meta,
                    detached_part,
                    partial(observe_uploaded_part, part_meta),
                )
            except Exception as e:
                if is_broken_executor(e):
                    raise
                logging.warning(
                    "Failed to upload detached part {} of {}.{} from disk {}; "
                    "skipping it because detached-parts backup is best effort: {}",
                    detached_part.name,
                    detached_part.database,
                    detached_part.table,
                    detached_part.disk_name,
                    e,
                )

        # Detached parts are not frozen and may disappear while their uploads
        # are still running. Keep the regular backup successful and retain
        # metadata only for uploads whose callbacks completed.
        context.backup_layout.wait(keep_going=True)
        self._validate_uploaded_detached_parts(context, uploaded_parts)

    @staticmethod
    def _validate_uploaded_parts(context: BackupContext, uploaded_parts: list) -> None:
        if context.config["validate_part_after_upload"]:
            invalid_parts = []

            for part in uploaded_parts:
                if not context.backup_layout.check_data_part(
                    context.backup_meta.name,
                    part,
                ):
                    invalid_parts.append(part)

            if invalid_parts:
                for part in invalid_parts:
                    logging.error(
                        f"Uploaded part is broken, {part.database}.{part.table}: {part.name}"
                    )
                raise RuntimeError(
                    f'Uploaded parts are broken, {", ".join(map(lambda p: p.name, invalid_parts))}'
                )

    @staticmethod
    def _validate_uploaded_detached_parts(
        context: BackupContext, uploaded_parts: list[DetachedPartMetadata]
    ) -> None:
        if not context.config["validate_part_after_upload"]:
            return

        invalid_parts = []
        for part in uploaded_parts:
            try:
                valid = context.backup_layout.check_detached_part(
                    context.backup_meta.name, part
                )
            except Exception as e:
                if is_broken_executor(e):
                    raise
                logging.warning(
                    "Failed to validate uploaded detached part {} of {}.{} "
                    "on disk {}; treating it as invalid: {}",
                    part.name,
                    part.database,
                    part.table,
                    part.disk_name,
                    e,
                )
                valid = False
            if not valid:
                invalid_parts.append(part)

        if not invalid_parts:
            return

        for part in invalid_parts:
            logging.warning(
                "Uploaded detached part is invalid and will be removed from "
                "the backup, {}.{} on {}: {}",
                part.database,
                part.table,
                part.disk_name,
                part.name,
            )

        try:
            context.backup_layout.delete_detached_parts(
                context.backup_meta, invalid_parts
            )
        except Exception as e:
            if is_broken_executor(e):
                raise
            logging.warning(
                "Failed to schedule cleanup of invalid detached parts; "
                "their archives may remain unreferenced: {}",
                e,
            )

        table = context.backup_meta.get_table(
            invalid_parts[0].database, invalid_parts[0].table
        )
        context.backup_meta.remove_detached_parts(table, invalid_parts)
        context.backup_layout.wait(keep_going=True)

    @staticmethod
    def _get_change_time(file_name: str) -> TableMetadataChangeTime | None:
        """
        Fetch change time of the table metadata file safely.
        """
        try:
            stat = os.stat(file_name)
            return TableMetadataChangeTime(
                metadata_path=file_name,
                mtime_ns=stat.st_mtime_ns,
                ctime_ns=stat.st_ctime_ns,
            )
        except OSError as e:
            logging.debug(f"Failed to get stat of {file_name}: {str(e)}")
            return None

    def _try_fix_broken_detached_table(
        self, context: BackupContext, detached_table: Table
    ) -> None:
        context.ch_ctl.create_metadata_for_missed_detached_table(
            detached_table.metadata_path, detached_table.uuid  # type: ignore
        )
        context.ch_ctl.attach_table(detached_table)
        context.ch_ctl.drop_table_if_exists(detached_table)

    # pylint: disable=too-many-positional-arguments
    def _adapt_storage_schemas(
        self,
        context: BackupContext,
        databases: dict[str, Database],
        tables: list[Table],
        storage_restore: StorageSchemaRestore,
        keep_going: bool,
        restore_tables_in_replicated_database: bool,
    ) -> list[Table]:
        result = []
        for table in tables:
            try:
                # Resolve names, UUIDs and engine conversion before choosing the
                # target policy or resolving a replicated table's Keeper path.
                self._rewrite_table_schema(
                    context, databases[table.database], table, add_uuid_if_required=True
                )
                # Replicated databases normally recreate tables themselves. Adapt
                # only tables that this restore is allowed to create explicitly.
                if (
                    not databases[table.database].is_replicated_db_engine()
                    or restore_tables_in_replicated_database
                ):
                    storage_restore.adapt(table)
                result.append(table)
            except (
                ClickhouseBackupError,
                ClickhouseError,
                OSError,
                ValueError,
            ) as error:
                if not keep_going:
                    raise
                # Exclude this table from subsequent schema/data restore, but save
                # the error so independent work can finish before restore fails.
                storage_restore.errors[(table.database, table.name)] = str(error)
                logging.exception(
                    "Cannot adapt storage schema of {}.{}; independent tables will be restored",
                    table.database,
                    table.name,
                )
        return result

    # pylint: disable=too-many-branches
    # pylint: disable=too-many-statements
    def _preprocess_tables_to_restore(
        self,
        context: BackupContext,
        databases: dict[str, Database],
        tables: list[Table],
        keep_going: bool,
        restore_tables_in_replicated_database: bool,
        metadata_cleaner: MetadataCleaner | None,
        storage_restore: StorageSchemaRestore | None = None,
    ) -> tuple[list[Table], int, set[tuple[str, str]]]:
        # Prepare table schema to restore.

        detached_tables_by_uuid = {}
        detached_tables_by_name = {}
        for table in context.ch_ctl.get_detached_tables():
            detached_tables_by_name[(table.database, table.name)] = table
            if table.uuid:
                detached_tables_by_uuid[table.uuid] = table

        # Filter out already restored tables.
        existing_tables_by_name = {}
        existing_tables_by_uuid = {}
        for table in context.ch_ctl.get_tables(short_query=True):
            existing_tables_by_name[(table.database, table.name)] = table
            if table.uuid:
                existing_tables_by_uuid[table.uuid] = table

        # In-place replica repair reuses the old local schema. Tables with adapted
        # DDL must instead go through drop and CREATE.
        existing_readonly_tables, readonly_cleaned_metadata = (
            self._get_remaining_readonly_tables_after_fix_attempt(
                context,
                [
                    table
                    for table in tables
                    if storage_restore is None or not storage_restore.changed(table)
                ],
                metadata_cleaner,
            )
        )

        result: list[Table] = []
        tables_to_clean_metadata: list[Table] = []
        preprocessing_failed_skipped = 0
        preprocessing_failed_tables: set[tuple[str, str]] = set()
        for table in tables:
            table_name_for_logs = f'"{table.database}"."{table.name}"'
            try:
                # Storage adaptation already rewrote names and UUIDs before locks
                # were taken; repeating it here could change the resolved path.
                if storage_restore is None:
                    self._rewrite_table_schema(
                        context,
                        databases[table.database],
                        table,
                        add_uuid_if_required=True,
                    )

                detached_table = None
                if table.uuid:
                    detached_table = detached_tables_by_uuid.get(table.uuid)
                if not detached_table:
                    detached_table = detached_tables_by_name.get(
                        (table.database, table.name)
                    )

                if detached_table:
                    try:
                        logging.debug(
                            "Table {} already exists as detached, attaching it",
                            table_name_for_logs,
                        )
                        context.ch_ctl.attach_table(detached_table)
                    except ClickhouseError as ch_err:
                        if "CANNOT_GET_CREATE_TABLE_QUERY" in str(ch_err):
                            logging.warning(
                                "Got an exception during attaching detached table. "
                                "Will ignore the ignore and try to restore table as usual {}",
                                repr(ch_err),
                            )
                            self._try_fix_broken_detached_table(context, detached_table)
                        else:
                            raise

                existing_table: Table | None = None
                if (table.database, table.name) in existing_readonly_tables:
                    existing_table = table
                    logging.warning(
                        f"Table {table_name_for_logs} will be recreated because it is in readonly state",
                    )

                elif existing_table := existing_tables_by_name.get(
                    (table.database, table.name)
                ):
                    if compare_schema(
                        existing_table.create_statement, table.create_statement
                    ):
                        logging.debug(
                            f"Table {table_name_for_logs} already exists and its schema is the same as the schema from backup. Skipping it.",
                        )
                        continue
                    logging.warning(
                        'Table {} will be recreated as its schema mismatches the schema from backup: "{}" != "{}"',
                        table_name_for_logs,
                        existing_table.create_statement,
                        table.create_statement,
                    )
                elif existing_table := (
                    existing_tables_by_uuid.get(table.uuid) if table.uuid else None
                ):
                    # Schemas mismatch here at least in name
                    logging.warning(
                        'Table "{}"."{}" will be dropped as its UUID is equal but schema mismatches the schema from backup: "{}" != "{}"',
                        existing_table.database,
                        existing_table.name,
                        existing_table.create_statement,
                        table.create_statement,
                    )

                drop_performed = False

                if (
                    databases[table.database].is_replicated_db_engine()
                    and not restore_tables_in_replicated_database
                ):
                    action = "drop" if existing_table else "restore"
                    logging.info(
                        f"Skipping {action} of table {table_name_for_logs} because it is in replicated database "
                        f"and --restore-tables-in-replicated-database flag is not set",
                    )
                elif not existing_table:
                    result.append(table)
                else:
                    self._drop_existing_table(context, table, existing_table)
                    drop_performed = True
                    result.append(table)

                if (
                    metadata_cleaner
                    and table.is_replicated()
                    and not drop_performed
                    and (table.database, table.name) not in readonly_cleaned_metadata
                ):
                    logging.debug(
                        f"Will clean ZooKeeper metadata for table {table_name_for_logs}"
                    )
                    tables_to_clean_metadata.append(table)
            except (ClickhouseBackupError, ClickhouseError, OSError, ValueError) as e:
                if not keep_going:
                    raise
                preprocessing_failed_skipped += 1
                preprocessing_failed_tables.add((table.database, table.name))
                logging.exception(
                    f"Preprocessing of table {table.database}.{table.name} failed, skipping due to --keep-going flag. Reason: {e}"
                )
                continue

        if metadata_cleaner and tables_to_clean_metadata:
            metadata_cleaner.clean_tables_metadata(tables_to_clean_metadata)

        return result, preprocessing_failed_skipped, preprocessing_failed_tables

    def _get_remaining_readonly_tables_after_fix_attempt(
        self,
        context: BackupContext,
        tables: list[Table],
        metadata_cleaner: MetadataCleaner | None,
    ) -> tuple[set[tuple[str, str]], set[tuple[str, str]]]:
        """
        Find replicated tables that are in readonly state and attempt to fix them in-place by:
        1. Cleaning ZooKeeper metadata via MetadataCleaner
        2. Downloading parts of other replicas from the backup given by --parts-from-backup
        3. Running SYSTEM RESTORE REPLICA

        Returns a tuple of:
        - set of (database, table_name) tuples for tables that are still readonly
          after the fix attempt (or all readonly tables if metadata_cleaner is not provided).
        - set of (database, table_name) tuples for tables whose metadata was already cleaned
          during the fix attempt (to avoid redundant re-cleaning later).
        """
        existing_readonly_tables = {
            (replica["database"], replica["table"])
            for replica in context.ch_ctl.get_replicas(readonly=True)
        }

        if not metadata_cleaner or not existing_readonly_tables:
            return existing_readonly_tables, set()

        replicated_readonly_tables = [
            table
            for table in tables
            if table.is_replicated()
            and (table.database, table.name) in existing_readonly_tables
        ]

        if not replicated_readonly_tables:
            return existing_readonly_tables, set()

        logging.info(
            f"Attempting to fix {len(replicated_readonly_tables)} readonly table(s) via metadata cleanup + SYSTEM RESTORE REPLICA"
        )

        metadata_cleaner.clean_tables_metadata(replicated_readonly_tables)

        for table in replicated_readonly_tables:
            try:
                self._download_parts_of_other_replicas(context, table)
                logging.debug(
                    f'Running SYSTEM RESTORE REPLICA for "{table.database}"."{table.name}"'
                )
                context.ch_ctl.restore_replica(table)
            except Exception as e:
                logging.warning(
                    f'SYSTEM RESTORE REPLICA failed for "{table.database}"."{table.name}", will attempt to recreate: {repr(e)}'
                )

        still_readonly = {
            (replica["database"], replica["table"])
            for replica in context.ch_ctl.get_replicas(readonly=True)
        }

        already_cleaned: set[tuple[str, str]] = {
            (table.database, table.name) for table in replicated_readonly_tables
        }
        for table in replicated_readonly_tables:
            key = (table.database, table.name)
            if key not in still_readonly:
                logging.info(
                    f'Table "{table.database}"."{table.name}" successfully fixed from readonly state'
                )
                existing_readonly_tables.discard(key)
            else:
                logging.warning(
                    f'Table "{table.database}"."{table.name}" is still readonly after fix attempt'
                )

        return existing_readonly_tables, already_cleaned

    @staticmethod
    def _drop_existing_table(
        context: BackupContext,
        table: Table,
        existing_table: Table,
    ) -> None:
        """
        Drop an existing table, handling long table names and dictionaries.
        """
        ### If table name longer than `getMaxTableNameLengthForDatabase()` ch function result
        ### then we can't drop the table. Just rename it to some random name and drop the table.
        ### But for dictionaries doesn't works create/attach with long name, so we can't even restore the dictionary
        ### with long name. So it better to keep it.
        if existing_table.is_dictionary():
            context.ch_ctl.drop_dictionary_if_exists(existing_table)
        else:
            ### The lightweight copy to that we can modify and use for ch queries
            table_to_drop = Table.make_dummy(
                existing_table.database,
                existing_table.name,
            )
            if len(table.name) > context.config_root["restore"]["max_table_name"]:
                new_table_name = "to_drop_" + "".join(
                    choices(ascii_lowercase, k=RANDOM_TABLE_NAME_LENGTH)
                )
                table_to_drop.name = new_table_name
                context.ch_ctl.drop_table_if_exists(table_to_drop)
                context.ch_ctl.rename_table(existing_table, table_to_drop.name)

            context.ch_ctl.drop_table_if_exists(table_to_drop)

    @staticmethod
    def _get_tables_from_meta(
        context: BackupContext, tables_meta: list[TableMetadata], keep_going: bool
    ) -> tuple[list[Table], int]:
        databases_to_download: set[str] = set()
        tables: dict[tuple[str, str], Table] = {}
        for table_meta in tables_meta:
            databases_to_download.add(table_meta.database)
            tables[(table_meta.database, table_meta.name)] = Table(
                database=table_meta.database,
                name=table_meta.name,
                engine=table_meta.engine,
                # TODO: set disks and data_paths
                disks=[],
                data_paths=[],
                metadata_path="",
                uuid=table_meta.uuid,
                create_statement="",
            )

        for database in databases_to_download:
            logging.debug(f"Downloading create statements for database {database}")
            try:
                statements = context.backup_layout.get_table_create_statements(
                    context.backup_meta,
                    database,
                    [table.name for table in tables_meta if table.database == database],
                    keep_going=keep_going,
                )
            except Exception as error:
                if not keep_going:
                    raise
                logging.warning(
                    "Failed to load table schemas for database {}: {}. "
                    "Skipping schema and data restore for its selected tables due to --keep-going flag",
                    database,
                    repr(error),
                )
                continue
            for table_name, create_statement in statements:
                if (database, table_name) in tables:
                    tables[(database, table_name)].create_statement = create_statement

        result = []
        missing_create_statement_skipped = 0
        for table in tables.values():
            if table.create_statement:
                result.append(table)
                continue

            if not keep_going:
                raise ClickhouseBackupError(
                    f"Create statement for table {table.database}.{table.name} is missing in backup layout"
                )

            missing_create_statement_skipped += 1
            logging.warning(
                "Create statement for table {}.{} is unavailable, skipping schema and data restore due to --keep-going flag",
                table.database,
                table.name,
            )

        return result, missing_create_statement_skipped

    @staticmethod
    def _filter_duplicate_uuid(
        tables_meta: list[TableMetadata], keep_going: bool
    ) -> tuple[list[TableMetadata], int]:
        tables_by_uuid: dict[str, TableMetadata] = {}
        duplicate_uuid_skipped = 0
        filtered_tables_meta: list[TableMetadata] = []
        for table_meta in tables_meta:
            if not table_meta.uuid:
                filtered_tables_meta.append(table_meta)
                continue

            kept_table_meta = tables_by_uuid.get(table_meta.uuid)
            if not kept_table_meta:
                tables_by_uuid[table_meta.uuid] = table_meta
                filtered_tables_meta.append(table_meta)
                continue

            duplicate_uuid_skipped += 1
            if (
                kept_table_meta.database == table_meta.database
                and kept_table_meta.name == table_meta.name
            ):
                logging.warning(
                    "Skipping duplicate metadata entry for table {}.{} with UUID {}",
                    table_meta.database,
                    table_meta.name,
                    table_meta.uuid,
                )
                continue

            if not keep_going:
                raise ClickhouseBackupError(
                    "Backup metadata contains conflicting tables with same UUID "
                    f"{table_meta.uuid}: {kept_table_meta.database}.{kept_table_meta.name} "
                    f"and {table_meta.database}.{table_meta.name}"
                )

            logging.warning(
                "Skipping table {}.{} from backup restore: UUID {} duplicates {}.{} in backup metadata and --keep-going is enabled",
                table_meta.database,
                table_meta.name,
                table_meta.uuid,
                kept_table_meta.database,
                kept_table_meta.name,
            )
        return filtered_tables_meta, duplicate_uuid_skipped

    @staticmethod
    def _log_restore_preprocess_summary(
        duplicate_uuid_skipped: int,
        missing_create_statement_skipped: int,
        preprocessing_failed_skipped: int,
    ) -> None:
        skipped_total = (
            duplicate_uuid_skipped
            + missing_create_statement_skipped
            + preprocessing_failed_skipped
        )
        if skipped_total == 0:
            return

        logging.info(
            "Restore preprocessing skipped {} table(s): {} duplicate UUID, {} missing create statement, {} preprocessing failures",
            skipped_total,
            duplicate_uuid_skipped,
            missing_create_statement_skipped,
            preprocessing_failed_skipped,
        )

    # pylint: disable=too-many-positional-arguments
    def _restore_tables(
        self,
        context: BackupContext,
        databases: dict[str, Database],
        tables: Iterable[Table],
        keep_going: bool = False,
    ) -> list[Table]:
        logging.info("Preparing tables for restoring")

        merge_tree_tables = []
        distributed_tables = []
        view_tables = []
        other_tables = []

        for table in tables:
            logging.debug(
                "Preparing table {} for restoring", f"{table.database}.{table.name}"
            )
            if table.is_merge_tree():
                merge_tree_tables.append(table)
            elif table.is_distributed():
                distributed_tables.append(table)
            elif table.is_view():
                view_tables.append(table)
            else:
                other_tables.append(table)

        return self._restore_table_objects(
            context,
            databases,
            chain(merge_tree_tables, other_tables, distributed_tables, view_tables),
            keep_going,
        )

    @staticmethod
    def _restore_data(
        context: BackupContext,
        tables: Iterable[TableMetadata],
        disks: ClickHouseTemporaryDisks,
        skip_cloud_storage: bool,
        keep_going: bool,
        storage_disks: dict[tuple[str, str], tuple[str, ...]] | None = None,
    ) -> None:
        # pylint: disable=too-many-branches
        logging.info("Restoring tables data")
        for table_meta in tables:
            cloud_storage_parts = []
            try:
                maybe_table_short = context.ch_ctl.get_table(
                    table_meta.database, table_meta.name, short_query=True
                )
                if not maybe_table_short:
                    if keep_going:
                        logging.warning(
                            f"Haven't found table {table_meta.database}.{table_meta.name} keep going"
                        )
                        continue
                    raise ClickhouseBackupError(
                        f"Table not found {table_meta.database}.{table_meta.name}"
                    )

                # We have to check table engine on short Table version
                # because some of columns might be inaccessbible, for old ch versions.
                # Fix https://github.com/ClickHouse/ClickHouse/pull/55540 is pesented since 23.8.
                if not maybe_table_short.is_merge_tree():
                    logging.debug(
                        'Skip table "{}.{}" data restore, because it is not MergeTree family.',
                        table_meta.database,
                        table_meta.name,
                    )
                    continue

                if table_meta.data_skipped_reason:
                    logging.warning(
                        'Table "{}.{}" was backed up without data (reason: {}). '
                        "Restoring schema only",
                        table_meta.database,
                        table_meta.name,
                        table_meta.data_skipped_reason,
                    )
                    continue

                logging.debug(
                    'Running table "{}.{}" data restore',
                    table_meta.database,
                    table_meta.name,
                )

                table: Table = context.ch_ctl.get_table(
                    table_meta.database, table_meta.name
                )  # type: ignore
                attach_parts = []
                for part in table_meta.get_parts():
                    if context.restore_context.part_restored(part):
                        if part.disk_name in context.backup_meta.cloud_storage.disks:
                            # Retry marker cleanup if the previous invocation stopped
                            # after persisting RESTORED but before removing the marker.
                            disks.remove_part_copy_marker(table, part)
                        logging.debug(
                            f"{table.database}.{table.name} part {part.name} already restored, skipping it"
                        )
                        continue

                    if context.restore_context.part_downloaded(part):
                        logging.debug(
                            f"{table.database}.{table.name} part {part.name} already downloading, only attach it"
                        )
                        attach_parts.append(part)
                        continue

                    try:
                        if part.disk_name in context.backup_meta.cloud_storage.disks:
                            if skip_cloud_storage:
                                logging.debug(
                                    f"Skipping restoring of {table.database}.{table.name} part {part.name} "
                                    "on cloud storage because of --skip-cloud-storage flag"
                                )
                                continue
                            cloud_storage_parts.append((table, part))
                        else:
                            # Keep the backup part's identity for download; only
                            # its local destination changes with the target policy.
                            fs_part_path = context.ch_ctl.get_detached_part_path(
                                table,
                                restore_disk_name(table, part.disk_name, storage_disks),
                                part.name,
                            )
                            context.backup_layout.download_data_part(
                                context.backup_meta,
                                part,
                                fs_part_path,
                                callback=partial(
                                    context.restore_context.change_part_state,
                                    PartState.DOWNLOADED,
                                ),
                            )

                        attach_parts.append(part)
                    except Exception:
                        if keep_going:
                            logging.exception(
                                f"Restore of part {part.name} failed, skipping due to --keep-going flag"
                            )
                        else:
                            raise

                disks.copy_parts(
                    context.backup_meta,
                    cloud_storage_parts,
                    context.config_root["multiprocessing"][
                        "cloud_storage_restore_workers"
                    ],
                    keep_going,
                    part_callback=partial(
                        context.restore_context.change_part_state, PartState.DOWNLOADED
                    ),
                )

                context.backup_layout.wait(keep_going)
                # Setting state with callback is not possible if part is not stored
                # as a single file, because there are multiple async download tasks
                # per part. Currently all uploading parts are stored as tarball,
                # this is done only for backward compatibility.
                # TODO: It can probably be removed already.
                for part in attach_parts:
                    if not part.tarball:
                        context.restore_context.change_part_state(
                            PartState.DOWNLOADED, part
                        )

                context.ch_ctl.chown_detached_table_parts(
                    table, context.restore_context
                )
                for part in attach_parts:
                    logging.debug(
                        'Attaching "{}.{}" part: {}',
                        table.database,
                        table.name,
                        part.name,
                    )
                    try:
                        context.ch_ctl.attach_part(table, part.name)
                    except Exception as e:
                        logging.warning(
                            'Attaching "{}.{}" part {} failed: {}',
                            table.database,
                            table.name,
                            part.name,
                            repr(e),
                        )
                        context.restore_context.add_failed_part(part, e)
                        # if part failed to attach due to corrupted data during download
                        context.restore_context.change_part_state(
                            PartState.INVALID, part
                        )
                    else:
                        context.restore_context.change_part_state(
                            PartState.RESTORED, part
                        )
                        if part.disk_name in context.backup_meta.cloud_storage.disks:
                            # A marker cleanup failure must not mark an attached part
                            # INVALID and cause it to be copied again.
                            disks.remove_part_copy_marker(table, part)
            finally:
                context.restore_context.dump_state()

        logging.info("Restoring tables data completed")

    @staticmethod
    def _restore_detached_data(
        context: BackupContext,
        tables: Iterable[TableMetadata],
        skip_cloud_storage: bool,
        keep_going: bool,
        storage_disks: dict[tuple[str, str], tuple[str, ...]] | None = None,
    ) -> None:
        """Restore backed-up detached parts without attaching them.

        Both local parts and object-storage metadata files are downloaded into
        a temporary directory and promoted only after a complete download.
        Referenced object-storage blobs are neither copied nor rewritten.
        """
        logging.info("Restoring detached table parts")
        for table_meta in tables:
            detached_parts = table_meta.get_detached_parts()
            if not detached_parts:
                continue

            table = context.ch_ctl.get_table(table_meta.database, table_meta.name)
            if table is None:
                error = ClickhouseBackupError(
                    f"Table not found {table_meta.database}.{table_meta.name}"
                )
                if keep_going:
                    logging.warning(str(error))
                    continue
                raise error

            scheduled_paths: list[str] = []
            downloaded_parts: list[tuple[DetachedPartMetadata, str, str, str]] = []
            disks_to_chown: set[str] = set()
            for part in detached_parts:
                try:
                    if part.object_storage and skip_cloud_storage:
                        logging.debug(
                            "Skipping detached part {}.{} {} on cloud disk {} because of --skip-cloud-storage flag",
                            part.database,
                            part.table,
                            part.name,
                            part.disk_name,
                        )
                        continue

                    # Apply the same local disk mapping as for active parts.
                    # Object-storage metadata remains tied to its original disk.
                    target_disk = (
                        part.disk_name
                        if part.object_storage
                        else restore_disk_name(table, part.disk_name, storage_disks)
                    )
                    destination = TableBackup._find_detached_restore_destination(
                        context, table, part, target_disk
                    )
                    if destination is None:
                        disks_to_chown.add(target_disk)
                        continue
                    restore_part, destination_path = destination

                    staging_path = TableBackup._detached_staging_path(destination_path)
                    scheduled_paths.append(staging_path)
                    # Carry the target disk through the asynchronous download so
                    # ownership is fixed where the part was actually restored.
                    callback = partial(
                        downloaded_parts.append,
                        (restore_part, staging_path, destination_path, target_disk),
                    )
                    # The archive is stored under its original backup name. The
                    # possibly suffixed name applies only to the restore target.
                    context.backup_layout.download_detached_part(
                        context.backup_meta,
                        part,
                        staging_path,
                        callback=callback,
                    )
                except Exception:
                    if keep_going:
                        logging.exception(
                            f"Restore of detached part {part.name} failed, skipping due to --keep-going flag"
                        )
                    else:
                        try:
                            context.backup_layout.wait(keep_going=True)
                        finally:
                            TableBackup._cleanup_detached_staging_paths(scheduled_paths)
                        raise

            try:
                context.backup_layout.wait(keep_going)

                for part, staging_path, target_path, target_disk in downloaded_parts:
                    try:
                        TableBackup._promote_detached_part(
                            context, part, staging_path, target_path
                        )
                        disks_to_chown.add(target_disk)
                    except Exception:
                        if keep_going:
                            logging.exception(
                                f"Restore of detached part {part.name} failed, skipping due to --keep-going flag"
                            )
                        else:
                            raise
            finally:
                TableBackup._cleanup_detached_staging_paths(scheduled_paths)

            if disks_to_chown:
                context.ch_ctl.chown_detached_table_parts(
                    table,
                    context.restore_context,
                    disk_names=sorted(disks_to_chown),
                )

        logging.info("Restoring detached table parts completed")

    @staticmethod
    def _find_detached_restore_destination(
        context: BackupContext,
        table: Table,
        part: DetachedPartMetadata,
        target_disk: str | None = None,
    ) -> tuple[DetachedPartMetadata, str] | None:
        """Find a free detached path, adding ClickHouse-style ``_tryN`` suffixes.

        An existing matching directory makes restore idempotent. An existing
        directory with different contents is preserved and the first free
        ``_tryN`` name is used instead.
        """
        if (
            not part.name
            or part.name in {".", ".."}
            or os.path.basename(part.name) != part.name
        ):
            raise ClickhouseBackupError(
                f'Invalid detached part directory name: "{part.name}"'
            )

        for try_no in count():
            restore_part = (
                part if try_no == 0 else part.with_name(f"{part.name}_try{try_no}")
            )
            target_path = context.ch_ctl.get_detached_part_path(
                table, target_disk or restore_part.disk_name, restore_part.name
            )
            if os.path.exists(target_path):
                if context.ch_ctl.detached_part_matches(part, target_path):
                    logging.debug(
                        "Detached part {}.{} {} on disk {} is already restored as {}",
                        part.database,
                        part.table,
                        part.name,
                        part.disk_name,
                        restore_part.name,
                    )
                    return None
                logging.warning(
                    'Detached part path "{}" already exists with different contents; trying a _tryN suffix',
                    target_path,
                )
                continue

            if try_no:
                logging.warning(
                    "Restoring detached part {}.{} {} on disk {} as {} because the original path is occupied",
                    part.database,
                    part.table,
                    part.name,
                    part.disk_name,
                    restore_part.name,
                )
            return restore_part, target_path

        raise AssertionError("unreachable")

    @staticmethod
    def _detached_staging_path(destination_path: str) -> str:
        """Return a unique same-filesystem staging path for atomic promotion."""
        return os.path.join(
            os.path.dirname(destination_path), f".ch-backup-{uuid4().hex}"
        )

    @staticmethod
    def _cleanup_detached_staging_paths(staging_paths: Iterable[str]) -> None:
        """Remove private staging directories left by failed downloads."""
        for staging_path in staging_paths:
            if os.path.exists(staging_path):
                shutil.rmtree(staging_path)

    @staticmethod
    def _promote_detached_part(
        context: BackupContext,
        part: DetachedPartMetadata,
        staging_path: str,
        destination_path: str,
    ) -> None:
        """Validate and atomically promote a completely downloaded part."""
        if not context.ch_ctl.detached_part_matches(part, staging_path):
            raise ClickhouseBackupError(
                f'Downloaded detached part does not match backup metadata: "{staging_path}"'
            )
        if os.path.exists(destination_path):
            raise ClickhouseBackupError(
                f'Detached part path appeared during restore: "{destination_path}"'
            )
        os.rename(staging_path, destination_path)

    def _rewrite_table_schema(
        self,
        context: BackupContext,
        db: Database,
        table: Table,
        add_uuid_if_required: bool = False,
    ) -> None:
        add_uuid = False
        inner_uuid = None
        if add_uuid_if_required and table.uuid and db.is_atomic():
            add_uuid = True
            # Starting with 21.4 it's required to explicitly set inner table UUID for materialized views.
            if table.is_materialized_view() and context.ch_ctl.ch_version_ge("21.4"):
                inner_table = context.ch_ctl.get_table(
                    table.database, f".inner_id.{table.uuid}"
                )
                if inner_table:
                    inner_uuid = inner_table.uuid

        rewrite_table_schema(
            table,
            force_non_replicated_engine=context.config["force_non_replicated"],
            override_replica_name=context.config["override_replica_name"],
            add_uuid=add_uuid,
            inner_uuid=inner_uuid,
        )

    def _restore_table_objects(
        self,
        context: BackupContext,
        databases: dict[str, Database],
        tables: Iterable[Table],
        keep_going: bool = False,
    ) -> list[Table]:
        logging.info("Restoring tables")
        errors: list[tuple[Table, Exception]] = []
        unprocessed = deque(table for table in tables)
        while unprocessed:
            table = unprocessed.popleft()
            try:
                logging.debug(
                    "Trying to restore table object for table {}",
                    f"{table.database}.{table.name}",
                )
                self._restore_table_object(context, databases[table.database], table)
            except Exception as e:
                errors.append((table, e))
                unprocessed.append(table)
                logging.debug(f"Errors {len(errors)}, unprocessed {len(unprocessed)}")
                if len(errors) > len(unprocessed):
                    logging.error(
                        f'Failed to restore "{table.database}"."{table.name}" with "{repr(e)}",'
                        " no retries anymore"
                    )
                    break
                logging.warning(
                    f'Failed to restore "{table.database}"."{table.name}" with "{repr(e)}",'
                    " will retry after restoring other tables"
                )
            else:
                errors.clear()

        self._check_readonly_restored_tables(context, tables, errors)

        logging.info("Restoring tables completed")

        if errors:
            logging.error(
                "Failed to restore tables:\n{}",
                "\n".join(f'"{v.database}"."{v.name}": {e!r}' for v, e in errors),
            )

            if keep_going:
                return list(set(table for table, _ in errors))

            failed_tables = sorted(
                list(
                    set(
                        f"`{t.database}`.`{t.name}`"
                        for t in set(table for table, _ in errors)
                    )
                )
            )
            raise ClickhouseBackupError(
                f'Failed to restore tables: {", ".join(failed_tables)}'
            )

        return []

    @staticmethod
    def _check_readonly_restored_tables(
        context: BackupContext,
        tables: Iterable[Table],
        errors: list[tuple[Table, Exception]],
    ) -> None:
        """
        Successful RESTORE REPLICA doesn't mean that replica becomes active.
        Need to check if table is still readonly.
        """
        readonly_tables = {
            (replica["database"], replica["table"])
            for replica in context.ch_ctl.get_replicas(readonly=True)
        }
        has_errors = {(table.database, table.name) for table, _ in errors}
        for table in tables:
            if (table.database, table.name) not in has_errors and (
                table.database,
                table.name,
            ) in readonly_tables:
                errors.append(
                    (
                        table,
                        Exception("Table is readonly after successful RESTORE REPLICA"),
                    )
                )

    @staticmethod
    def _restore_table_object(
        context: BackupContext,
        db: Database,
        table: Table,
    ) -> None:
        try:
            use_create_method_for_merge_tree = (
                db.is_replicated_db_engine() and context.ch_ctl.ch_version_ge("24.9")
            )

            if (
                (table.is_merge_tree() and not use_create_method_for_merge_tree)
                or table.is_materialized_view()
                or table.is_external_engine()
                or table.is_distributed()
            ):
                logging.debug(
                    f"Trying to restore table `{db.name}`.`{table.name}` by ATTACH method"
                )
                table.create_statement = to_attach_query(table.create_statement)
                context.ch_ctl.create_table(table)
                if table.is_replicated():
                    TableBackup._download_parts_of_other_replicas(context, table)
                    return context.ch_ctl.restore_replica(table)
            else:
                logging.debug(
                    f"Trying to restore table `{db.name}`.`{table.name}` by CREATE method"
                )
                table.create_statement = to_create_query(table.create_statement)
                context.ch_ctl.create_table(table)
        except Exception as e:
            logging.debug(
                f"Failed to restore table `{db.name}`.`{table.name}`. Removing it. Exception: {e}"
            )
            if table.is_dictionary():
                context.ch_ctl.drop_dictionary_if_exists(table)
            else:
                context.ch_ctl.drop_table_if_exists(table)
            raise ClickhouseBackupError(
                f"Failed to restore table: {table.database}.{table.name}"
            )

    @staticmethod
    def _download_parts_of_other_replicas(context: BackupContext, table: Table) -> None:
        """
        Download to detached/ the backup parts that every other replica has as active.

        SYSTEM RESTORE REPLICA takes such a part by checksum instead of fetching it, but
        does not hash its files, so they are checked here. Parts active in the table or
        already in detached/ are skipped. Any failure leaves the parts to replicas.
        """
        backup_meta = context.parts_backup_meta
        if not backup_meta:
            return
        try:
            table_meta = backup_meta.get_table(table.database, table.name)
        except KeyError:
            return

        try:
            ch_table: Table = context.ch_ctl.get_table(table.database, table.name)  # type: ignore
            local_disks = {
                disk.name
                for _, disk in ch_table.paths_with_disks
                if disk.type == "local"
            }
            replica_parts = context.ch_ctl.get_parts_of_other_replicas(
                table, context.zk_ctl
            )
            parts = [
                part
                for part in table_meta.get_parts(
                    excluded_parts=context.ch_ctl.get_active_parts(table)
                )
                if part.name in replica_parts and part.disk_name in local_disks
            ]
            part_paths = {
                part.name: context.ch_ctl.get_detached_part_path(
                    ch_table, part.disk_name, part.name
                )
                for part in parts
            }
            parts = [
                part for part in parts if not os.path.exists(part_paths[part.name])
            ]
        except Exception:
            logging.warning(
                'Failed to choose parts of "{}"."{}" to download, they are left to replicas',
                table.database,
                table.name,
                exc_info=True,
            )
            return
        if not parts:
            return
        logging.info(
            'Downloading {} parts of "{}"."{}" from backup {}',
            len(parts),
            table.database,
            table.name,
            backup_meta.name,
        )
        for part in parts:
            try:
                context.backup_layout.download_data_part(
                    backup_meta,
                    part,
                    part_paths[part.name],
                    callback=lambda _: None,
                )
            except Exception:
                logging.warning(
                    'Failed to download part {} of "{}"."{}", it is left to replicas',
                    part.name,
                    table.database,
                    table.name,
                    exc_info=True,
                )
        context.backup_layout.wait(keep_going=True)
        for part in parts:
            part_path = part_paths[part.name]
            if not os.path.exists(part_path):
                continue
            try:
                check_part_files(part_path)
            except Exception:
                logging.warning(
                    'Part {} of "{}"."{}" does not match its checksums, it is left to replicas',
                    part.name,
                    table.database,
                    table.name,
                    exc_info=True,
                )
                shutil.rmtree(part_path)
        context.ch_ctl.chown_detached_table_parts(ch_table, context.restore_context)
