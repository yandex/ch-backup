"""
Clickhouse backup logic for tables
"""

# pylint: disable=too-many-lines

import os
from collections import deque
from dataclasses import dataclass
from functools import partial
from itertools import chain
from pathlib import Path
from random import choices
from string import ascii_lowercase
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from ch_backup import logging
from ch_backup.backup.deduplication import deduplicate_parts
from ch_backup.backup.metadata import PartMetadata, TableMetadata
from ch_backup.backup.restore_context import PartState
from ch_backup.backup_context import BackupContext
from ch_backup.clickhouse.client import ClickhouseError
from ch_backup.clickhouse.disks import ClickHouseTemporaryDisks
from ch_backup.clickhouse.metadata_cleaner import MetadataCleaner
from ch_backup.clickhouse.models import Database, FrozenPart, Table
from ch_backup.clickhouse.schema import (
    rewrite_table_schema,
    to_attach_query,
    to_create_query,
)
from ch_backup.exceptions import ClickhouseBackupError
from ch_backup.logic.backup_manager import BackupManager
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


class TableBackup(BackupManager):
    """
    Table backup class
    """

    # pylint: disable=too-many-positional-arguments
    def backup(
        self,
        context: BackupContext,
        databases: Sequence[Database],
        db_tables: Dict[str, list],
        schema_only: bool,
        multiprocessing_config: Dict,
    ) -> None:
        """
        Backup tables metadata, MergeTree data and Cloud storage metadata.
        """

        backup_name = context.backup_meta.get_sanitized_name()

        if context.cloud_conf.get("cloud_storage", {}).get("encryption", True):
            logging.debug('Cloud Storage "shadow" backup will be encrypted')
            context.backup_meta.cloud_storage.encrypt()
        if context.cloud_conf.get("cloud_storage", {}).get("compression", True):
            logging.debug('Cloud Storage "shadow" backup will be compressed')
            context.backup_meta.cloud_storage.compress()

        # Since https://github.com/ClickHouse/ClickHouse/pull/75016
        if (
            context.ch_ctl.ch_version_ge("25.2")
            and context.config["kill_old_freeze_queries"]
        ):
            context.ch_ctl.kill_old_freeze_queries()

        for db in databases:
            self._backup(
                context,
                db,
                db_tables[db.name],
                backup_name,
                schema_only,
                multiprocessing_config,
            )

    def _collect_local_metadata_change_times(
        self, context: BackupContext, db: Database, tables: Sequence[str]
    ) -> Dict[str, TableMetadataChangeTime]:
        """
        Collect modification timestamps of table metadata files.
        """
        logging.debug("Collecting local metadata modification times")
        res = {}

        for table in context.ch_ctl.get_tables(db.name, tables):
            change_time = self._get_change_time(table.metadata_path)
            if change_time is None:
                logging.warning(
                    'Cannot get metadata change time for table "{}"."{}". Skipping it',
                    table.database,
                    table.name,
                )
                continue

            res[table.name] = change_time

        return res

    # pylint: disable=too-many-positional-arguments
    def _backup(
        self,
        context: BackupContext,
        db: Database,
        tables: Sequence[str],
        backup_name: str,
        schema_only: bool,
        multiprocessing_config: Dict,
    ) -> None:
        """
        Backup single database tables.
        """
        if not db.is_external_db_engine():
            # Collect modification timestamps of table metadata files for optimistic concurrency
            # control of backup creation.
            # To ensure consistency between metadata and data backups.
            # See https://en.wikipedia.org/wiki/Optimistic_concurrency_control
            change_times = self._collect_local_metadata_change_times(
                context, db, tables
            )
            tables_ = list(
                filter(
                    lambda table: table.name in change_times,
                    context.ch_ctl.get_tables(db.name, tables),
                )
            )

            # Create shadow/increment.txt if not exists manually to avoid
            # race condition with parallel freeze
            context.ch_ctl.create_shadow_increment()
            with ThreadExecPool(
                multiprocessing_config.get("freeze_threads", 1)
            ) as pool:
                for table in tables_:
                    pool.submit(
                        f'Freeze table "{table.database}"."{table.name}"',
                        TableBackup._freeze_table,
                        context,
                        db,
                        table,
                        backup_name,
                        schema_only,
                        multiprocessing_config.get("freeze_partition_threads", 0),
                    )

                for freezed_table in pool.as_completed(keep_going=False):
                    if freezed_table is not None:
                        self._backup_freezed_table(
                            context,
                            db,
                            freezed_table,
                            backup_name,
                            schema_only,
                            change_times,
                        )
                        self._backup_cloud_storage_metadata(context, freezed_table)

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
        freeze_partition_threads: int,
    ) -> Optional[Table]:
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

        # Freeze only MergeTree tables
        if not schema_only and table.is_merge_tree():
            try:
                context.ch_ctl.freeze_table(
                    backup_name, table, freeze_partition_threads
                )
            except ClickhouseError:
                if context.ch_ctl.does_table_exist(table.database, table.name):
                    logging.error(
                        'Cannot freeze table "{}"."{}"',
                        table.database,
                        table.name,
                    )
                    raise

                logging.warning(
                    'Table "{}"."{}" was removed by a user during backup',
                    table.database,
                    table.name,
                )
                return None

        return table

    @staticmethod
    def _load_create_statement_from_disk(table: Table) -> Optional[str]:
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
            return Path(table.metadata_path).read_text("utf-8")
        except OSError as e:
            logging.debug(
                'Cannot load a create statement of the table "{}"."{}": {}',
                table.database,
                table.name,
                str(e),
            )
            return None

    @staticmethod
    def _backup_cloud_storage_metadata(context: BackupContext, table: Table) -> None:
        """
        Backup cloud storage metadata files.
        """
        logging.debug(
            'Backing up Cloud Storage disks "shadow" directory of "{}"."{}"',
            table.database,
            table.name,
        )
        for _, disk in table.paths_with_disks:
            if disk.type == "s3" and not disk.cache_path:
                if not context.backup_layout.upload_cloud_storage_metadata(
                    context.backup_meta, disk, table
                ):
                    logging.debug(f'No data frozen on disk "{disk.name}", skipping')
                    continue
                context.backup_meta.cloud_storage.add_disk(disk.name)

    # pylint: disable=too-many-arguments,too-many-locals,too-many-positional-arguments
    def restore(
        self,
        context: BackupContext,
        databases: Dict[str, Database],
        schema_only: bool,
        tables: List[TableMetadata],
        metadata_cleaner: Optional[MetadataCleaner],
        cloud_storage_source_bucket: Optional[str],
        cloud_storage_source_path: Optional[str],
        cloud_storage_source_endpoint: Optional[str],
        skip_cloud_storage: bool,
        keep_going: bool,
        restore_tables_in_replicated_database: bool,
    ) -> None:
        """
        Restore tables and MergeTree data.
        """
        logging.debug("Retrieving tables metadata")
        tables_meta: List[TableMetadata] = list(
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
                logging.critical(
                    "Required tables {} were not found in backup metadata",
                    ", ".join([f"{t.database}.{t.name}" for t in missed_tables]),
                )
                raise ClickhouseBackupError(
                    "Required tables were not found in backup metadata"
                )

            logging.debug("All required tables are present")

            logging.debug("Leaving only required tables metadata")
            required_tables = [(table.database, table.name) for table in tables]
            tables_meta = list(
                filter(lambda t: (t.database, t.name) in required_tables, tables_meta)
            )

        logging.debug("Retrieving tables from tables metadata")
        tables_to_preprocess: List[Table] = list(
            map(lambda meta: self._get_table_from_meta(context, meta), tables_meta)
        )
        tables_to_restore = self._preprocess_tables_to_restore(
            context,
            databases,
            tables_to_preprocess,
            keep_going,
            restore_tables_in_replicated_database,
            metadata_cleaner,
        )

        failed_tables = self._restore_tables(
            context,
            databases,
            tables_to_restore,
            keep_going,
        )

        if schema_only:
            logging.debug(
                "Skipping restoring of table data as --schema-only flag passed"
            )
            return

        failed_tables_names = [f"`{t.database}`.`{t.name}`" for t in failed_tables]
        tables_to_restore_data = filter(
            lambda t: f"`{t.database}`.`{t.name}`" not in failed_tables_names,
            tables_meta,
        )

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
            use_local_copy=use_inplace_cloud_restore,
        ) as disks:
            self._restore_data(
                context,
                tables=tables_to_restore_data,
                disks=disks,
                skip_cloud_storage=skip_cloud_storage,
                keep_going=keep_going,
            )

    # pylint: disable=too-many-positional-arguments
    def _backup_freezed_table(
        self,
        context: BackupContext,
        db: Database,
        table: Table,
        backup_name: str,
        schema_only: bool,
        change_times: Dict[str, TableMetadataChangeTime],
    ) -> None:
        # Check if table metadata was updated
        new_change_time = self._get_change_time(table.metadata_path)
        if new_change_time is None or change_times[table.name] != new_change_time:
            logging.warning(
                'Skipping table backup for "{}"."{}". The metadata file was updated or removed during backup',
                table.database,
                table.name,
            )
            context.ch_ctl.remove_freezed_data(backup_name, table)
            return

        logging.debug(
            'Performing table backup for "{}"."{}"', table.database, table.name
        )
        # Add table metadata to backup metadata
        context.backup_meta.add_table(
            TableMetadata(table.database, table.name, table.engine, table.uuid)
        )
        # Backup table metadata
        context.backup_layout.upload_table_create_statement(
            context.backup_meta, db, table
        )
        # Backup table data
        if not schema_only:
            self._backup_frozen_table_data(context, table, backup_name)

    def _backup_frozen_table_data(
        self,
        context: BackupContext,
        table: Table,
        backup_name: str,
    ) -> None:
        """
        Backup table with data opposed to schema only.
        """

        def deduplicate_parts_in_batch(
            context: BackupContext,
            upload_observer: UploadPartObserver,
            frozen_parts: Dict[str, FrozenPart],
        ) -> None:
            logging.debug(
                "Working on deduplication of {} frozen parts", len(frozen_parts)
            )
            deduplicated_parts = deduplicate_parts(
                context, table.database, table.name, frozen_parts
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
                    context.backup_layout.upload_data_part(
                        context.backup_meta,
                        frozen_parts[part_name],
                        partial(
                            upload_observer,
                            PartMetadata.from_frozen_part(
                                frozen_parts[part_name], context.backup_meta.encrypted
                            ),
                        ),
                    )
            frozen_parts.clear()

        if not table.is_merge_tree():
            logging.info(
                'Skipping table data backup for non MergeTree table "{}"."{}"',
                table.database,
                table.name,
            )
            return

        logging.debug('Uploading table data for "{}"."{}"', table.database, table.name)

        upload_observer = UploadPartObserver(context)

        frozen_parts_batch: Dict[str, FrozenPart] = {}
        dedup_batch_size = context.config["deduplication_batch_size"]
        for data_path, disk in table.paths_with_disks:
            for fpart in context.ch_ctl.scan_frozen_parts(
                table,
                disk,
                data_path,
                backup_name,
            ):
                logging.debug("Working on {}", fpart)
                if disk.type == "s3":
                    context.backup_meta.add_part(
                        PartMetadata.from_frozen_part(
                            fpart, context.backup_meta.encrypted
                        )
                    )
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

        context.backup_layout.wait()

        self._validate_uploaded_parts(context, upload_observer.uploaded_parts)

        context.ch_ctl.remove_freezed_data(backup_name, table)

    @staticmethod
    def _validate_uploaded_parts(context: BackupContext, uploaded_parts: list) -> None:
        if context.config["validate_part_after_upload"]:
            invalid_parts = []

            for part in uploaded_parts:
                if not context.backup_layout.check_data_part(
                    context.backup_meta.path, part
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
    def _get_change_time(file_name: str) -> Optional[TableMetadataChangeTime]:
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

    # pylint: disable=too-many-branches
    def _preprocess_tables_to_restore(
        self,
        context: BackupContext,
        databases: Dict[str, Database],
        tables: List[Table],
        keep_going: bool,
        restore_tables_in_replicated_database: bool,
        metadata_cleaner: Optional[MetadataCleaner],
    ) -> List[Table]:
        # Prepare table schema to restore.

        for table in tables:
            self._rewrite_table_schema(
                context, databases[table.database], table, add_uuid_if_required=True
            )

        detached_tables = {}
        for table in context.ch_ctl.get_detached_tables():
            if table.uuid:
                detached_tables[table.uuid] = table

        for table in tables:
            if detached_table := (
                detached_tables.get(table.uuid) if table.uuid else None
            ):
                context.ch_ctl.attach_table(detached_table)

        # Filter out already restored tables.
        existing_tables_by_name = {}
        existing_tables_by_uuid = {}
        for table in context.ch_ctl.get_tables(short_query=True):
            existing_tables_by_name[(table.database, table.name)] = table
            if table.uuid:
                existing_tables_by_uuid[table.uuid] = table

        existing_readonly_tables = {
            (replica["database"], replica["table"])
            for replica in context.ch_ctl.get_replicas(readonly=True)
        }

        result: List[Table] = []
        tables_to_clean_metadata: List[Table] = []
        for table in tables:
            existing_table: Optional[Table] = None
            if (table.database, table.name) in existing_readonly_tables:
                existing_table = table
                logging.warning(
                    'Table "{}"."{}" will be recreated because it is in readonly state',
                    table.database,
                    table.name,
                )

            elif existing_table := existing_tables_by_name.get(
                (table.database, table.name)
            ):
                if compare_schema(
                    existing_table.create_statement, table.create_statement
                ):
                    # Skip table
                    continue
                logging.warning(
                    'Table "{}"."{}" will be recreated as its schema mismatches the schema from backup: "{}" != "{}"',
                    table.database,
                    table.name,
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

            if existing_table:
                try:
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
                        if (
                            len(table.name)
                            > context.config_root["restore"]["max_table_name"]
                        ):
                            new_table_name = "to_drop_" + "".join(
                                choices(ascii_lowercase, k=RANDOM_TABLE_NAME_LENGTH)
                            )
                            table_to_drop.name = new_table_name
                            context.ch_ctl.drop_table_if_exists(table_to_drop)
                            context.ch_ctl.rename_table(
                                existing_table, table_to_drop.name
                            )

                        context.ch_ctl.drop_table_if_exists(table_to_drop)

                except Exception as e:
                    if not keep_going:
                        raise
                    logging.exception(
                        f"Drop of table {existing_table.name} was failed, skipping due to --keep-going flag. Reason {e}"
                    )
                    continue

            if metadata_cleaner and table.is_replicated():
                tables_to_clean_metadata.append(table)

            if (
                not restore_tables_in_replicated_database
                and databases[table.database].is_replicated_db_engine()
            ):
                logging.info(
                    'Skipping table "{}"."{}" because it is in replicated database and --restore-tables-in-replicated-database flag is not set',
                    table.database,
                    table.name,
                )
            else:
                result.append(table)

        if metadata_cleaner:  # type: ignore
            metadata_cleaner.clean_tables_metadata(tables_to_clean_metadata)

        return result

    @staticmethod
    def _get_table_from_meta(context: BackupContext, meta: TableMetadata) -> Table:
        return Table(
            database=meta.database,
            name=meta.name,
            engine=meta.engine,
            # TODO: set disks and data_paths
            disks=[],
            data_paths=[],
            metadata_path="",
            uuid=meta.uuid,
            create_statement=context.backup_layout.get_table_create_statement(
                context.backup_meta, meta.database, meta.name
            ),
        )

    # pylint: disable=too-many-positional-arguments
    def _restore_tables(
        self,
        context: BackupContext,
        databases: Dict[str, Database],
        tables: Iterable[Table],
        keep_going: bool = False,
    ) -> List[Table]:
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
                            fs_part_path = context.ch_ctl.get_detached_part_path(
                                table, part.disk_name, part.name
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
                        context.restore_context.change_part_state(
                            PartState.RESTORED, part
                        )
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
            finally:
                context.restore_context.dump_state()

        logging.info("Restoring tables data completed")

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
        databases: Dict[str, Database],
        tables: Iterable[Table],
        keep_going: bool = False,
    ) -> List[Table]:
        logging.info("Restoring tables")
        errors: List[Tuple[Table, Exception]] = []
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
        errors: List[Tuple[Table, Exception]],
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
