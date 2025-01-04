"""
Clickhouse backup logic for tables
"""

import os
from collections import deque
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from functools import partial
from itertools import chain
from pathlib import Path
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
from ch_backup.util import compare_schema


@dataclass
class TableMetadataMtime:
    """
    Class contains timestamp of table metadata last modification.
    """

    metadata_path: str
    mtime: float


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
        freeze_threads: int,
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

        for db in databases:
            self._backup(
                context,
                db,
                db_tables[db.name],
                backup_name,
                schema_only,
                freeze_threads,
            )

    def _collect_local_metadata_mtime(
        self, context: BackupContext, db: Database, tables: Sequence[str]
    ) -> Dict[str, TableMetadataMtime]:
        """
        Collect modification timestamps of table metadata files.
        """
        logging.debug("Collecting local metadata modification times")
        res = {}

        for table in context.ch_ctl.get_tables(db.name, tables):
            mtime = self._get_mtime(table.metadata_path)
            if mtime is None:
                logging.warning(
                    'Cannot get metadata mtime for table "{}"."{}". Skipping it',
                    table.database,
                    table.name,
                )
                continue

            res[table.name] = TableMetadataMtime(
                metadata_path=table.metadata_path, mtime=mtime
            )

        return res

    # pylint: disable=too-many-positional-arguments
    def _backup(
        self,
        context: BackupContext,
        db: Database,
        tables: Sequence[str],
        backup_name: str,
        schema_only: bool,
        freeze_threads: int,
    ) -> None:
        """
        Backup single database tables.
        """
        if not db.is_external_db_engine():
            # Collect modification timestamps of table metadata files for optimistic concurrency
            # control of backup creation.
            # To ensure consistency between metadata and data backups.
            # See https://en.wikipedia.org/wiki/Optimistic_concurrency_control
            mtimes = self._collect_local_metadata_mtime(context, db, tables)
            tables_ = list(
                filter(
                    lambda table: table.name in mtimes,
                    context.ch_ctl.get_tables(db.name, tables),
                )
            )

            # Create shadow/increment.txt if not exists manually to avoid
            # race condition with parallel freeze
            context.ch_ctl.create_shadow_increment()
            futures: List[Future] = []
            with ThreadPoolExecutor(max_workers=freeze_threads) as pool:
                for table in tables_:
                    future = pool.submit(
                        TableBackup._freeze_table,
                        context,
                        db,
                        table,
                        backup_name,
                        schema_only,
                    )
                    futures.append(future)

                for future in as_completed(futures):
                    table_and_create_statement = future.result()
                    if table_and_create_statement is not None:
                        table, create_statement = table_and_create_statement
                        self._backup_freezed_table(
                            context,
                            db,
                            table,
                            backup_name,
                            schema_only,
                            mtimes,
                            create_statement,
                        )
                        self._backup_cloud_storage_metadata(context, table)

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
    ) -> Optional[Tuple[Table, bytes]]:
        """
        Freeze table and return it's create statement
        """
        logging.debug('Trying to freeze "{}"."{}"', table.database, table.name)

        create_statement = TableBackup._load_create_statement_from_disk(table)
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
                context.ch_ctl.freeze_table(backup_name, table)
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

        return (table, create_statement)

    @staticmethod
    def _load_create_statement_from_disk(table: Table) -> Optional[bytes]:
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
            return Path(table.metadata_path).read_bytes()
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
        exclude_tables: List[TableMetadata],
        metadata_cleaner: Optional[MetadataCleaner],
        cloud_storage_source_bucket: Optional[str],
        cloud_storage_source_path: Optional[str],
        cloud_storage_source_endpoint: Optional[str],
        skip_cloud_storage: bool,
        keep_going: bool,
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

        if exclude_tables:
            logging.debug("Excluding unnecessary tables metadata")
            excluded_tables = [(table.database, table.name) for table in exclude_tables]
            tables_meta = list(
                filter(
                    lambda t: (t.database, t.name) not in excluded_tables, tables_meta
                )
            )

        logging.debug("Retrieving tables from tables metadata")
        tables_to_restore: List[Table] = list(
            map(lambda meta: self._get_table_from_meta(context, meta), tables_meta)
        )
        tables_to_restore = self._preprocess_tables_to_restore(
            context, databases, tables_to_restore
        )

        failed_tables = self._restore_tables(
            context,
            databases,
            tables_to_restore,
            metadata_cleaner,
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
        mtimes: Dict[str, TableMetadataMtime],
        create_statement: bytes,
    ) -> None:
        # Check if table metadata was updated
        new_mtime = self._get_mtime(table.metadata_path)
        if new_mtime is None or mtimes[table.name].mtime != new_mtime:
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
            context.backup_meta, db, table, create_statement
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
    def _get_mtime(file_name: str) -> Optional[float]:
        """
        Fetch last modification time of the file safely.
        """
        try:
            return os.path.getmtime(file_name)
        except OSError as e:
            logging.debug(f"Failed to get mtime of {file_name}: {str(e)}")
            return None

    def _preprocess_tables_to_restore(
        self,
        context: BackupContext,
        databases: Dict[str, Database],
        tables: List[Table],
    ) -> List[Table]:
        # Prepare table schema to restore.
        for table in tables:
            self._rewrite_table_schema(context, databases[table.database], table)

        # Filter out already restored tables.
        existing_tables = {}
        for table in context.ch_ctl.get_tables(short_query=True):
            existing_tables[(table.database, table.name)] = table

        result: List[Table] = []
        for table in tables:
            existing_table = existing_tables.get((table.database, table.name))
            if existing_table:
                if compare_schema(
                    existing_table.create_statement, table.create_statement
                ):
                    continue
                logging.warning(
                    'Table "{}"."{}" will be recreated as its schema mismatches the schema from backup: "{}" != "{}"',
                    table.database,
                    table.name,
                    existing_table.create_statement,
                    table.create_statement,
                )
                if table.is_dictionary():
                    context.ch_ctl.drop_dictionary_if_exists(table)
                else:
                    context.ch_ctl.drop_table_if_exists(table)

            result.append(table)

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
        metadata_cleaner: Optional[MetadataCleaner],
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
            self._rewrite_table_schema(
                context, databases[table.database], table, add_uuid_if_required=True
            )

            if table.is_merge_tree():
                merge_tree_tables.append(table)
            elif table.is_distributed():
                distributed_tables.append(table)
            elif table.is_view():
                view_tables.append(table)
            else:
                other_tables.append(table)

        if metadata_cleaner:  # type: ignore
            replicated_tables = [
                table for table in merge_tree_tables if table.is_replicated()
            ]
            metadata_cleaner.clean_tables_metadata(replicated_tables)

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
                                context.backup_meta, part, fs_part_path
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
                )

                context.backup_layout.wait(keep_going)
                for part in attach_parts:
                    context.restore_context.add_part(part, PartState.DOWNLOADED)

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
                        context.restore_context.add_part(part, PartState.RESTORED)
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
                        context.restore_context.add_part(part, PartState.INVALID)
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
    def _restore_table_object(
        context: BackupContext,
        db: Database,
        table: Table,
    ) -> None:
        try:
            if (
                table.is_merge_tree()
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
