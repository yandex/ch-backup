"""
Data part deduplication.
"""

from collections import defaultdict
from copy import copy
from datetime import timedelta
from typing import Dict, List, Sequence, Set

from ch_backup import logging
from ch_backup.backup.layout import BackupLayout
from ch_backup.backup.metadata import BackupMetadata, BackupState, PartMetadata
from ch_backup.backup_context import BackupContext
from ch_backup.clickhouse.models import Database, FrozenPart, Table
from ch_backup.util import Slotted, utcnow


# pylint: disable=too-many-instance-attributes
class PartDedupInfo(Slotted):
    """
    Information about data part to use for deduplication / creation incremental backups.
    """

    __slots__ = (
        "database",
        "table",
        "name",
        "backup_path",
        "checksum",
        "size",
        "files",
        "tarball",
        "disk_name",
        "verified",
        "encrypted",
    )

    # pylint: disable=too-many-arguments,too-many-positional-arguments
    def __init__(
        self,
        database: str,
        table: str,
        name: str,
        backup_path: str,
        checksum: str,
        size: int,
        files: Sequence[str],
        tarball: bool,
        disk_name: str,
        verified: bool,
        encrypted: bool,
    ) -> None:
        self.database = database
        self.table = table
        self.name = name
        self.backup_path = backup_path
        self.checksum = checksum
        self.size = size
        self.files = files
        self.tarball = tarball
        self.disk_name = disk_name
        self.verified = verified
        self.encrypted = encrypted

    def to_sql(self):
        """
        Convert to string to use it in insert query
        """
        files_array = "[" + ",".join(f"'{file}'" for file in self.files) + "]"
        return f"('{self.database}','{self.table}','{self.name}','{self.backup_path}','{self.checksum}',{self.size},{files_array},{int(self.tarball)},'{self.disk_name}',{int(self.verified)}, {int(self.encrypted)})"


TableDedupReferences = Set[str]

DatabaseDedupReferences = Dict[str, TableDedupReferences]

DedupReferences = Dict[str, DatabaseDedupReferences]


def _create_empty_dedup_references() -> DedupReferences:
    """
    Create empty dedup references
    """
    return defaultdict(lambda: defaultdict(set))


def collect_dedup_info(
    context: BackupContext,
    databases: Sequence[Database],
    backups_with_light_meta: List[BackupMetadata],
) -> None:
    """
    Collect deduplication information for creating incremental backups.
    """
    # Do not populate DedupInfo if we are creating schema-only backup.
    if context.backup_meta.schema_only:
        return

    context.ch_ctl.create_deduplication_table()

    backup_age_limit = None
    if context.config.get("deduplicate_parts"):
        backup_age_limit = utcnow() - timedelta(
            **context.config["deduplication_age_limit"]
        )

    # Determine backups that can be used for deduplication.
    dedup_backups = []
    for backup in backups_with_light_meta:
        if not backup_age_limit or backup.start_time < backup_age_limit:
            break

        if backup.schema_only:
            continue

        dedup_backups.append(backup)

    _populate_dedup_info(
        context,
        dedup_backups,
        databases,
    )


class _DatabaseToHandle:
    def __init__(self, name, replicated_tables=False, nonreplicated_tables=False):
        self.name = name
        self.replicated_tables_handled = replicated_tables
        self.nonreplicated_tables_handled = nonreplicated_tables

    @property
    def handled(self):
        """
        Return True if both replicated and non-replicated tables are handled.
        """
        return self.replicated_tables_handled and self.nonreplicated_tables_handled


def _populate_dedup_info(
    context: BackupContext,
    dedup_backups_with_light_meta: List[BackupMetadata],
    databases: Sequence[Database],
) -> None:
    # pylint: disable=too-many-locals,too-many-branches
    layout = context.backup_layout
    # Used to check if part is already collected for deduplication
    dedup_info = _create_empty_dedup_references()
    dedup_batch_size = context.config["deduplication_batch_size"]

    databases_to_handle = {db.name: _DatabaseToHandle(db.name) for db in databases}
    dedup_backup_paths = set(backup.path for backup in dedup_backups_with_light_meta)
    for backup in dedup_backups_with_light_meta:
        backup = layout.reload_backup(backup, use_light_meta=False)

        # Process only replicated tables if backup is created on replica.
        only_replicated = context.backup_meta.hostname != backup.hostname

        databases_to_iterate = []
        for db_name in backup.get_databases():
            db = databases_to_handle.get(db_name)
            if not db:
                continue

            databases_to_iterate.append(copy(db))

            if backup.state == BackupState.CREATED:
                db.replicated_tables_handled = True
                if not only_replicated:
                    db.nonreplicated_tables_handled = True

                if db.handled:
                    del databases_to_handle[db_name]

        dedup_info_batch = []
        for db in databases_to_iterate:
            db_dedup_info = dedup_info[db.name]
            for table in backup.get_tables(db.name):
                replicated = Table.engine_is_replicated(table.engine)
                if replicated and db.replicated_tables_handled:
                    continue
                if not replicated and (
                    db.nonreplicated_tables_handled or only_replicated
                ):
                    continue

                table_dedup_info = db_dedup_info[table.name]
                for part in table.get_parts():
                    if part.name in table_dedup_info:
                        continue

                    if part.link:
                        verified = True
                        backup_path = part.link
                        if backup_path not in dedup_backup_paths:
                            continue
                    else:
                        verified = False
                        backup_path = backup.path

                    part_dedup = PartDedupInfo(
                        database=db.name,
                        table=table.name,
                        name=part.name,
                        backup_path=backup_path,
                        checksum=part.checksum,
                        size=part.size,
                        files=part.files,
                        tarball=part.tarball,
                        disk_name=part.disk_name,
                        verified=verified,
                        encrypted=part.encrypted,
                    )

                    table_dedup_info.add(part.name)
                    dedup_info_batch.append(part_dedup.to_sql())

                    if len(dedup_info_batch) >= dedup_batch_size:
                        context.ch_ctl.insert_deduplication_info(dedup_info_batch)
                        dedup_info_batch.clear()

        if dedup_info_batch:
            context.ch_ctl.insert_deduplication_info(dedup_info_batch)

        if not databases_to_handle:
            break


def deduplicate_parts(
    context: BackupContext,
    database: str,
    table: str,
    frozen_parts: Dict[str, FrozenPart],
) -> Dict[str, PartMetadata]:
    """
    Deduplicate part if it's possible.
    """
    layout = context.backup_layout

    existing_parts = context.ch_ctl.get_deduplication_info(
        database, table, frozen_parts
    )
    deduplicated_parts: Dict[str, PartMetadata] = {}

    for existing_part in existing_parts:
        part = PartMetadata(
            database=database,
            table=table,
            name=existing_part["name"],
            checksum=existing_part["checksum"],
            size=int(existing_part["size"]),
            link=existing_part["backup_path"],
            files=existing_part["files"],
            tarball=existing_part["tarball"],
            disk_name=existing_part["disk_name"],
            encrypted=existing_part.get("encrypted", True),
        )

        if not existing_part["verified"]:
            if not layout.check_data_part(existing_part["backup_path"], part):
                logging.debug(
                    'Part "{}" found in "{}", but it\'s invalid, skipping',
                    part.name,
                    existing_part["backup_path"],
                )
                continue

        deduplicated_parts[part.name] = part

        logging.debug(
            'Part "{}" found in "{}", reusing', part.name, existing_part["backup_path"]
        )

    return deduplicated_parts


def collect_dedup_references_for_batch_backup_deletion(
    layout: BackupLayout,
    retained_backups_light_meta: List[BackupMetadata],
    deleting_backups_light_meta: List[BackupMetadata],
) -> Dict[str, DedupReferences]:
    """
    Collect deduplication information for deleting multiple backups. It contains names of data parts that should
    pe preserved during deletion.
    """
    dedup_references: Dict[str, DedupReferences] = defaultdict(
        _create_empty_dedup_references
    )

    deleting_backup_name_resolver = {
        b.path: b.name for b in deleting_backups_light_meta
    }
    for backup in retained_backups_light_meta:
        backup = layout.reload_backup(backup, use_light_meta=False)
        for db_name in backup.get_databases():
            for table in backup.get_tables(db_name):
                for part in table.get_parts():
                    if not part.link:
                        continue

                    backup_name = deleting_backup_name_resolver.get(part.link)
                    if not backup_name:
                        continue

                    _add_part_to_dedup_references(dedup_references[backup_name], part)

    return dedup_references


def _add_part_to_dedup_references(
    dedup_references: DedupReferences, part: PartMetadata
) -> None:
    dedup_references[part.database][part.table].add(part.name)
