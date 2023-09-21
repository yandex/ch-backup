"""
Data part deduplication.
"""
from collections import defaultdict
from copy import copy
from datetime import timedelta
from typing import Dict, List, Optional, Sequence, Set

from ch_backup import logging
from ch_backup.backup.layout import BackupLayout
from ch_backup.backup.metadata import BackupMetadata, BackupState, PartMetadata
from ch_backup.backup_context import BackupContext
from ch_backup.clickhouse.models import Database, FrozenPart
from ch_backup.clickhouse.schema import is_replicated
from ch_backup.util import utcnow


class PartDedupInfo:
    """
    Information about data part to use for deduplication / creation incremental backups.
    """

    def __init__(
        self,
        backup_path: str,
        checksum: str,
        size: int,
        files: Sequence[str],
        tarball: bool,
        disk_name: str,
        verified: bool,
    ) -> None:
        self.backup_path = backup_path
        self.checksum = checksum
        self.size = size
        self.files = files
        self.tarball = tarball
        self.disk_name = disk_name
        self.verified = verified

    def __repr__(self):
        return f"PartDedupInfo({self.__dict__})"

    def __eq__(self, other):
        return self.__dict__ == other.__dict__


TableDedupInfo = Dict[str, PartDedupInfo]


class DatabaseDedupInfo:
    """
    Information about data parts of single database to use for deduplication and creation of incremental backups.
    """

    def __init__(self, tables: Dict[str, TableDedupInfo] = None) -> None:
        if tables is None:
            tables = defaultdict(dict)
        self._tables = tables

    def table(self, table_name: str) -> TableDedupInfo:
        """
        Return deduplication information for the table.
        """
        return self._tables[table_name]

    def __repr__(self):
        return f"DatabaseDedupInfo({dict(self._tables)})"

    def __eq__(self, other):
        return self.__dict__ == other.__dict__


class DedupInfo:
    """
    Information about data parts of all databases to use for deduplication and creation of incremental backups.
    """

    def __init__(self, databases: Dict[str, DatabaseDedupInfo] = None) -> None:
        if databases is None:
            databases = defaultdict(DatabaseDedupInfo)
        self._databases = databases

    def database(self, database_name: str) -> DatabaseDedupInfo:
        """
        Return deduplication information for the database.
        """
        return self._databases[database_name]

    def __repr__(self):
        return f"DedupInfo({dict(self._databases)})"

    def __eq__(self, other):
        return self.__dict__ == other.__dict__


def collect_dedup_info(
    context: BackupContext,
    databases: Sequence[Database],
    backups_with_light_meta: List[BackupMetadata],
) -> DedupInfo:
    """
    Collect deduplication information for creating incremental backups.
    """
    dedup_info = DedupInfo()

    # Do not populate DedupInfo if we are creating schema-only backup.
    if context.backup_meta.schema_only:
        return dedup_info

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
        dedup_info,
        context.backup_layout,
        context.backup_meta.hostname,
        dedup_backups,
        databases,
    )

    return dedup_info


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
    dedup_info: DedupInfo,
    layout: BackupLayout,
    hostname: str,
    dedup_backups_with_light_meta: List[BackupMetadata],
    databases: Sequence[Database],
) -> None:
    # pylint: disable=too-many-locals,too-many-branches
    databases_to_handle = {db.name: _DatabaseToHandle(db.name) for db in databases}
    dedup_backup_paths = set(backup.path for backup in dedup_backups_with_light_meta)
    for backup in dedup_backups_with_light_meta:
        backup = layout.reload_backup(backup, use_light_meta=False)

        # Process only replicated tables if backup is created on replica.
        only_replicated = hostname != backup.hostname

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

        for db in databases_to_iterate:
            db_dedup_info = dedup_info.database(db.name)
            for table in backup.get_tables(db.name):
                replicated = is_replicated(table.engine)
                if replicated and db.replicated_tables_handled:
                    continue
                if not replicated and (
                    db.nonreplicated_tables_handled or only_replicated
                ):
                    continue

                table_dedup_info = db_dedup_info.table(table.name)
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

                    table_dedup_info[part.name] = PartDedupInfo(
                        backup_path=backup_path,
                        checksum=part.checksum,
                        size=part.size,
                        files=part.files,
                        tarball=part.tarball,
                        disk_name=part.disk_name,
                        verified=verified,
                    )

        if not databases_to_handle:
            break


def deduplicate_part(
    layout: BackupLayout, fpart: FrozenPart, dedup_info: TableDedupInfo
) -> Optional[PartMetadata]:
    """
    Deduplicate part if it's possible.
    """
    part_name = fpart.name

    logging.debug('Looking for deduplication of part "{}"', part_name)

    existing_part = dedup_info.get(part_name)
    if not existing_part:
        return None

    if existing_part.checksum != fpart.checksum:
        return None

    part = PartMetadata(
        database=fpart.database,
        table=fpart.table,
        name=part_name,
        checksum=existing_part.checksum,
        size=existing_part.size,
        link=existing_part.backup_path,
        files=existing_part.files,
        tarball=existing_part.tarball,
        disk_name=existing_part.disk_name,
    )

    if not existing_part.verified:
        if not layout.check_data_part(existing_part.backup_path, part):
            logging.debug(
                'Part "{}" found in "{}", but it\'s invalid, skipping',
                part_name,
                existing_part.backup_path,
            )
            return None

    logging.debug(
        'Part "{}" found in "{}", reusing', part_name, existing_part.backup_path
    )

    return part


TableDedupReferences = Set[str]

DatabaseDedupReferences = Dict[str, TableDedupReferences]

DedupReferences = Dict[str, DatabaseDedupReferences]


def collect_dedup_references_for_batch_backup_deletion(
    retained_backups: List[BackupMetadata],
    deleting_backups: List[BackupMetadata],
) -> Dict[str, DedupReferences]:
    """
    Collect deduplication information for deleting multiple backups. It contains names of data parts that should
    pe preserved during deletion.
    """
    dedup_references: Dict[str, DedupReferences] = defaultdict(dict)

    deleting_backup_name_resolver = {b.path: b.name for b in deleting_backups}
    for backup in retained_backups:
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
    if part.database not in dedup_references:
        dedup_references[part.database] = {part.table: {part.name}}
        return

    db_dedup_references = dedup_references[part.database]
    if part.table not in db_dedup_references:
        db_dedup_references[part.table] = {part.name}
        return

    db_dedup_references[part.table].add(part.name)
