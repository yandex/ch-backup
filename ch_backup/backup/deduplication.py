"""
Data part deduplication.
"""

from collections import defaultdict
from datetime import timedelta
from typing import Dict, List, Optional, Sequence, Set

from ch_backup import logging
from ch_backup.backup.layout import BackupLayout
from ch_backup.backup.metadata import BackupMetadata, BackupState, PartMetadata
from ch_backup.clickhouse.control import FreezedPart
from ch_backup.util import utcnow


class PartDedupInfo:
    """
    Information about data part to use for deduplication / creation incremental backups.
    """
    def __init__(self, backup_path: str, checksum: str, size: int, files: Sequence[str], tarball: bool, disk_name: str,
                 verified: bool) -> None:
        self.backup_path = backup_path
        self.checksum = checksum
        self.size = size
        self.files = files
        self.tarball = tarball
        self.disk_name = disk_name
        self.verified = verified


TableDedupInfo = Dict[str, PartDedupInfo]


class DatabaseDedupInfo:
    """
    Information about data parts of single database to use for deduplication and creation of incremental backups.
    """
    def __init__(self) -> None:
        self._tables: Dict[str, TableDedupInfo] = defaultdict(dict)

    def table(self, table_name: str) -> TableDedupInfo:
        """
        Return deduplication information for the table.
        """
        return self._tables[table_name]


class DedupInfo:
    """
    Information about data parts of all databases to use for deduplication and creation of incremental backups.
    """
    def __init__(self) -> None:
        self._databases: Dict[str, DatabaseDedupInfo] = defaultdict(DatabaseDedupInfo)

    def database(self, database_name: str) -> DatabaseDedupInfo:
        """
        Return deduplication information for the database.
        """
        return self._databases[database_name]


def collect_dedup_info(config: dict,
                       layout: BackupLayout,
                       databases: Sequence[str],
                       schema_only: bool,
                       backups_with_light_meta: List[BackupMetadata] = None) -> DedupInfo:
    """
    Collect deduplication information for creating incremental backups.
    """
    dedup_info = DedupInfo()

    # Do not populate DedupInfo if we are creating schema-only backup.
    if schema_only:
        return dedup_info

    if not backups_with_light_meta:
        backups_with_light_meta = layout.get_backups(use_light_meta=True)

    backup_age_limit = None
    if config.get('deduplicate_parts'):
        backup_age_limit = utcnow() - timedelta(**config['deduplication_age_limit'])

    # Determine backups that can be used for deduplication.
    dedup_backups = []
    for backup in backups_with_light_meta:
        if not backup_age_limit or backup.start_time < backup_age_limit:
            break

        if backup.schema_only:
            continue

        dedup_backups.append(backup)

    _populate_dedup_info(dedup_info, layout, dedup_backups, databases)

    return dedup_info


def _populate_dedup_info(dedup_info: DedupInfo, layout: BackupLayout,
                         dedup_backups_with_light_meta: List[BackupMetadata], databases: Sequence[str]) -> None:
    databases_to_handle = set(databases)
    dedup_backup_paths = set(backup.path for backup in dedup_backups_with_light_meta)
    for backup in dedup_backups_with_light_meta:
        backup = layout.reload_backup(backup, use_light_meta=False)

        databases_to_iterate = databases_to_handle.intersection(backup.get_databases())
        for db_name in databases_to_iterate:
            db_dedup_info = dedup_info.database(db_name)
            for table in backup.get_tables(db_name):
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

                    table_dedup_info[part.name] = PartDedupInfo(backup_path=backup_path,
                                                                checksum=part.checksum,
                                                                size=part.size,
                                                                files=part.files,
                                                                tarball=part.tarball,
                                                                disk_name=part.disk_name,
                                                                verified=verified)

        if backup.state == BackupState.CREATED:
            databases_to_handle.difference_update(databases_to_iterate)

        if not databases_to_handle:
            break


def deduplicate_part(layout: BackupLayout, fpart: FreezedPart, dedup_info: TableDedupInfo) -> Optional[PartMetadata]:
    """
    Deduplicate part if it's possible.
    """
    part_name = fpart.name

    logging.debug('Looking for deduplication of part "%s"', part_name)

    existing_part = dedup_info.get(part_name)
    if not existing_part:
        return None

    if existing_part.checksum != fpart.checksum:
        return None

    part = PartMetadata(database=fpart.database,
                        table=fpart.table,
                        name=part_name,
                        checksum=existing_part.checksum,
                        size=existing_part.size,
                        link=existing_part.backup_path,
                        files=existing_part.files,
                        tarball=existing_part.tarball,
                        disk_name=existing_part.disk_name)

    if not existing_part.verified:
        if not layout.check_data_part(existing_part.backup_path, part):
            logging.debug('Part "%s" found in "%s", but it\'s invalid, skipping', part_name, existing_part.backup_path)
            return None

    logging.debug('Part "%s" found in "%s", reusing', part_name, existing_part.backup_path)

    return part


TableDedupReferences = Set[str]

DatabaseDedupReferences = Dict[str, TableDedupReferences]

DedupReferences = Dict[str, DatabaseDedupReferences]


def collect_dedup_references_for_backup_deletion(layout: BackupLayout,
                                                 retained_backups_with_light_meta: List[BackupMetadata],
                                                 deleting_backup_with_light_meta: BackupMetadata) -> DedupReferences:
    """
    Collect deduplication information for deleting backup. It contains names of data parts that should pe preserved
    during deletion.
    """
    dedup_refences = collect_dedup_references_for_batch_backup_deletion(
        layout=layout,
        retained_backups_with_light_meta=retained_backups_with_light_meta,
        deleting_backups_with_light_meta=[deleting_backup_with_light_meta])

    return dedup_refences[deleting_backup_with_light_meta.name]


def collect_dedup_references_for_batch_backup_deletion(
        layout: BackupLayout, retained_backups_with_light_meta: List[BackupMetadata],
        deleting_backups_with_light_meta: List[BackupMetadata]) -> Dict[str, DedupReferences]:
    """
    Collect deduplication information for deleting multiple backups. It contains names of data parts that should
    pe preserved during deletion.
    """
    dedup_references: Dict[str, DedupReferences] = defaultdict(dict)

    deleting_backup_name_resolver = {b.path: b.name for b in deleting_backups_with_light_meta}
    for backup in retained_backups_with_light_meta:
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


def _add_part_to_dedup_references(dedup_references: DedupReferences, part: PartMetadata) -> None:
    if part.database not in dedup_references:
        dedup_references[part.database] = {part.table: {part.name}}
        return

    db_dedup_references = dedup_references[part.database]
    if part.table not in db_dedup_references:
        db_dedup_references[part.table] = {part.name}
        return

    db_dedup_references[part.table].add(part.name)
