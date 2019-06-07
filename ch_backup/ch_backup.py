"""
Clickhouse backup logic
"""

from collections import defaultdict
from copy import copy
from datetime import timedelta
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from ch_backup import logging
from ch_backup.backup.layout import BackupLayout
from ch_backup.backup.metadata import (BackupMetadata, BackupState, PartMetadata, TableMetadata)
from ch_backup.clickhouse.client import ClickhouseError
from ch_backup.clickhouse.control import ClickhouseCTL, FreezedPart, Table
from ch_backup.config import Config
from ch_backup.exceptions import BackupNotFound, ClickhouseBackupError
from ch_backup.util import now, utcnow
from ch_backup.version import get_version


class ClickhouseBackup:
    """
    Clickhouse backup logic
    """

    def __init__(self, config: Config) -> None:
        self._ch_ctl = ClickhouseCTL(config['clickhouse'])
        self._backup_layout = BackupLayout(config)
        self._config = config['backup']

    def get(self, backup_name: str) -> BackupMetadata:
        """
        Get backup information.
        """
        return self._get_backup(backup_name)

    def list(self, state: BackupState = None) -> Sequence[BackupMetadata]:
        """
        Get list of existing backups sorted by start timestamp.
        """
        backups = []
        for backup in self._iter_backups():
            if state and backup.state != state:
                continue
            backups.append(backup)

        return backups

    def backup(self,
               name: str,
               databases: Sequence[str] = None,
               tables: Sequence[str] = None,
               force: bool = False,
               labels: dict = None) -> Tuple[str, Optional[str]]:
        """
        Perform backup.

        If force is True, backup.min_interval config option is ignored.
        """
        # pylint: disable=too-many-locals,too-many-branches
        assert not (databases and tables)

        backup_labels = copy(self._config.get('labels'))
        if labels:
            backup_labels.update(labels)

        db_tables: Dict[str, list] = defaultdict(list)
        if tables:
            for table in tables or []:
                db_name, table_name = table.split('.', 1)
                db_tables[db_name].append(table_name)

            databases = list(db_tables.keys())

        if databases is None:
            databases = self._ch_ctl.get_databases(self._config['exclude_dbs'])

        backup_age_limit = None
        if self._config.get('deduplicate_parts'):
            backup_age_limit = utcnow() - timedelta(**self._config['deduplication_age_limit'])

        last_backup = None
        dedup_backups = []
        for backup in self._iter_backups():
            if not last_backup:
                last_backup = backup

            if not backup_age_limit or backup.start_time < backup_age_limit:
                break

            if backup.state in (BackupState.DELETING, BackupState.PARTIALLY_DELETED):
                continue

            dedup_backups.append(backup)

        if last_backup and not self._check_min_interval(last_backup, force):
            msg = 'Backup is skipped per backup.min_interval config option.'
            logging.info(msg)
            return (last_backup.name, msg)

        backup_meta = BackupMetadata(name=name,
                                     path=self._backup_layout.get_backup_path(name),
                                     labels=backup_labels,
                                     version=get_version(),
                                     ch_version=self._ch_ctl.get_version())

        self._backup_layout.upload_backup_metadata(backup_meta)

        logging.debug('Starting backup "%s" for databases: %s', backup_meta.name, ', '.join(databases))

        try:
            for db_name in databases:
                self._backup_database(backup_meta, db_name, db_tables[db_name], dedup_backups)
            backup_meta.state = BackupState.CREATED
        except Exception:
            logging.critical('Backup failed', exc_info=True)
            backup_meta.state = BackupState.FAILED
            raise
        finally:
            backup_meta.update_end_time()
            self._backup_layout.upload_backup_metadata(backup_meta)

            if not self._config.get('keep_freezed_data_on_failure'):
                self._ch_ctl.remove_freezed_data()

        return (backup_meta.name, None)

    def restore(self, backup_name: str, databases: Sequence[str] = None, schema_only: bool = False) -> None:
        """
        Restore specified backup
        """
        backup_meta = self._get_backup(backup_name)

        if databases is None:
            databases = backup_meta.get_databases()
        else:
            # check all required databases exists in backup meta
            missed_databases = [db_name for db_name in databases if db_name not in backup_meta.get_databases()]
            if missed_databases:
                logging.critical('Required databases %s were not found in backup meta: %s',
                                 ', '.join(missed_databases), backup_meta.path)
                raise ClickhouseBackupError('Required databases were not found in backup struct')

        for db_name in databases:
            self._restore_database_schema(db_name, backup_meta)
            if schema_only:
                logging.debug('Don\'t restore %s data, cause schema_only is set %r', db_name, schema_only)
            else:
                self._restore_database_data(db_name, backup_meta)

    def _backup_database(self, backup_meta: BackupMetadata, db_name: str, tables: Sequence[str],
                         dedup_backups: Sequence[BackupMetadata]) -> None:
        """
        Backup database.
        """
        logging.debug('Performing database backup for "%s"', db_name)

        schema = self._ch_ctl.get_database_schema(db_name)
        self._backup_layout.upload_database_create_statement(backup_meta.name, db_name, schema)

        backup_meta.add_database(db_name)

        for table in self._ch_ctl.get_tables_ordered(db_name, tables):
            self._backup_table(backup_meta, table, dedup_backups)

    def _backup_table(self, backup_meta: BackupMetadata, table: Table,
                      dedup_backups: Sequence[BackupMetadata]) -> None:
        """
        Backup table.
        """
        logging.debug('Performing table backup for "%s"."%s"', table.database, table.name)

        self._backup_layout.upload_table_create_statement(backup_meta.name, table.database, table.name,
                                                          table.create_statement)

        table_meta = TableMetadata(table.database, table.name, table.engine)

        try:
            freezed_parts = self._ch_ctl.freeze_table(table)
        except ClickhouseError:
            if self._ch_ctl.does_table_exist(table.database, table.name):
                raise

            logging.warning('Table "%s"."%s" was removed by a user during backup', table.database, table.name)
            return

        for fpart in freezed_parts:
            logging.debug('Working on %s', fpart)

            # trying to find part in storage
            part = self._deduplicate_part(fpart, dedup_backups)
            if not part:
                part = self._backup_layout.upload_data_part(backup_meta.name, fpart)
            else:
                self._ch_ctl.remove_freezed_part(fpart)

            table_meta.add_part(part)

        backup_meta.add_table(table_meta)

        self._backup_layout.upload_backup_metadata(backup_meta)

        self._backup_layout.wait()

        self._ch_ctl.remove_freezed_data()

    def delete(self, backup_name: str) -> Tuple[Optional[str], Optional[str]]:
        """
        Delete the specified backup.
        """
        deleting_backup = None
        newer_backups = []
        for backup in self._iter_backups():
            if backup.name == backup_name:
                deleting_backup = backup
                break

            if backup.state in (BackupState.DELETING, BackupState.PARTIALLY_DELETED):
                continue

            newer_backups.append(backup)

        if not deleting_backup:
            raise BackupNotFound(backup_name)

        return self._delete(deleting_backup, newer_backups)

    def _delete(self, backup: BackupMetadata,
                newer_backups: Sequence[BackupMetadata]) -> Tuple[Optional[str], Optional[str]]:
        logging.info('Deleting backup %s, state: %s', backup.name, backup.state)

        is_changed, deleting_parts = self._pop_deleting_parts(backup, newer_backups)
        is_empty = backup.is_empty()

        if not is_empty and not is_changed:
            logging.info('Nothing was found for deletion')
            return None, 'Backup was not deleted as its data is in use by subsequent backups per ' \
                         'deduplication settings.'

        backup.state = BackupState.DELETING
        self._backup_layout.upload_backup_metadata(backup)

        try:
            # delete whole backup prefix if backup entry is empty
            if is_empty:
                logging.info('Removing backup data entirely')
                self._backup_layout.delete_backup(backup.name)
                return backup.name, None

            backup.state = BackupState.PARTIALLY_DELETED

            # some data (not linked) parts were deleted
            if deleting_parts:
                logging.info('Removing non-linked backup data parts')
                self._backup_layout.delete_data_parts(backup, deleting_parts)

            return None, 'Backup was partially deleted as its data is in use by subsequent backups per ' \
                         'deduplication settings.'

        except Exception:
            logging.critical('Delete failed', exc_info=True)
            backup.state = BackupState.FAILED
            raise

        finally:
            self._backup_layout.wait()
            if not is_empty:
                self._backup_layout.upload_backup_metadata(backup)

    def purge(self) -> Tuple[Sequence[str], Optional[str]]:
        """
        Purge backups.
        """
        retain_time = self._config['retain_time']
        retain_count = self._config['retain_count']

        deleted_backup_names: List[str] = []

        if not retain_time and retain_count is None:
            logging.info('Retain policies are not specified')
            return deleted_backup_names, 'Retain policies are not specified.'

        retain_time_limit = None
        if retain_time:
            retain_time_limit = now() - timedelta(**retain_time)

        retained_backups: List[BackupMetadata] = []
        deleting_backups: List[BackupMetadata] = []
        for name, backup in self._iter_backup_dir():
            if not backup:
                logging.info('Deleting backup without metadata: %s', name)
                self._backup_layout.delete_backup(name)
                continue

            if retain_count and len(retained_backups) < retain_count:
                if backup.state == BackupState.CREATED:
                    logging.info('Preserving backup per retain count policy: %s, state %s', name, backup.state)
                    retained_backups.append(backup)
                    continue

            if retain_time_limit and backup.start_time >= retain_time_limit:
                logging.info('Preserving backup per retain time policy: %s, state %s', name, backup.state)
                retained_backups.append(backup)
                continue

            deleting_backups.append(backup)

        deduplicatable_backups = [
            backup for backup in retained_backups
            if backup.state not in (BackupState.DELETING, BackupState.PARTIALLY_DELETED)
        ]

        for backup in deleting_backups:
            backup_name, _ = self._delete(backup, deduplicatable_backups)
            if backup_name:
                deleted_backup_names.append(backup_name)

        return deleted_backup_names, None

    def _restore_database_schema(self, db_name: str, backup_meta: BackupMetadata) -> None:
        """
        Restore database schema
        """
        logging.debug('Running database schema restore: %s', db_name)

        db_sql = self._backup_layout.get_database_create_statement(backup_meta, db_name)
        self._ch_ctl.restore_meta(db_sql)

        for table_meta in backup_meta.get_tables(db_name):
            table_sql = self._backup_layout.get_table_create_statement(backup_meta, db_name, table_meta.name)
            self._ch_ctl.restore_meta(table_sql)

    def _restore_database_data(self, db_name: str, backup_meta: BackupMetadata) -> None:
        """
        Restore database data
        """
        # restore table data (download and attach parts)
        for table_meta in backup_meta.get_tables(db_name):
            logging.debug('Running table "%s.%s" data restore', db_name, table_meta.name)

            table = self._ch_ctl.get_table(db_name, table_meta.name)

            attach_parts = []
            for part in table_meta.get_parts():
                fs_part_path = self._ch_ctl.get_detached_part_path(table, part.name)
                self._backup_layout.download_data_part(backup_meta, part, fs_part_path)
                attach_parts.append(part.name)

            self._backup_layout.wait()

            self._ch_ctl.chown_detached_table_parts(table)
            for part_name in attach_parts:
                logging.debug('Attaching "%s.%s" part: %s', db_name, table.name, part_name)
                self._ch_ctl.attach_part(table, part_name)

    def _deduplicate_part(self, fpart: FreezedPart, dedup_backups: Sequence[BackupMetadata]) -> Optional[PartMetadata]:
        """
        Deduplicate part if it's possible
        """
        logging.debug('Looking for deduplication of part "%s"', fpart.name)

        db_name = fpart.database
        table_name = fpart.table
        part_name = fpart.name
        for backup_meta in dedup_backups:
            existing_part = backup_meta.find_part(db_name, table_name, part_name)

            if not existing_part:
                logging.debug('Part "%s" was not found in backup "%s", skip', part_name, backup_meta.name)
                continue

            if existing_part.link:
                logging.debug('Part "%s" in backup "%s" is link, skip', part_name, backup_meta.name)
                continue

            if existing_part.checksum != fpart.checksum:
                logging.debug('Part "%s" in backup "%s" has mismatched checksum, skip', part_name, backup_meta.name)
                continue

            if not self._backup_layout.check_data_part(backup_meta, existing_part):
                logging.debug('Part "%s" in backup "%s" is invalid, skip', part_name, backup_meta.name)
                continue

            logging.info('Deduplicating part "%s" based on %s', part_name, backup_meta.name)
            return PartMetadata(database=db_name,
                                table=table_name,
                                name=part_name,
                                checksum=existing_part.checksum,
                                size=existing_part.size,
                                link=backup_meta.path,
                                files=existing_part.files)

        return None

    def _get_backup(self, backup_name: str) -> BackupMetadata:
        backup = self._backup_layout.get_backup_metadata(backup_name)
        if not backup:
            raise BackupNotFound(backup_name)

        return backup

    def _iter_backup_dir(self) -> Iterable[Tuple[str, Optional[BackupMetadata]]]:
        logging.debug('Collecting existing backups')

        def _sort_key(item: Tuple[str, Optional[BackupMetadata]]) -> str:
            backup = item[1]
            return backup.start_time.isoformat() if backup else ''

        result = []
        for name in self._backup_layout.get_backup_names():
            try:
                backup = self._backup_layout.get_backup_metadata(name)
                result.append((name, backup))
            except Exception:
                logging.exception('Failed to load metadata for backup %s', name)

        return sorted(result, key=_sort_key, reverse=True)

    def _iter_backups(self) -> Iterable[BackupMetadata]:
        for _name, backup in self._iter_backup_dir():
            if backup:
                yield backup

    def _check_min_interval(self, last_backup: BackupMetadata, force: bool) -> bool:
        if force:
            return True

        min_interval = self._config.get('min_interval')
        if not min_interval:
            return True

        if not last_backup.end_time:
            return True

        if utcnow() - last_backup.end_time >= timedelta(**min_interval):
            return True

        return False

    @staticmethod
    def _pop_deleting_parts(backup_meta: BackupMetadata,
                            newer_backups: Sequence[BackupMetadata]) -> Tuple[bool, Sequence[PartMetadata]]:
        """
        Get backup parts which are safe to delete.
        """
        skip_parts = {}
        for new_backup in newer_backups:
            for part in new_backup.get_parts():
                if not part.link:
                    continue

                if not part.link.endswith(backup_meta.name):
                    continue

                part_id = (part.database, part.table, part.name)
                skip_parts[part_id] = new_backup.name

        is_changed = False
        deleting_parts: List[PartMetadata] = []
        for part in backup_meta.get_parts():
            part_id = (part.database, part.table, part.name)

            logging.debug('Working on part "%s.%s.%s"', *part_id)
            if part_id in skip_parts:
                logging.debug('Skip removing part "%s": link from "%s" was found', part.name, skip_parts[part_id])
                continue

            logging.debug('Dropping part contents from meta "%s"', part.name)

            if not part.link:
                logging.debug('Scheduling deletion of part files "%s"', part.name)
                deleting_parts.append(part)

            backup_meta.remove_part(part)
            is_changed = True

        return is_changed, deleting_parts
