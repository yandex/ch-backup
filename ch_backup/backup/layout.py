"""
Management of backup data layout.
"""

import os
from typing import List, Optional, Sequence
from urllib.parse import quote

from ch_backup import logging
from ch_backup.backup.metadata import BackupMetadata, PartMetadata
from ch_backup.clickhouse.control import FreezedPart
from ch_backup.config import Config
from ch_backup.exceptions import StorageError
from ch_backup.storage import StorageLoader

BACKUP_META_FNAME = 'backup_struct.json'


class BackupLayout:
    """
    Class responsible for management of backup data layout.
    """

    def __init__(self, config: Config) -> None:
        self._storage_loader = StorageLoader(config)
        self._config = config['backup']

    def upload_backup_metadata(self, backup: BackupMetadata) -> None:
        """
        Upload backup metadata.
        """
        remote_path = self._backup_metadata_path(backup.name)
        try:
            logging.debug('Saving backup metadata in %s', remote_path)
            self._storage_loader.upload_data(backup.dump_json(), remote_path=remote_path)
        except Exception as e:
            raise StorageError('Failed to upload backup metadata') from e

    def upload_database_create_statement(self, backup_name: str, db_name: str, metadata: str) -> None:
        """
        Upload database create statement.
        """
        remote_path = _db_metadata_path(self.get_backup_path(backup_name), db_name)
        try:
            logging.debug('Uploading metadata (create statement) for database "%s"', db_name)
            self._storage_loader.upload_data(metadata, remote_path=remote_path, encryption=True)
        except Exception as e:
            msg = f'Failed to create async upload of {remote_path}'
            raise StorageError(msg) from e

    def upload_table_create_statement(self, backup_name: str, db_name: str, table_name: str, metadata: str) -> None:
        """
        Upload table create statement.
        """
        remote_path = _table_metadata_path(self.get_backup_path(backup_name), db_name, table_name)
        try:
            logging.debug('Uploading metadata (create statement) for table "%s"."%s"', db_name, table_name)
            self._storage_loader.upload_data(metadata, remote_path=remote_path, is_async=True, encryption=True)
        except Exception as e:
            msg = f'Failed to create async upload of {remote_path}'
            raise StorageError(msg) from e

    def upload_data_part(self, backup_name: str, fpart: FreezedPart) -> PartMetadata:
        """
        Upload part data.
        """
        logging.debug('Uploading data part %s of "%s"."%s"', fpart.name, fpart.database, fpart.table)

        remote_dir_path = _part_path(self.get_backup_path(backup_name), fpart.database, fpart.table, fpart.name)

        filenames = os.listdir(fpart.path)
        for filename in filenames:
            local_path = os.path.join(fpart.path, filename)
            remote_path = os.path.join(remote_dir_path, filename)
            try:
                self._storage_loader.upload_file(local_path=local_path,
                                                 remote_path=remote_path,
                                                 is_async=True,
                                                 encryption=True,
                                                 delete=True)
            except Exception as e:
                msg = f'Failed to create async upload of {remote_path}'
                raise StorageError(msg) from e

        return PartMetadata(database=fpart.database,
                            table=fpart.table,
                            name=fpart.name,
                            checksum=fpart.checksum,
                            size=fpart.size,
                            files=filenames)

    def get_backup_names(self) -> Sequence[str]:
        """
        Get names of existing backups.
        """
        return self._storage_loader.list_dir(self._config['path_root'], recursive=False, absolute=False)

    def get_backup_metadata(self, backup_name: str) -> Optional[BackupMetadata]:
        """
        Download and return backup metadata.
        """
        path = self._backup_metadata_path(backup_name)

        if not self._storage_loader.path_exists(path):
            return None

        try:
            data = self._storage_loader.download_data(path)
            return BackupMetadata.load_json(data)
        except Exception as e:
            raise StorageError('Failed to download backup metadata') from e

    def get_database_create_statement(self, backup_meta: BackupMetadata, db_name: str) -> str:
        """
        Download and return database create statement.
        """
        remote_path = _db_metadata_path(backup_meta.path, db_name)
        return self._storage_loader.download_data(remote_path, encryption=True)

    def get_table_create_statement(self, backup_meta: BackupMetadata, db_name: str, table_name: str) -> str:
        """
        Download and return table create statement.
        """
        remote_path = _table_metadata_path(backup_meta.path, db_name, table_name)
        return self._storage_loader.download_data(remote_path, encryption=True)

    def download_data_part(self, backup_meta: BackupMetadata, part: PartMetadata, fs_part_path: str) -> None:
        """
        Download part data to the specified directory.
        """
        logging.debug('Downloading data part %s of "%s"."%s"', part.name, part.database, part.table)

        os.makedirs(fs_part_path, exist_ok=True)

        remote_dir_path = _part_path(part.link or backup_meta.path, part.database, part.table, part.name)

        for filename in part.files:
            local_path = os.path.join(fs_part_path, filename)
            remote_path = os.path.join(remote_dir_path, filename)
            try:
                logging.debug('Downloading part file: %s', remote_path)
                self._storage_loader.download_file(remote_path=remote_path,
                                                   local_path=local_path,
                                                   is_async=True,
                                                   encryption=True)
            except Exception as e:
                msg = f'Failed to download part file {remote_path}'
                raise StorageError(msg) from e

    def check_data_part(self, backup_meta: BackupMetadata, part: PartMetadata) -> bool:
        """
        Check availability of part data in storage.
        """
        remote_dir_path = _part_path(part.link or backup_meta.path, part.database, part.table, part.name)
        remote_files = self._storage_loader.list_dir(remote_dir_path)

        notfound_files = set(part.files) - set(remote_files)
        if notfound_files:
            logging.warning('Some part files were not found in %s: %s', remote_dir_path, ', '.join(notfound_files))
            return False

        return True

    def delete_backup(self, backup_name: str) -> None:
        """
        Delete backup data and metadata from storage.
        """
        backup_path = self.get_backup_path(backup_name)

        logging.debug('Deleting data in %s', backup_path)

        deleting_files = self._storage_loader.list_dir(backup_path, recursive=True, absolute=True)
        self._delete_files(deleting_files)

    def delete_data_parts(self, backup_meta: BackupMetadata, parts: Sequence[PartMetadata]) -> None:
        """
        Delete backup data parts from storage.
        """
        deleting_files: List[str] = []
        for part in parts:
            part_path = _part_path(part.link or backup_meta.path, part.database, part.table, part.name)
            logging.debug('Deleting data part %s', part_path)
            deleting_files.extend(os.path.join(part_path, f) for f in part.files)

        self._delete_files(deleting_files)

    def wait(self) -> None:
        """
        Wait for completion of data upload and download.
        """
        try:
            logging.debug('Waiting for completion of async operations')
            self._storage_loader.wait()
        except Exception as e:
            raise StorageError('Failed to complete async operations') from e

    def get_backup_path(self, backup_name: str) -> str:
        """
        Get backup path by backup name.
        """
        return os.path.join(self._config['path_root'], backup_name)

    def _delete_files(self, remote_paths: Sequence[str]) -> None:
        """
        Delete files from storage.
        """
        try:
            self._storage_loader.delete_files(remote_paths=remote_paths, is_async=True)
        except Exception as e:
            msg = f'Failed to delete files {", ".join(remote_paths)}'
            raise StorageError(msg) from e

    def _backup_metadata_path(self, backup_name: str) -> str:
        return os.path.join(self.get_backup_path(backup_name), BACKUP_META_FNAME)


def _db_metadata_path(backup_path: str, db_name: str) -> str:
    """
    Return S3 path to database metadata.
    """
    return os.path.join(backup_path, 'metadata', _quote(db_name) + '.sql')


def _table_metadata_path(backup_path: str, db_name: str, table_name: str) -> str:
    """
    Return S3 path to table metadata.
    """
    return os.path.join(backup_path, 'metadata', _quote(db_name), _quote(table_name) + '.sql')


def _part_path(backup_path: str, db_name: str, table_name: str, part_name: str) -> str:
    """
    Return S3 path to data part.
    """
    return os.path.join(backup_path, 'data', db_name, table_name, part_name)


def _quote(value: str) -> str:
    return quote(value, safe='').translate({
        ord('.'): '%2E',
        ord('-'): '%2D',
    })
