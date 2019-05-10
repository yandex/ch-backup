"""
ClickHouse backup layout.
"""

import os
from typing import Optional, Sequence
from urllib.parse import quote

from ch_backup import logging
from ch_backup.backup.metadata import BackupMetadata, PartMetadata
from ch_backup.clickhouse.control import FreezedPart
from ch_backup.config import Config
from ch_backup.exceptions import StorageError
from ch_backup.storage import StorageLoader

BACKUP_META_FNAME = 'backup_struct.json'


class ClickhouseBackupLayout:
    """
    Storage layout and transfer
    """

    def __init__(self, config: Config) -> None:
        self._storage_loader = StorageLoader(config)
        self._config = config['backup']

    def get_backup_path(self, backup_name: str) -> str:
        """
        Returns storage backup path
        """
        return os.path.join(self._config['path_root'], backup_name)

    def _get_backup_meta_path(self, backup_name: str) -> str:
        """
        Returns backup meta path
        """
        return os.path.join(self.get_backup_path(backup_name),
                            BACKUP_META_FNAME)

    def save_table_meta(self, backup_name: str, db_name: str, table_name: str,
                        metadata: str) -> None:
        """
        Backup table metadata (create statement).
        """
        remote_path = _table_metadata_path(self.get_backup_path(backup_name),
                                           db_name, table_name)
        try:
            self._storage_loader.upload_data(metadata,
                                             remote_path=remote_path,
                                             is_async=True,
                                             encryption=True)

            logging.debug('Saving create statement for table: %s', table_name)
        except Exception as e:
            msg = f'Failed to create async upload of {remote_path}'
            raise StorageError(msg) from e

    def save_database_meta(self, backup_name: str, db_name: str,
                           metadata: str) -> None:
        """
        Backup database metadata (create statement).
        """
        remote_path = _db_metadata_path(self.get_backup_path(backup_name),
                                        db_name)
        try:
            logging.debug('Saving create statement for database: %s', db_name)
            self._storage_loader.upload_data(metadata,
                                             remote_path=remote_path,
                                             encryption=True)
        except Exception as e:
            msg = f'Failed to create async upload of {remote_path}'
            raise StorageError(msg) from e

    def save_backup_meta(self, backup: BackupMetadata) -> None:
        """
        Upload backup meta file into storage
        """
        remote_path = self._get_backup_meta_path(backup.name)
        try:
            json_dump = backup.dump_json()
            logging.debug('Saving backup meta in key %s:\n%s', remote_path,
                          json_dump)
            self._storage_loader.upload_data(json_dump,
                                             remote_path=remote_path)
        except Exception as e:
            raise StorageError('Failed to upload backup metadata') from e

    def save_part_data(self, backup_name: str,
                       fpart: FreezedPart) -> Sequence[str]:
        """
        Backup part files and return storage paths.
        """
        remote_dir_path = os.path.join(self.get_backup_path(backup_name),
                                       'data', fpart.database, fpart.table,
                                       fpart.name)

        uploaded_files = []
        part_files = [
            f for f in os.listdir(fpart.path)
            if os.path.isfile(os.path.join(fpart.path, f))
        ]

        for part_file in part_files:
            local_fname = os.path.join(fpart.path, part_file)
            remote_fname = os.path.join(remote_dir_path, part_file)
            try:
                self._storage_loader.upload_file(local_path=local_fname,
                                                 remote_path=remote_fname,
                                                 is_async=True,
                                                 encryption=True,
                                                 delete=True)
                uploaded_files.append(remote_fname)

            except Exception as e:
                msg = 'Failed to create async upload of {0}'.format(
                    remote_fname)
                raise StorageError(msg) from e

        return uploaded_files

    def get_backup_meta(self, backup_name: str) -> Optional[BackupMetadata]:
        """
        Download backup meta from storage
        """
        path = self._get_backup_meta_path(backup_name)

        if not self._storage_loader.path_exists(path):
            return None

        try:
            data = self._storage_loader.download_data(path)
            return BackupMetadata.load_json(data)
        except Exception as e:
            raise StorageError('Failed to download backup metadata') from e

    def download_database_meta(self, backup_meta: BackupMetadata,
                               db_name: str) -> str:
        """
        Download database metadata (create statement).
        """
        remote_path = _db_metadata_path(backup_meta.path, db_name)
        return self._storage_loader.download_data(remote_path, encryption=True)

    def download_table_meta(self, backup_meta: BackupMetadata, db_name: str,
                            table_name: str) -> str:
        """
        Download table metadata (create statement).
        """
        remote_path = _table_metadata_path(backup_meta.path, db_name,
                                           table_name)
        return self._storage_loader.download_data(remote_path, encryption=True)

    def download_part_data(self, part: PartMetadata,
                           fs_part_path: str) -> None:
        """
        Restore part from detached directory
        """
        logging.debug('Downloading part %s in %s.%s', part.name, part.database,
                      part.table)

        os.makedirs(fs_part_path, exist_ok=True)

        for remote_path in part.paths:
            file_name = os.path.basename(remote_path)
            local_path = os.path.join(fs_part_path, file_name)
            try:
                logging.debug('Downloading part file %s: %s', local_path,
                              remote_path)
                self._storage_loader.download_file(remote_path=remote_path,
                                                   local_path=local_path,
                                                   is_async=True,
                                                   encryption=True)
            except Exception as e:
                msg = 'Failed to download part file {0}'.format(remote_path)
                raise StorageError(msg) from e

    def delete_loaded_files(self, delete_files: Sequence[str]) -> None:
        """
        Delete files from backup storage
        """
        try:
            logging.debug('Deleting files: %s', ', '.join(delete_files))
            self._storage_loader.delete_files(remote_paths=delete_files,
                                              is_async=True)
        except Exception as e:
            msg = 'Failed to delete files {0}'.format(', '.join(delete_files))
            raise StorageError(msg) from e

    def delete_backup_path(self, backup_name: str) -> None:
        """
        Delete files from backup storage
        """
        path = self.get_backup_path(backup_name)

        delete_files = self._storage_loader.list_dir(path,
                                                     recursive=True,
                                                     absolute=True)
        self.delete_loaded_files(delete_files)

    def get_backup_names(self) -> Sequence[str]:
        """
        Get current backup entries
        """
        return self._storage_loader.list_dir(self._config['path_root'],
                                             recursive=False,
                                             absolute=False)

    def path_exists(self, remote_path: str) -> bool:
        """
        Check whether storage path exists or not.
        """
        return self._storage_loader.path_exists(remote_path)

    def wait(self) -> None:
        """
        Wait for async jobs
        """
        try:
            logging.debug('Collecting async jobs')
            self._storage_loader.wait()
        except Exception as e:
            raise StorageError('Failed to complete async jobs') from e


def _db_metadata_path(backup_path, db_name):
    """
    Return S3 path to database metadata.
    """
    return os.path.join(backup_path, 'metadata', _quote(db_name) + '.sql')


def _table_metadata_path(backup_path, db_name, table_name):
    """
    Return S3 path to table metadata.
    """
    return os.path.join(backup_path, 'metadata', _quote(db_name),
                        _quote(table_name) + '.sql')


def _quote(value: str) -> str:
    return quote(value, safe='').translate({
        ord('.'): '%2E',
        ord('-'): '%2D',
    })
