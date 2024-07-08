"""
Management of backup data layout.
"""

import os
from pathlib import Path
from typing import Callable, List, Optional, Sequence
from urllib.parse import quote

from nacl.exceptions import CryptoError

from ch_backup import logging
from ch_backup.backup.metadata import BackupMetadata, PartMetadata
from ch_backup.calculators import calc_encrypted_size, calc_tarball_size
from ch_backup.clickhouse.models import Database, Disk, FrozenPart, Table
from ch_backup.config import Config
from ch_backup.encryption import get_encryption
from ch_backup.exceptions import StorageError
from ch_backup.storage import StorageLoader
from ch_backup.storage.engine.s3 import S3RetryingError
from ch_backup.util import dir_is_empty, escape_metadata_file_name

BACKUP_META_FNAME = "backup_struct.json"
BACKUP_LIGHT_META_FNAME = "backup_light_struct.json"
ACCESS_CONTROL_FNAME = "access_control.tar"
COMPRESSED_EXTENSION = ".gz"


class BackupLayout:
    """
    Class responsible for management of backup data layout.
    """

    def __init__(self, config: Config) -> None:
        self._storage_loader = StorageLoader(config)
        self._config = config["backup"]
        self._access_control_path = config["clickhouse"]["access_control_path"]
        self._metadata_path = config["clickhouse"]["metadata_path"]
        self._named_collections_path = config["clickhouse"]["named_collections_path"]
        enc_conf = config["encryption"]
        self._encryption_chunk_size = enc_conf["chunk_size"]
        self._encryption_metadata_size = get_encryption(
            enc_conf["type"], enc_conf
        ).metadata_size()

    def upload_backup_metadata(self, backup: BackupMetadata) -> None:
        """
        Upload backup metadata.
        """
        remote_path = self._backup_metadata_path(backup.name)
        remote_light_path = self._backup_light_metadata_path(backup.name)
        try:
            logging.debug("Saving backup metadata in {}", remote_path)
            self._storage_loader.upload_data(
                backup.dump_json(light=False), remote_path=remote_path, encryption=True
            )
            logging.debug("Saving backup light metadata in {}", remote_light_path)
            self._storage_loader.upload_data(
                backup.dump_json(light=True), remote_path=remote_light_path
            )
        except Exception as e:
            raise StorageError("Failed to upload backup metadata") from e

    def upload_database_create_statement(self, backup_name: str, db: Database) -> None:
        """
        Upload database create statement from metadata file.
        """
        local_path = os.path.join(
            self._metadata_path, f"{escape_metadata_file_name(db.name)}.sql"
        )
        remote_path = _db_metadata_path(self.get_backup_path(backup_name), db.name)
        try:
            logging.debug(
                'Uploading metadata (create statement) for database "{}"', db.name
            )
            self._storage_loader.upload_file(
                local_path, remote_path=remote_path, encryption=True
            )
        except Exception as e:
            msg = f"Failed to create async upload of {remote_path}"
            raise StorageError(msg) from e

    def upload_table_create_statement(
        self, backup_name: str, db: Database, table: Table, create_statement: bytes
    ) -> None:
        """
        Upload table create statement.
        """
        assert db.metadata_path is not None

        remote_path = _table_metadata_path(
            self.get_backup_path(backup_name), db.name, table.name
        )
        try:
            logging.debug(
                'Uploading metadata (create statement) for table "{}"."{}"',
                db.name,
                table.name,
            )
            self._storage_loader.upload_data(
                create_statement, remote_path, is_async=True, encryption=True
            )
        except Exception as e:
            msg = f"Failed to create async upload of {remote_path}"
            raise StorageError(msg) from e

    def upload_access_control_file(self, backup_name: str, file_name: str) -> None:
        """
        Upload access control list.
        """
        local_path = os.path.join(self._access_control_path, file_name)
        remote_path = _access_control_data_path(
            self.get_backup_path(backup_name), file_name
        )
        try:
            logging.debug('Uploading access control data "{}"', local_path)
            self._storage_loader.upload_file(
                local_path=local_path, remote_path=remote_path, encryption=True
            )
        except Exception as e:
            msg = f'Failed to upload access control metadata file "{remote_path}"'
            raise StorageError(msg) from e

    def upload_access_control_files(
        self, local_path: str, backup_name: str, file_names: List[str]
    ) -> None:
        """
        Upload access control list.
        """
        remote_path = _access_control_data_path(
            self.get_backup_path(backup_name), ACCESS_CONTROL_FNAME
        )
        try:
            logging.debug('Uploading access control data "{}"', local_path)
            self._storage_loader.upload_files_tarball(
                local_path,
                remote_path,
                files=file_names,
                encryption=True,
            )

        except Exception as e:
            msg = f'Failed to upload access control metadata file "{remote_path}"'
            raise StorageError(msg) from e

    def upload_udf(self, backup_name: str, file_name: str, metadata: str) -> None:
        """
        Upload user defined function data
        """
        remote_path = _udf_data_path(self.get_backup_path(backup_name), file_name)
        try:
            self._storage_loader.upload_data(
                data=metadata, remote_path=remote_path, encryption=True
            )
        except Exception as e:
            msg = f'Failed to upload udf metadata "{remote_path}"'
            raise StorageError(msg) from e

    def upload_data_part(
        self, backup_name: str, fpart: FrozenPart, callback: Callable
    ) -> None:
        """
        Upload part data.
        """
        logging.debug(
            'Uploading data part {} of "{}"."{}"',
            fpart.name,
            fpart.database,
            fpart.table,
        )

        remote_dir_path = _part_path(
            self.get_backup_path(backup_name), fpart.database, fpart.table, fpart.name
        )
        remote_path = os.path.join(remote_dir_path, fpart.name + ".tar")
        try:
            self._storage_loader.upload_files_tarball(
                dir_path=fpart.path,
                files=fpart.files,
                remote_path=remote_path,
                is_async=True,
                encryption=True,
                delete=True,
                callback=callback,
            )
        except Exception as e:
            msg = f"Failed to create async upload of {remote_path}"
            raise StorageError(msg) from e

    def upload_cloud_storage_metadata(
        self, backup_meta: BackupMetadata, disk: Disk, delete_after_upload: bool = False
    ) -> bool:
        """
        Upload specified disk metadata files from given directory path as a tarball.
        Returns: whether backed up disk had data.
        """
        backup_name = backup_meta.get_sanitized_name()
        compression = backup_meta.cloud_storage.compressed
        remote_path = _disk_metadata_path(
            self.get_backup_path(backup_name), disk.name, compression
        )
        shadow_path = os.path.join(disk.path, "shadow", backup_name)
        exclude_file_names = ["frozen_metadata.txt"]
        if dir_is_empty(shadow_path, exclude_file_names):
            return False

        logging.debug(f'Uploading "{shadow_path}" content to "{remote_path}"')

        try:
            self._storage_loader.upload_files_tarball_scan(
                dir_path=shadow_path,
                remote_path=remote_path,
                exclude_file_names=exclude_file_names,
                is_async=True,
                encryption=backup_meta.cloud_storage.encrypted,
                delete=delete_after_upload,
                compression=compression,
            )
        except Exception as e:
            msg = f'Failed to upload "{shadow_path}" content to "{remote_path}"'
            raise StorageError(msg) from e
        return True

    def upload_named_collections_create_statement(
        self, backup_name: str, nc_name: str
    ) -> None:
        """
        Upload named collection create statement file.
        """
        local_path = os.path.join(
            self._named_collections_path, f"{escape_metadata_file_name(nc_name)}.sql"
        )
        remote_path = _named_collections_data_path(
            self.get_backup_path(backup_name), nc_name
        )
        try:
            logging.debug('Uploading named collection create statement "{}"', nc_name)
            self._storage_loader.upload_file(
                local_path, remote_path=remote_path, encryption=True
            )
        except Exception as e:
            msg = f"Failed to create async upload of {remote_path}"
            raise StorageError(msg) from e

    def get_udf_create_statement(
        self, backup_meta: BackupMetadata, filename: str
    ) -> str:
        """
        Download user defined function create statement.
        """
        remote_path = _udf_data_path(backup_meta.path, filename)
        return self._storage_loader.download_data(remote_path, encryption=True)

    def get_local_nc_create_statement(self, nc_name: str) -> Optional[str]:
        """
        Read named collection create statement from local file.
        """
        local_path = os.path.join(
            self._named_collections_path, f"{escape_metadata_file_name(nc_name)}.sql"
        )
        try:
            return Path(local_path).read_bytes().decode("utf-8")
        except OSError as e:
            logging.debug(
                'Cannot load a create statement of the named collection "{}": {}',
                nc_name,
                str(e),
            )
            return None

    def get_named_collection_create_statement(
        self, backup_meta: BackupMetadata, filename: str
    ) -> str:
        """
        Download named collection create statement.
        """
        remote_path = _named_collections_data_path(backup_meta.path, filename)
        return self._storage_loader.download_data(remote_path, encryption=True)

    def get_backup_names(self) -> Sequence[str]:
        """
        Get names of existing backups.
        """
        return self._storage_loader.list_dir(
            self._config["path_root"], recursive=False, absolute=False
        )

    def _load_metadata(self, path: str, encryption: bool) -> BackupMetadata:
        try:
            data = self._storage_loader.download_data(path, encryption=encryption)
            return BackupMetadata.load_json(data)
        except CryptoError:
            raise
        except Exception as e:
            raise StorageError("Failed to download backup metadata") from e

    def get_backup(
        self, backup_name: str, use_light_meta: bool = False
    ) -> Optional[BackupMetadata]:
        """
        Download and return backup metadata.
        """
        path = (
            self._backup_light_metadata_path(backup_name)
            if use_light_meta
            else self._backup_metadata_path(backup_name)
        )

        if not self._storage_loader.path_exists(path):
            return None

        # New backup metadata is encrypted
        # Retry in case it is old and not encrypted
        try:
            return self._load_metadata(path, not use_light_meta)
        except CryptoError:
            return self._load_metadata(path, False)

    def get_backups(self, use_light_meta: bool = False) -> List[BackupMetadata]:
        """
        Return list of existing backups sorted by start_time in descent order.
        """
        logging.debug(
            "Collecting {} of existing backups",
            "light metadata" if use_light_meta else "metadata",
        )

        backups = []
        for name in self.get_backup_names():
            backup = self.get_backup(name, use_light_meta)
            if backup:
                backups.append(backup)

        return sorted(backups, key=lambda b: b.start_time.isoformat(), reverse=True)

    def reload_backup(
        self, backup: BackupMetadata, use_light_meta: bool = False
    ) -> BackupMetadata:
        """
        Reload backup metadata.
        """
        path = (
            self._backup_light_metadata_path(backup.name)
            if use_light_meta
            else self._backup_metadata_path(backup.name)
        )

        # New backup metadata is encrypted
        # Retry in case it is old and not encrypted
        try:
            return self._load_metadata(path, not use_light_meta)
        except CryptoError:
            return self._load_metadata(path, False)

    def get_database_create_statement(
        self, backup_meta: BackupMetadata, db_name: str
    ) -> str:
        """
        Download and return database create statement.
        """
        remote_path = _db_metadata_path(backup_meta.path, db_name)
        return self._storage_loader.download_data(remote_path, encryption=True)

    def write_database_metadata(self, db: Database, db_sql: str) -> None:
        """
        Write db sql to metadata file to prepare ATTACH query.
        """
        metadata_path = os.path.join(
            self._metadata_path, f"{escape_metadata_file_name(db.name)}.sql"
        )
        with open(metadata_path, "w", encoding="utf-8") as f:
            f.write(db_sql)

    def get_table_create_statement(
        self, backup_meta: BackupMetadata, db_name: str, table_name: str
    ) -> str:
        """
        Download and return table create statement.
        """
        remote_path = _table_metadata_path(backup_meta.path, db_name, table_name)
        return self._storage_loader.download_data(remote_path, encryption=True)

    def download_access_control_file(
        self, local_path: str, backup_name: str, file_name: str
    ) -> None:
        """
        Download access control object metadata and save on disk.
        """
        remote_path = _access_control_data_path(
            self.get_backup_path(backup_name), file_name
        )
        local_path = os.path.join(local_path, file_name)
        logging.debug(
            'Downloading access control metadata "{}" to "{}', remote_path, local_path
        )
        try:
            self._storage_loader.download_file(remote_path, local_path, encryption=True)
        except Exception as e:
            msg = f"Failed to download access control metadata file {remote_path}"
            raise StorageError(msg) from e

    def download_access_control(self, local_path: str, backup_name: str) -> None:
        """
        Download access control object metadata and save on disk.
        """
        remote_path = _access_control_data_path(
            self.get_backup_path(backup_name), ACCESS_CONTROL_FNAME
        )
        logging.debug(
            'Downloading access control metadata "{}" to "{}', remote_path, local_path
        )
        try:
            self._storage_loader.download_files(
                remote_path, local_path, encryption=True
            )
        except Exception as e:
            msg = f"Failed to download access control metadata file {remote_path}"
            raise StorageError(msg) from e

    def download_data_part(
        self, backup_meta: BackupMetadata, part: PartMetadata, fs_part_path: str
    ) -> None:
        """
        Download part data to the specified directory.
        """
        logging.debug(
            'Downloading data part {} of "{}"."{}"',
            part.name,
            part.database,
            part.table,
        )

        os.makedirs(fs_part_path, exist_ok=True)

        remote_dir_path = _part_path(
            part.link or backup_meta.path, part.database, part.table, part.name
        )

        if part.tarball:
            remote_path = os.path.join(remote_dir_path, f"{part.name}.tar")
            logging.debug("Downloading part tarball file: {}", remote_path)
            try:
                self._storage_loader.download_files(
                    remote_path=remote_path,
                    local_path=fs_part_path,
                    is_async=True,
                    encryption=True,
                )
            except Exception as e:
                msg = f"Failed to download part tarball file {remote_path}"
                raise StorageError(msg) from e
        else:
            for filename in part.files:
                local_path = os.path.join(fs_part_path, filename)
                remote_path = os.path.join(remote_dir_path, filename)
                try:
                    logging.debug("Downloading part file: {}", remote_path)
                    self._storage_loader.download_file(
                        remote_path=remote_path,
                        local_path=local_path,
                        is_async=True,
                        encryption=True,
                    )
                except Exception as e:
                    msg = f"Failed to download part file {remote_path}"
                    raise StorageError(msg) from e

    def check_data_part(self, backup_path: str, part: PartMetadata) -> bool:
        """
        Check availability of part data in storage.
        """
        try:
            remote_dir_path = _part_path(
                part.link or backup_path, part.database, part.table, part.name
            )
            remote_files = self._storage_loader.list_dir(remote_dir_path)

            if remote_files == [f"{part.name}.tar"]:
                actual_size = self._storage_loader.get_file_size(
                    os.path.join(remote_dir_path, f"{part.name}.tar")
                )
                target_size = self._target_part_size(part)
                if target_size != actual_size:
                    logging.warning(
                        f"Part {part.name} files stored in tar, size not match {target_size} != {actual_size}"
                    )
                    return False
                return True

            notfound_files = set(part.files) - set(remote_files)
            if notfound_files:
                logging.warning(
                    "Some part files were not found in {}: {}",
                    remote_dir_path,
                    ", ".join(notfound_files),
                )
                return False

            return True

        except S3RetryingError:
            logging.warning(
                f"Failed to check data part {part.name}, consider it's broken",
                exc_info=True,
            )
            return False

    def download_cloud_storage_metadata(
        self, backup_meta: BackupMetadata, disk: Disk, source_disk_name: str
    ) -> None:
        """
        Download files packed in tarball and unpacks them into specified directory.
        """
        backup_name = backup_meta.get_sanitized_name()
        compression = backup_meta.cloud_storage.compressed
        disk_path = os.path.join(disk.path, "shadow", backup_name)
        os.makedirs(disk_path, exist_ok=True)
        remote_path = _disk_metadata_path(
            self.get_backup_path(backup_name), source_disk_name, compression
        )

        logging.debug(f'Downloading "{disk_path}" files from "{remote_path}"')
        try:
            self._storage_loader.download_files(
                remote_path=remote_path,
                local_path=disk_path,
                is_async=True,
                encryption=backup_meta.cloud_storage.encrypted,
                compression=compression,
            )
        except Exception as e:
            msg = f'Failed to download tarball file "{remote_path}"'
            raise StorageError(msg) from e

    def delete_backup(self, backup_name: str) -> None:
        """
        Delete backup data and metadata from storage.
        """
        backup_path = self.get_backup_path(backup_name)

        logging.debug("Deleting data in {}", backup_path)

        deleting_files = self._storage_loader.list_dir(
            backup_path, recursive=True, absolute=True
        )
        self._delete_files(deleting_files)

    def delete_data_parts(
        self, backup_meta: BackupMetadata, parts: Sequence[PartMetadata]
    ) -> None:
        """
        Delete backup data parts from storage.
        """
        if not parts:
            return

        deleting_files: List[str] = []
        for part in parts:
            part_path = _part_path(
                part.link or backup_meta.path, part.database, part.table, part.name
            )
            logging.debug("Deleting data part {}", part_path)
            if part.tarball:
                deleting_files.append(os.path.join(part_path, f"{part.name}.tar"))
            else:
                deleting_files.extend(os.path.join(part_path, f) for f in part.files)

        self._delete_files(deleting_files)

    def wait(self, keep_going: bool = False) -> None:
        """
        Wait for completion of data upload and download.
        """
        try:
            logging.memory_usage()
            logging.debug(
                f"Waiting for completion of async operations with keep_going={keep_going}"
            )
            self._storage_loader.wait(keep_going)
        except Exception as e:
            raise StorageError("Failed to complete async operations") from e

    def get_backup_path(self, backup_name: str) -> str:
        """
        Get backup path by backup name.
        """
        return os.path.join(self._config["path_root"], backup_name)

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

    def _backup_light_metadata_path(self, backup_name: str) -> str:
        return os.path.join(self.get_backup_path(backup_name), BACKUP_LIGHT_META_FNAME)

    def _target_part_size(self, part: PartMetadata) -> int:
        """
        Predicts tar archive size after encryption.
        """
        tar_size = calc_tarball_size(list(part.raw_metadata.files), part.size)
        return calc_encrypted_size(
            tar_size, self._encryption_chunk_size, self._encryption_metadata_size
        )


def _access_control_data_path(backup_path: str, file_name: str) -> str:
    """
    Return S3 path to access control data.
    """
    return os.path.join(backup_path, "access_control", file_name)


def _udf_data_path(backup_path: str, udf_file: str) -> str:
    """
    Return S3 path to UDF data
    """
    return os.path.join(backup_path, "udf", udf_file)


def _db_metadata_path(backup_path: str, db_name: str) -> str:
    """
    Return S3 path to database metadata.
    """
    return os.path.join(backup_path, "metadata", _quote(db_name) + ".sql")


def _table_metadata_path(backup_path: str, db_name: str, table_name: str) -> str:
    """
    Return S3 path to table metadata.
    """
    return os.path.join(
        backup_path, "metadata", _quote(db_name), _quote(table_name) + ".sql"
    )


def _named_collections_data_path(backup_path: str, nc_name: str) -> str:
    """
    Return S3 path to named collections.
    """
    return os.path.join(backup_path, "named_collections", _quote(nc_name) + ".sql")


def _part_path(backup_path: str, db_name: str, table_name: str, part_name: str) -> str:
    """
    Return S3 path to data part.
    """
    return os.path.join(backup_path, "data", db_name, table_name, part_name)


def _disk_metadata_path(
    backup_path: str, disk_name: str, compressed: bool = False
) -> str:
    """
    Returns path to store tarball with cloud storage shadow metadata.
    """
    extension = ".tar"
    if compressed:
        extension += COMPRESSED_EXTENSION
    return os.path.join(backup_path, "disks", f"{disk_name}{extension}")


def _quote(value: str) -> str:
    return quote(value, safe="").translate(
        {
            ord("."): "%2E",
            ord("-"): "%2D",
        }
    )
