"""Restore staging for encrypted object storage data."""

import os
from pathlib import Path
from typing import Iterator, Literal, Sequence

from ch_backup import logging
from ch_backup.backup.layout import BackupLayout
from ch_backup.backup.metadata import BackupMetadata, TableMetadata
from ch_backup.clickhouse.object_storage_metadata import (
    FROZEN_METADATA_FNAME,
    ObjectStorageMetadataError,
    parse_object_storage_metadata,
    replace_object_keys,
)
from ch_backup.config import Config
from ch_backup.exceptions import (
    ClickhouseBackupError,
    ConfigurationError,
    TerminatingSignal,
    is_broken_executor,
)
from ch_backup.storage import StorageLoader
from ch_backup.storage.engine import get_storage_engine
from ch_backup.storage.object_copy import (
    CopyEntry,
    get_s3_disk_location,
    make_storage_config,
    storage_id,
)
from ch_backup.util import chunked

DEFAULT_DECRYPTED_SUFFIX = "_decrypted"


class CloudStorageDecryption:
    """Decrypt copied object storage data into restore staging."""

    def __init__(self, config: Config | dict, backup_layout: BackupLayout) -> None:
        self._layout = backup_layout
        self._storage_loader = StorageLoader(config)
        self._cleanup_plans: list[tuple[str, dict, str]] = []

    # pylint: disable=too-many-positional-arguments,too-many-locals
    def decrypt(
        self,
        backup: BackupMetadata,
        disk_name: str,
        desired_tables: Sequence[TableMetadata] | Literal["all"],
        source_storage: dict,
        source_path: str,
        staging_storage: dict,
        staging_path: str,
        keep_going: bool = False,
    ) -> None:
        """Decrypt objects referenced by disk metadata into staging."""
        source_path = _normalize_prefix(source_path)
        staging_path = _normalize_prefix(staging_path)
        entries: dict[str, CopyEntry] = {}
        remote_paths = self._layout.get_cloud_storage_metadata_remote_paths(
            backup.get_sanitized_name(),
            disk_name,
            backup.cloud_storage.compressed,
            desired_tables,
        )
        for remote_path in remote_paths:
            for file_name, data in self._layout.get_cloud_storage_metadata_files(
                backup, remote_path
            ):
                if os.path.basename(file_name) == FROZEN_METADATA_FNAME:
                    continue
                try:
                    metadata = parse_object_storage_metadata(data)
                except ObjectStorageMetadataError as e:
                    raise ClickhouseBackupError(
                        f'Failed to parse metadata file "{file_name}" '
                        f'from "{remote_path}": {e}'
                    ) from e

                for obj in metadata.objects:
                    relative_key = _object_relative_key(
                        obj.key, source_path, metadata.full_object_keys
                    )
                    entry = CopyEntry(
                        source_storage,
                        staging_storage,
                        (
                            obj.key
                            if metadata.full_object_keys
                            else os.path.join(source_path, relative_key)
                        ),
                        os.path.join(staging_path, relative_key),
                        obj.size,
                        obj.size,
                        decrypt=True,
                    )
                    previous = entries.setdefault(entry.dst_path, entry)
                    if (
                        storage_id(previous.src_storage),
                        previous.src_path,
                        previous.size,
                    ) != (
                        storage_id(entry.src_storage),
                        entry.src_path,
                        entry.size,
                    ):
                        raise ClickhouseBackupError(
                            f'Multiple encrypted objects map to staging key "{entry.dst_path}"'
                        )

        logging.info(
            "Decrypting {} cloud storage objects of disk {} into restore staging",
            len(entries),
            disk_name,
        )
        self._decrypt_entries(entries, disk_name, keep_going)

    def register_cleanup(
        self, metadata_root: str, staging_storage: dict, staging_path: str
    ) -> None:
        """Remember where to find exact staging keys after restore."""
        self._cleanup_plans.append(
            (metadata_root, staging_storage, _normalize_prefix(staging_path))
        )

    def cleanup(self) -> None:
        """Remove plaintext staging objects after a completed restore."""
        for metadata_root, storage, staging_path in self._cleanup_plans:
            engine = get_storage_engine(storage)
            for batch in chunked(
                _staging_object_paths(metadata_root, staging_path),
                storage["bulk_delete_chunk_size"],
            ):
                paths = list(dict.fromkeys(batch))
                logging.info(
                    "Removing {} decrypted cloud storage staging objects", len(paths)
                )
                engine.delete_files(paths)
        self._cleanup_plans.clear()

    def _decrypt_entries(
        self, entries: dict[str, CopyEntry], disk_name: str, keep_going: bool
    ) -> None:
        """Run decryption jobs and drain already submitted jobs on failure."""
        failures = 0

        def failed(_error: BaseException) -> None:
            nonlocal failures
            failures += 1

        index = 0
        try:
            for index, entry in enumerate(entries.values(), 1):
                try:
                    self._storage_loader.copy_object(
                        entry, is_async=True, on_error=failed
                    )
                except (Exception, TerminatingSignal) as e:
                    if (
                        not keep_going
                        or not isinstance(e, Exception)
                        or is_broken_executor(e)
                    ):
                        raise
                if index % 256 == 0:
                    self._storage_loader.wait(keep_going=keep_going)
            if index % 256:
                self._storage_loader.wait(keep_going=keep_going)
        except (Exception, TerminatingSignal):
            try:
                self._storage_loader.wait(keep_going=True)
            except (Exception, TerminatingSignal):
                logging.exception("Failed to drain cloud storage decryption jobs")
            raise

        if failures:
            if not keep_going:
                raise ClickhouseBackupError(
                    f"Failed to decrypt {failures} cloud storage objects of disk {disk_name}"
                )
            logging.warning(
                "Failed to decrypt {} of {} cloud storage objects of disk {}; "
                "restore continues with keep-going",
                failures,
                len(entries),
                disk_name,
            )

    @staticmethod
    def rewrite_metadata(
        metadata_root: str, source_path: str, staging_path: str
    ) -> None:
        """Point full object keys in downloaded metadata to decryption staging."""
        source_path = _normalize_prefix(source_path)
        staging_path = _normalize_prefix(staging_path)
        if source_path == staging_path:
            return

        for file_path in Path(metadata_root).rglob("*"):
            if not file_path.is_file() or file_path.name == FROZEN_METADATA_FNAME:
                continue
            try:
                data = file_path.read_bytes()
                metadata = parse_object_storage_metadata(data)
                if not metadata.full_object_keys:
                    continue
                keys = [
                    os.path.join(
                        staging_path,
                        _object_relative_key(obj.key, source_path, full_key=True),
                    )
                    for obj in metadata.objects
                ]
                file_path.write_bytes(replace_object_keys(data, keys))
            except ObjectStorageMetadataError as e:
                raise ClickhouseBackupError(
                    f'Failed to rewrite cloud storage metadata file "{file_path}"'
                ) from e


def resolve_decryption_staging(
    config: Config | dict,
    source_storage: dict,
    source_path: str,
    destination_disk: dict,
) -> tuple[dict, str]:
    """Return storage and key prefix for plaintext cloud storage objects."""
    disk_location = get_s3_disk_location(destination_disk)
    if disk_location is None:
        raise ConfigurationError(
            "Failed to determine decryption staging location from ClickHouse disk; "
            "set restore.cloud_storage.decryption_staging explicitly"
        )
    disk_endpoint, disk_bucket, _ = disk_location

    staging_config = config["restore"]["cloud_storage"]["decryption_staging"]
    endpoint_url = staging_config.get("endpoint_url") or disk_endpoint
    bucket = staging_config.get("bucket") or disk_bucket
    if staging_config.get("access_key_id"):
        access_key_id = staging_config["access_key_id"]
        secret_access_key = staging_config.get("secret_access_key")
    else:
        access_key_id = destination_disk.get("access_key_id")
        secret_access_key = destination_disk.get("secret_access_key")

    storage = make_storage_config(
        config["storage"],
        endpoint_url,
        bucket,
        access_key_id,
        secret_access_key,
    )
    source_path = _normalize_prefix(source_path)
    configured_path = staging_config.get("path")
    if configured_path is None:
        staging_path = source_path
        if _same_storage(source_storage, storage):
            staging_path = f"{source_path}{DEFAULT_DECRYPTED_SUFFIX}"
    else:
        staging_path = _normalize_prefix(configured_path)

    if _same_storage(source_storage, storage) and staging_path == source_path:
        raise ConfigurationError(
            "Decryption staging must not overwrite encrypted cloud storage data"
        )
    return storage, staging_path


def _same_storage(first: dict, second: dict) -> bool:
    first_id = storage_id(first)
    second_id = storage_id(second)
    return (
        first_id[0] == second_id[0]
        and str(first_id[1]).rstrip("/") == str(second_id[1]).rstrip("/")
        and first_id[2] == second_id[2]
    )


def _normalize_prefix(path: str) -> str:
    prefix = path.strip("/")
    if any(part in (".", "..") for part in prefix.split("/")):
        raise ConfigurationError(f'Unsafe decryption staging path "{path}"')
    return prefix


def _object_relative_key(key: str, source_path: str, full_key: bool) -> str:
    if (
        not key
        or key.startswith("/")
        or any(part in (".", "..") for part in key.split("/"))
    ):
        raise ClickhouseBackupError(f'Unsafe cloud storage object key "{key}"')
    if not full_key:
        return key
    if not source_path:
        return key
    prefix = f"{source_path}/"
    if not key.startswith(prefix):
        raise ClickhouseBackupError(
            f'Cloud storage object key "{key}" is outside source prefix "{source_path}"'
        )
    return key[len(prefix) :]


def _staging_object_paths(metadata_root: str, staging_path: str) -> Iterator[str]:
    """Yield exact staging keys referenced by downloaded disk metadata."""
    for file_path in Path(metadata_root).rglob("*"):
        if not file_path.is_file() or file_path.name == FROZEN_METADATA_FNAME:
            continue
        try:
            metadata = parse_object_storage_metadata(file_path.read_bytes())
        except ObjectStorageMetadataError as e:
            raise ClickhouseBackupError(
                f'Failed to read cloud storage metadata file "{file_path}" for cleanup'
            ) from e
        for obj in metadata.objects:
            relative_key = _object_relative_key(
                obj.key, staging_path, metadata.full_object_keys
            )
            yield os.path.join(staging_path, relative_key)
