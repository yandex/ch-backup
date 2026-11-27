"""Restore staging for encrypted object storage data."""

import json
import os
from contextlib import ExitStack
from fcntl import LOCK_EX, LOCK_NB, flock
from hashlib import sha256
from pathlib import Path
from typing import Literal, Sequence
from uuid import UUID, uuid4

from ch_backup import logging
from ch_backup.backup.layout import BackupLayout
from ch_backup.backup.metadata import BackupMetadata, TableMetadata
from ch_backup.backup.metadata.cloud_storage_metadata import CloudStorageBackup
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
    defer_termination_signals,
    is_broken_executor,
)
from ch_backup.storage import StorageLoader
from ch_backup.storage.engine import get_storage_engine
from ch_backup.storage.object_copy import (
    CopyEntry,
    get_s3_disk_location,
    make_storage_config,
    physical_storage_id,
    storage_id,
)
from ch_backup.util import chunked

DEFAULT_DECRYPTED_SUFFIX = "_decrypted"


class CloudStorageDecryption:
    """Decrypt copied object storage data into restore staging."""

    def __init__(self, config: Config | dict, backup_layout: BackupLayout) -> None:
        self._lock_dir = Path(config["clickhouse"]["tmp_path"]) / "decryption-staging"
        self._locks = ExitStack()
        self._checked_buckets: set[tuple] = set()
        self._owned_roots: dict[tuple, str] = {}
        self._layout = backup_layout
        self._storage_loader = StorageLoader(config, use_threads=True)
        self._staging_prefixes: dict[tuple, tuple[dict, set[str]]] = {}

    def _check_staging_bucket(self, storage: dict) -> None:
        """Require permanent deletion before touching plaintext in this bucket."""
        identity = physical_storage_id(storage)
        if identity not in self._checked_buckets:
            get_storage_engine(storage).require_unversioned_staging()
            self._checked_buckets.add(identity)

    def allocate_staging(self, storage: dict, prefix: str) -> str:
        """Reclaim this worker's abandoned staging while excluding active restores."""
        key = (physical_storage_id(storage), _normalize_prefix(prefix))
        self._check_staging_bucket(storage)
        if key not in self._owned_roots:
            self._lock_dir.mkdir(parents=True, exist_ok=True)
            lock_path = self._lock_dir / sha256(json.dumps(key).encode()).hexdigest()
            lock = self._locks.enter_context(lock_path.open("a+", encoding="ascii"))
            try:
                flock(lock, LOCK_EX | LOCK_NB)
            except BlockingIOError as error:
                raise ConfigurationError(
                    "Decryption staging is already in use"
                ) from error
            lock.seek(0)
            token = lock.read()
            if not token:
                token = uuid4().hex
                lock.write(token)
                lock.flush()
                os.fsync(lock.fileno())
            root = os.path.join(prefix, UUID(token).hex)
            self._cleanup_prefix(storage, root)
            self._owned_roots[key] = root
        return os.path.join(self._owned_roots[key], uuid4().hex)

    # pylint: disable=too-many-positional-arguments,too-many-locals
    def decrypt(
        self,
        backup: BackupMetadata | CloudStorageBackup,
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
        validate_staging_sources(
            staging_storage, staging_path, [(source_storage, source_path)]
        )
        self._check_staging_bucket(staging_storage)
        staging = self._staging_prefixes.setdefault(
            storage_id(staging_storage), (staging_storage, set())
        )
        if staging_path not in staging[1]:
            if get_storage_engine(staging_storage).path_exists(
                staging_path, is_dir=True
            ):
                raise ConfigurationError(
                    "Decryption staging directory is already in use"
                )
            staging[1].add(staging_path)

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

    def cleanup(self) -> None:
        """Remove all plaintext objects under the registered staging prefixes."""
        with defer_termination_signals():
            self._storage_loader.finish_staging_writes()
            with self._locks, ExitStack() as cleanup:
                for storage, prefixes in self._staging_prefixes.values():
                    for prefix in prefixes:
                        cleanup.callback(self._cleanup_prefix, storage, prefix)
            self._staging_prefixes.clear()
            self._owned_roots.clear()

    @staticmethod
    def _cleanup_prefix(storage: dict, prefix: str) -> None:
        """Remove all objects under one exclusive staging prefix."""
        engine = get_storage_engine(storage)
        try:
            engine.abort_multipart_uploads(prefix)
        finally:
            paths = engine.list_dir(prefix, recursive=True, absolute=True)
            logging.info(
                "Removing {} decrypted cloud storage staging objects from {}",
                len(paths),
                prefix,
            )
            error = None
            for batch in chunked(paths, storage["bulk_delete_chunk_size"]):
                try:
                    engine.delete_files(batch)
                except Exception as exc:
                    error = error or exc
            if error is not None:
                raise error

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
    staging_config = config["restore"]["cloud_storage"]["decryption_staging"]
    disk_location = get_s3_disk_location(destination_disk)
    if disk_location is None:
        if not staging_config.get("endpoint_url") or not staging_config.get("bucket"):
            raise ConfigurationError(
                "Failed to determine decryption staging location from ClickHouse disk; "
                "set restore.cloud_storage.decryption_staging explicitly"
            )
        disk_endpoint = disk_bucket = None
    else:
        disk_endpoint, disk_bucket, _ = disk_location

    endpoint_url = staging_config.get("endpoint_url") or disk_endpoint
    bucket = staging_config.get("bucket") or disk_bucket
    if (
        not isinstance(endpoint_url, str)
        or not endpoint_url
        or not isinstance(bucket, str)
        or not bucket
    ):
        raise ConfigurationError("Invalid decryption staging endpoint or bucket")
    configured_access_key = staging_config.get("access_key_id")
    configured_secret_key = staging_config.get("secret_access_key")
    if bool(configured_access_key) != bool(configured_secret_key):
        raise ConfigurationError(
            "Decryption staging access_key_id and secret_access_key must be set together"
        )
    if (
        staging_config.get("endpoint_url") is not None
        or staging_config.get("bucket") is not None
    ) and not configured_access_key:
        raise ConfigurationError(
            "Decryption staging credentials are required when endpoint or bucket is overridden"
        )
    if configured_access_key:
        access_key_id = configured_access_key
        secret_access_key = configured_secret_key
    else:
        access_key_id = destination_disk.get("access_key_id")
        secret_access_key = destination_disk.get("secret_access_key")
    if bool(access_key_id) != bool(secret_access_key):
        raise ConfigurationError(
            "Resolved decryption staging access_key_id and secret_access_key "
            "must be set together"
        )

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

    if not staging_path:
        raise ConfigurationError("Decryption staging path must not be the bucket root")
    if _same_storage(source_storage, storage) and _prefixes_overlap(
        source_path, staging_path
    ):
        raise ConfigurationError(
            "Decryption staging path must not overlap encrypted cloud storage path"
        )
    return storage, staging_path


def validate_staging_sources(
    staging: dict, path: str, sources: Sequence[tuple[dict, str]]
) -> None:
    """Reject staging that overlaps any protected data on the same S3 service."""
    if any(
        _same_storage(storage, staging)
        and _prefixes_overlap(_normalize_prefix(source), _normalize_prefix(path))
        for storage, source in sources
    ):
        raise ConfigurationError("Decryption staging overlaps encrypted cloud storage")


def _same_storage(first: dict, second: dict) -> bool:
    return physical_storage_id(first) == physical_storage_id(second)


def _normalize_prefix(path: str) -> str:
    prefix = path.strip("/")
    if any(part in (".", "..") for part in prefix.split("/")):
        raise ConfigurationError(f'Unsafe decryption staging path "{path}"')
    return prefix


def _prefixes_overlap(first: str, second: str) -> bool:
    return (
        not first
        or not second
        or first == second
        or first.startswith(f"{second}/")
        or second.startswith(f"{first}/")
    )


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
