"""
Copying of backups from one storage to another.
"""

# pylint: disable=too-many-lines

import copy
import json
import os
from contextlib import closing, contextmanager
from datetime import datetime
from functools import lru_cache, partial
from pathlib import PurePosixPath
from tarfile import BLOCKSIZE
from typing import Callable, Iterator, NamedTuple, Sequence

from ch_backup import logging
from ch_backup.backup.checker_archive import cloud_part_member, cloud_table_paths
from ch_backup.backup.copy_context import (
    COPY_CONTEXT_DIR,
    CopyContext,
    FailedObjects,
    source_dir,
)
from ch_backup.backup.copy_deduplication import (
    CopyDeduplication,
    copy_deduplication_cutoff,
)
from ch_backup.backup.copy_index import CopyEntries, CopyIndex, EntryIndex
from ch_backup.backup.copy_report import BackupCopyResult, CopyReport
from ch_backup.backup.deduplication import StoredFiles, _part_data_exists
from ch_backup.backup.layout import (
    ACCESS_CONTROL_FNAME,
    BACKUP_LIGHT_META_FNAME,
    BACKUP_META_FNAME,
    BackupLayout,
    _db_metadata_path,
    _db_metadata_path_tar,
    _detached_part_path,
    _disk_metadata_path,
    _http_handlers_data_path,
    _named_collections_data_path,
    _table_metadata_path,
    _table_metadata_path_tar,
    _udf_data_path,
    _workload_entities_data_path,
    get_data_part_file_names,
    get_data_part_relative_dirs,
    read_metadata_in_batches,
)
from ch_backup.backup.metadata import (
    BackupMetadata,
    BackupState,
    CopyScope,
    CopyScopeStatus,
    DetachedPartMetadata,
    PartMetadata,
    TableMetadata,
    parse_copy_scopes,
    sanitize_backup_name,
)
from ch_backup.backup.metadata.cloud_storage_metadata import CloudStorageBackup
from ch_backup.backup.metadata.common import BackupStorageFormat
from ch_backup.calculators import calc_tarball_size
from ch_backup.clickhouse.config import ClickhouseConfig
from ch_backup.clickhouse.object_storage_metadata import (
    FROZEN_METADATA_FNAME,
    ObjectStorageMetadata,
    ObjectStorageMetadataError,
    StorageObject,
    parse_object_storage_metadata,
    replace_object_keys,
)
from ch_backup.config import Config
from ch_backup.encryption import (
    calc_encrypted_size_from_config,
    get_cloud_storage_data_encryption_config,
    get_cloud_storage_data_encryption_metadata,
    get_encryption,
    get_encryption_class,
)
from ch_backup.exceptions import (
    BackupNotFound,
    ClickhouseBackupError,
    ConfigurationError,
    StorageError,
    TerminatingSignal,
    is_broken_executor,
)
from ch_backup.logic.lock_manager import local_copy_locks
from ch_backup.storage.async_pipeline.base_pipeline.exec_pool import ThreadExecPool
from ch_backup.storage.engine import get_storage_engine
from ch_backup.storage.engine.base import PipeLineCompatibleStorageEngine
from ch_backup.storage.engine.s3 import S3RetryingError
from ch_backup.storage.metadata_archive import (
    MetadataArchiveWriter,
    MetadataFiles,
    TemporaryMetadataArchive,
)
from ch_backup.storage.object_copy import (
    CopyEntry,
    ServerSideCopyMode,
    get_s3_disk_location,
    make_storage_config,
    parse_server_side_copy_mode,
    physical_storage_id,
    server_side_copy_possible,
    storage_id,
)
from ch_backup.util import now, utcnow

REENCRYPT_CLAIM = "copy_encryption.json"


class _RewrittenTarball(NamedTuple):
    """A target disk metadata tarball and its rewritten files."""

    path: str
    files: MetadataFiles


class _ScopePlan:
    """
    Objects of a copy scope and target storage locations where they are put.

    Rewritten metadata holds tarballs with disk metadata that are uploaded with modified
    content instead of copying as is: (target path, files). A scope is planned if its
    planning succeeded.
    """

    def __init__(self) -> None:
        """Start an empty, unplanned copy scope."""
        self.entries = CopyEntries()
        self.rewritten_cloud_storage_metadata: list[_RewrittenTarball] = []
        self.planned = False

    def count(self) -> int:
        """Return the number of objects and rewritten metadata tarballs."""
        return len(self.entries) + len(self.rewritten_cloud_storage_metadata)


_CopyPlan = dict[CopyScope, _ScopePlan]


# pylint: disable=too-many-instance-attributes
class BackupCopier:
    """
    Copies backups from one storage to another.

    Backup data is copied as is (without decryption), except that ClickHouse object storage
    data can optionally be encrypted in transit. Backup metadata is uploaded to the target
    storage before data with the state "creating" and after with the state "created", so the
    copy becomes valid only when all its data is in place. Interrupted copying is resumed by
    the next run, which skips objects confirmed by the copy context and still present in the
    target storage.

    Copying is split into scopes: "files" copies files of the backup, "cloud_storage" copies
    data and metadata of object storage disks. Scopes may be copied separately, and the copy
    becomes "created" once every scope is confirmed.
    """

    # pylint: disable=too-many-arguments,too-many-positional-arguments
    def __init__(
        self,
        config: Config,
        target_config: Config,
        layout: BackupLayout,
        target_layout: BackupLayout,
        ch_config: ClickhouseConfig,
    ) -> None:
        """Initialize copy state and caches.

        Target table listings map (backup, database, table) to part, file and size,
        and are used to check links to incomplete copies.
        """
        self._config = config
        self._target_config = target_config
        self._layout = layout
        self._target_layout = target_layout
        self._ch_config = ch_config
        self._storage: dict = config["storage"]
        self._target_storage: dict = target_config["storage"]
        self._copy_config: dict = target_config["copy"]
        self._reencrypt = self._copy_config["reencrypt"]
        self._target_encryption: dict | None = None
        self._source_encryption: dict | None = None
        self._metadata_read_threads = target_config["multiprocessing"][
            "metadata_read_threads"
        ]
        self._encrypt_cloud_storage_data = bool(
            self._reencrypt or self._copy_config["cloud_storage"].get("encrypt_data")
        )
        self._cloud_storage_data_encryption = (
            get_cloud_storage_data_encryption_config(target_config)
            if self._encrypt_cloud_storage_data
            else None
        )

        self._engines: dict[tuple, PipeLineCompatibleStorageEngine] = {}
        # Metadata of backups in target storage used to resolve deduplication links.
        self._target_backups: dict[str, BackupMetadata | None] = {}
        self._target_table_files: StoredFiles = {}
        # Files of backups in source storage used to resolve deduplication links.
        self._source_backup_files: dict[str, CopyIndex] = {}
        # Original part identity -> successfully materialized target owner and name.
        self._materialized_parts: dict[tuple, tuple[str, str]] = {}
        self._target_deduplication: CopyDeduplication | None = None
        # Availability of server-side copy for bucket pairs with particular credentials.
        self._server_side_copy: dict[tuple, bool] = {}
        self._copy_context: CopyContext | None = None
        self._keep_going = False
        self._server_side_copy_mode = ServerSideCopyMode.AUTO
        self._scopes: tuple[CopyScope, ...] = tuple(CopyScope)
        self._report = CopyReport()

    def copy(
        self,
        backup_names: Sequence[str] | None = None,
        *,
        keep_going: bool = False,
        report: CopyReport | None = None,
        command_start: datetime | None = None,
    ) -> tuple[list[str], list[str]]:
        """
        Copy specified backups or all backups in the state "created" if names are not specified.

        Return names of copied backups and names of backups skipped as already copied.
        A copy with scopes left to copy is reported as partial and stays "creating".
        With keep_going, attempt every viable backup before raising an aggregate error.
        Copy scopes are taken from the target configuration, all of them by default.
        The server-side copy mode is taken from the target configuration.
        """
        self._keep_going = keep_going
        self._report = report if report is not None else CopyReport()
        self._report.error = None
        self._report.backups.clear()
        self._materialized_parts.clear()
        self._target_table_files.clear()
        try:
            self._validate_configs(utcnow() if command_start is None else command_start)
            backups = self._select_backups(backup_names)
            results = {result.name: result for result in self._report.backups}
            self._validate_backup_names(backups, results)

            copied, skipped = [], []
            for backup in backups:
                result = results[backup.name]
                if result.status == "failed":
                    continue
                try:
                    if self._copy_backup(backup.name, result):
                        result.status = (
                            "copied"
                            if result.backup_state == BackupState.CREATED.value
                            else "partial"
                        )
                        copied.append(backup.name)
                    else:
                        result.status = "skipped"
                        result.backup_state = BackupState.CREATED.value
                        skipped.append(backup.name)
                except (Exception, TerminatingSignal) as e:
                    if not result.errors:
                        result.add_error(e, "planning")
                    if (
                        not keep_going
                        or not isinstance(e, Exception)
                        or is_broken_executor(e)
                    ):
                        raise

            failed = [
                result.name
                for result in self._report.backups
                if result.status == "failed"
            ]
            if failed:
                raise ClickhouseBackupError(
                    f"Copy failed for {len(failed)} backup(s): {', '.join(failed)}"
                )
            return copied, skipped
        except BaseException as e:
            self._report.error = f"{type(e).__name__}: {e}"
            raise

    def _validate_backup_names(
        self, backups: Sequence[BackupMetadata], results: dict[str, BackupCopyResult]
    ) -> None:
        """
        Preserve the shared cloud-storage namespace check used when creating backups.
        """
        names = self._get_target_backup_names(results)
        for backup in backups:
            try:
                _validate_relative_key(backup.name)
                for disk_name in backup.cloud_storage.disks:
                    _validate_relative_key(disk_name)
                existing = names.setdefault(backup.get_sanitized_name(), backup.name)
                if existing != backup.name:
                    raise ClickhouseBackupError(
                        f"Backup with name {backup.name} conflicts with backup {existing} "
                        "in target storage: their sanitized names are equal"
                    )
                if (
                    backup.cloud_storage.requires_source_bucket
                    and CopyScope.CLOUD_STORAGE in self._scopes
                ):
                    self._get_target_cloud_storage()
            except Exception as e:
                results[backup.name].add_error(e, "validation")
                if not self._keep_going or is_broken_executor(e):
                    raise

    def _get_target_backup_names(
        self, results: dict[str, BackupCopyResult]
    ) -> dict[str, str]:
        """Reserve unreadable target names without blocking independent backups."""
        names = {}
        candidates = []
        readable_names = []
        for name in self._target_layout.get_backup_names():
            if self._target_layout.get_delete_pending(name) is not None:
                names[sanitize_backup_name(name)] = name
            else:
                readable_names.append(name)
        with ThreadExecPool(self._metadata_read_threads) as pool:
            for name, read in read_metadata_in_batches(
                self._target_layout,
                readable_names,
                pool=pool,
                workers=self._metadata_read_threads,
                use_light_meta=True,
            ):
                try:
                    backup = read.result()
                    if backup is None:
                        continue
                    if backup.name != name:
                        raise ClickhouseBackupError(
                            f'Target backup metadata name does not match "{name}"'
                        )
                    names[backup.get_sanitized_name()] = name
                    if (
                        self._target_deduplication is not None
                        and self._target_deduplication.eligible(backup)
                        and self._compatible_export_owner(backup)
                    ):
                        candidates.append(backup)
                except Exception as e:
                    if name not in results:
                        results[name] = BackupCopyResult(name)
                        self._report.backups.append(results[name])
                    results[name].add_error(e, "target_selection")
                    if not self._keep_going or is_broken_executor(e):
                        raise
                    names[sanitize_backup_name(name)] = name
        if self._target_deduplication is not None:
            self._target_deduplication.load(
                candidates,
                workers=self._target_config["multiprocessing"][
                    "full_metadata_read_threads"
                ],
            )
        return names

    @contextmanager
    def _planning_step(self, result: BackupCopyResult, source: str) -> Iterator[None]:
        """Collect local planning failures while retaining independent work."""
        try:
            yield
        except Exception as e:
            if not self._keep_going or is_broken_executor(e):
                raise
            result.add_error(e, "planning", source=source)

    def _validate_configs(self, command_start: datetime) -> None:
        """Validate copy settings before selecting backups or writing objects."""
        self._server_side_copy_mode = parse_server_side_copy_mode(
            self._copy_config["server_side_copy_mode"]
        )
        if (
            self._server_side_copy_mode == ServerSideCopyMode.STRICT
            and self._encrypt_cloud_storage_data
        ):
            mode = (
                "copy.reencrypt"
                if self._reencrypt
                else "copy.cloud_storage.encrypt_data"
            )
            raise ConfigurationError(
                f"Strict server-side copy is incompatible with {mode}"
            )
        self._scopes = parse_copy_scopes(self._copy_config["scopes"])
        if not isinstance(self._reencrypt, bool):
            raise ConfigurationError("copy.reencrypt must be a boolean")
        if self._reencrypt:
            self._validate_export_encryption()
        if not isinstance(self._copy_config["only_with_data"], bool):
            raise ConfigurationError("copy.only_with_data must be a boolean")
        cutoff = copy_deduplication_cutoff(self._copy_config, command_start)
        self._target_deduplication = (
            CopyDeduplication(
                self._target_layout,
                cutoff,
                command_start,
                validate_owner=(
                    self._published_export_owner if self._reencrypt else None
                ),
            )
            if cutoff is not None
            else None
        )
        if cutoff is not None:
            logging.info(
                "Copy target deduplication window lower bound: {}", cutoff.isoformat()
            )
        if storage_id(self._storage) == storage_id(self._target_storage) and _key(
            self._config["backup"]["path_root"]
        ).rstrip("/") == _key(self._target_config["backup"]["path_root"]).rstrip("/"):
            raise ConfigurationError("Source and target storages are the same")

        # Backup data is copied as is, so it must be readable with the target config.
        for section, keys in (
            ("encryption", ("type", "key", "chunk_size")),
            ("compression", ("type",)),
        ):
            for key in keys:
                if self._reencrypt and section == "encryption":
                    continue
                if self._config[section].get(key) != self._target_config[section].get(
                    key
                ):
                    raise ConfigurationError(
                        f'Config option "{section}.{key}" must be the same for source and target storages'
                    )

        if (
            self._cloud_storage_data_encryption is not None
            and not self._cloud_storage_data_encryption.get("key")
        ):
            raise ConfigurationError(
                "cloud_storage.data_encryption.key or encryption.key is required "
                "when copy.cloud_storage.encrypt_data is enabled"
            )

        if self._cloud_storage_data_encryption is not None:
            get_encryption(
                self._cloud_storage_data_encryption["type"],
                self._cloud_storage_data_encryption,
            )

    def _validate_export_encryption(self) -> None:
        """Require a distinct target key and an unchanged encryption format."""
        target = self._target_config["encryption"]
        cloud = get_cloud_storage_data_encryption_config(self._target_config)
        source = self._config["encryption"]
        configs = [target]
        if CopyScope.FILES in self._scopes:
            configs.append(source)
        for cfg in configs:
            chunk = cfg.get("chunk_size")
            key = cfg.get("key")
            if cfg.get("type") != "nacl" or not isinstance(key, str):
                raise ConfigurationError("Reencrypt requires NaCl encryption keys")
            try:
                crypto = get_encryption("nacl", cfg)
            except (UnicodeError, ValueError):
                raise ConfigurationError("Invalid NaCl encryption key") from None
            if (
                not isinstance(chunk, int)
                or isinstance(chunk, bool)
                or chunk <= 0
                or not isinstance(cfg.get("buffer_size"), int)
                or cfg["buffer_size"] < chunk + crypto.metadata_size()
            ):
                raise ConfigurationError("Invalid reencrypt chunk or buffer size")
        if any(
            cloud[field] != target[field] for field in ("key", "type", "chunk_size")
        ):
            raise ConfigurationError(
                "Cloud data encryption must use target encryption settings"
            )
        self._target_encryption = get_cloud_storage_data_encryption_metadata(target)
        self._source_encryption = None
        if CopyScope.FILES in self._scopes:
            if (
                source["chunk_size"] != target["chunk_size"]
                or source["key"] == target["key"]
            ):
                raise ConfigurationError(
                    "Reencrypt requires different keys and matching chunk sizes"
                )
            self._source_encryption = source

    def _select_backups(
        self, backup_names: Sequence[str] | None
    ) -> list[BackupMetadata]:
        """
        Select backups to copy, older ones first.

        Without the scope "files", copies in target storage are selected instead, so the source
        storage is not accessed: the specified ones, or all copies with the scope "cloud_storage"
        not completed, or the newest of them for "LAST".
        """
        staged = CopyScope.FILES not in self._scopes
        layout = self._target_layout if staged else self._layout
        last = staged and list(backup_names or ()) == ["LAST"]
        if last:
            backup_names = None
        backups = []
        names = (
            sorted(layout.get_backup_names()) if backup_names is None else backup_names
        )
        if backup_names is None:
            names = [name for name in names if layout.get_delete_pending(name) is None]
        results = {name: BackupCopyResult(name) for name in dict.fromkeys(names)}
        self._report.backups.extend(results.values())
        with ThreadExecPool(self._metadata_read_threads) as pool:
            for name, read in read_metadata_in_batches(
                layout,
                results,
                pool=pool,
                workers=self._metadata_read_threads,
                use_light_meta=True,
            ):
                result = results[name]
                try:
                    backup = read.result()
                    if backup_names is None and (
                        not backup
                        or (
                            not _ready_for_cloud_storage_copy(backup)
                            if staged
                            else backup.state != BackupState.CREATED
                        )
                        or (self._copy_config["only_with_data"] and backup.is_empty())
                    ):
                        self._report.backups.remove(result)
                        continue
                    if not backup:
                        raise (
                            _files_scope_required(name)
                            if staged
                            else BackupNotFound(name)
                        )
                    if not staged:
                        _check_backup_state(backup)
                    if backup.name != name:
                        raise ClickhouseBackupError(
                            f'Backup metadata name does not match "{name}"'
                        )
                    backups.append(backup)
                except Exception as e:
                    result.add_error(e, "selection")
                    if not self._keep_going or is_broken_executor(e):
                        raise

        # Older backups are copied first, so deduplicated data parts of newer backups
        # can be linked to the copies of older ones.
        backups.sort(key=lambda backup: backup.start_time)
        if last:
            backups = backups[-1:]
        self._report.backups[:] = [results[backup.name] for backup in backups] + [
            result for result in self._report.backups if result.status == "failed"
        ]
        return backups

    def _copy_backup(self, backup_name: str, result: BackupCopyResult) -> bool:
        """Keep the target metadata stable from the first read through publication."""
        held = {
            (physical_storage_id(self._target_storage), _key(path).rstrip("/"))
            for path in (
                self._target_layout.get_backup_path(backup_name),
                self._target_layout.get_cloud_storage_path(backup_name),
            )
        }
        with local_copy_locks(held):
            return self._copy_locked_backup(backup_name, result, held)

    # pylint: disable=too-many-branches,too-many-statements
    def _copy_locked_backup(
        self, backup_name: str, result: BackupCopyResult, held: set[tuple]
    ) -> bool:
        """
        Copy backup. Return False if the backup is already present in the target storage.

        A scope that is not copied is planned only to check it once it is completed, so it needs
        no settings or downloads before. Without data on object storage disks all scopes are planned,
        and the scope "cloud_storage" is completed right away.
        """
        self._source_backup_files.clear()
        self._target_backups.clear()
        target_backup = self._target_layout.get_backup(
            backup_name, use_light_meta=CopyScope.FILES in self._scopes
        )
        backup = self._get_source_backup(backup_name, target_backup)
        if backup.name != backup_name:
            raise ClickhouseBackupError(
                f'Backup metadata name does not match "{backup_name}"'
            )
        _validate_relative_key(backup.name)
        for disk_name in backup.cloud_storage.disks:
            _validate_relative_key(disk_name)
        target_data_encryption = (
            get_cloud_storage_data_encryption_metadata(
                self._cloud_storage_data_encryption
            )
            if self._cloud_storage_data_encryption and backup.cloud_storage.enabled
            else backup.cloud_storage.data_encryption
        )
        target_meta = self._make_target_metadata(backup)
        target_meta.copy_reencrypted = self._reencrypt
        published = self._resume_export_publication(backup, target_backup, target_meta)
        if target_backup:
            if (
                target_backup.start_time != backup.start_time
                or target_backup.hostname != backup.hostname
            ):
                raise ClickhouseBackupError(
                    f"Backup {backup_name} already exists in target storage and it is not a copy of the source backup"
                )
            if published:
                if (
                    not self._reencrypt
                    and target_backup.cloud_storage.data_encryption
                    != target_data_encryption
                ):
                    result.skip_reason = (
                        "already present in target storage with different cloud "
                        "storage data encryption"
                    )
                    logging.warning(
                        "Backup {} {}, skipping it",
                        backup.name,
                        result.skip_reason,
                    )
                if any(
                    part.link
                    and (
                        self._target_deduplication is None
                        or part.disk_name in backup.cloud_storage.disks
                    )
                    for part in backup.get_parts()
                ):
                    existing = self._target_layout.get_backup(backup_name)
                    if existing:
                        self._remember_materialized_parts(backup, existing)
                if result.skip_reason is None:
                    logging.info(
                        "Backup {} is already present in target storage, skipping it",
                        backup_name,
                    )
                return False
            logging.info(
                (
                    'Resuming copying of backup {} (state in target storage: "{}")'
                    if self._copy_config["resume"]
                    else 'Restarting copying of all objects of backup {} (state in target storage: "{}")'
                ),
                backup_name,
                target_backup.state.value,
            )
        else:
            logging.info("Copying backup {}", backup_name)

        if self._encrypt_cloud_storage_data:
            if backup.cloud_storage.data_copied and not self._reencrypt:
                raise ClickhouseBackupError(
                    f'Encrypting cloud storage data of backup "{backup.name}" created '
                    "with cloud_storage.copy_data is not supported"
                )
            if backup.cloud_storage.data_encryption is not None:
                raise ClickhouseBackupError(
                    f'Cloud storage data of backup "{backup.name}" is already encrypted'
                )

        if self._target_deduplication is not None:
            # A PARTIALLY_DELETED owner may be retried. If it fails, later backups
            # must not reuse the old index entry after its state changes to FAILED.
            self._target_deduplication.discard_backup(backup.name)
        self._copy_context = CopyContext(
            self._engine(self._target_storage),
            self._target_layout.get_backup_path(backup.name),
            backup,
            data_encryption=target_data_encryption,
            backup_encryption=target_meta.copy_encryption,
        )
        if target_backup and (
            self._copy_config["resume"] or set(CopyScope) - set(self._scopes)
        ):
            self._copy_context.load()

        previous = dict(target_backup.copy_scopes) if target_backup else {}
        plan: _CopyPlan = {scope: _ScopePlan() for scope in CopyScope}
        planners = {
            CopyScope.FILES: self._plan_files_scope,
            CopyScope.CLOUD_STORAGE: self._plan_cloud_storage_scope,
        }
        for scope, planner in planners.items():
            if (
                scope in self._scopes
                or previous.get(scope) == CopyScopeStatus.COMPLETED
                or not backup.cloud_storage.requires_source_bucket
            ):
                plan[scope].planned = self._plan_scope(
                    scope, result, partial(planner, backup, target_meta, plan[scope])
                )
        try:
            self._validate_destinations(backup, plan)
        except Exception as e:
            result.add_error(e, "validation")
            raise

        result.objects_total = sum(plan[scope].count() for scope in self._scopes)
        self._source_backup_files.clear()
        if self._reencrypt:
            if result.errors:
                raise ClickhouseBackupError(
                    f"Cannot export incomplete plan for backup {backup.name}"
                )
            target_meta.mark_export_encrypted()
        locations = {
            (physical_storage_id(storage), _key(prefix).rstrip("/"))
            for storage, prefix in self._target_prefixes(backup, plan)
        }
        with local_copy_locks(locations - held):
            self._claim_export(target_meta, backup, plan)
            self._transfer_backup(
                backup,
                target_meta,
                target_data_encryption,
                plan,
                previous,
                result,
            )
        return True

    def _export_claim_path(self, name: str) -> str:
        """Place the export claim in the backup directory so delete removes it."""
        return _key(
            os.path.join(self._target_layout.get_backup_path(name), REENCRYPT_CLAIM)
        )

    def _resume_export_publication(
        self,
        backup: BackupMetadata,
        previous: BackupMetadata | None,
        target: BackupMetadata,
    ) -> bool:
        """Check export compatibility and finish publishing a completed copy on retry."""
        if not self._reencrypt:
            if previous and (
                previous.copy_reencrypted
                or previous.copy_encryption != backup.copy_encryption
            ):
                raise ClickhouseBackupError("Target backup requires copy.reencrypt")
            return previous is not None and previous.state == BackupState.CREATED
        for part in backup.get_detached_parts():
            if part.object_storage:
                raise ClickhouseBackupError(
                    f"Cannot export detached S3 part {part.database}.{part.table}/{part.disk_name}/{part.name}"
                )
        if self._source_encryption is not None:
            source_identity = get_cloud_storage_data_encryption_metadata(
                self._source_encryption
            )
            if (
                backup.copy_encryption
                and backup.copy_encryption.get("target") != source_identity
            ):
                raise ClickhouseBackupError(
                    "Source backup encryption does not match the source key"
                )
            target.copy_encryption = {
                "version": 1,
                "source": source_identity,
                "target": self._target_encryption,
            }
        incompatible_previous = previous is not None and (
            not previous.copy_reencrypted
            or previous.copy_encryption != target.copy_encryption
            or previous.copied_from != target.copied_from
        )
        if (
            incompatible_previous
            or not target.copy_encryption
            or target.copy_encryption.get("target") != self._target_encryption
        ):
            raise ClickhouseBackupError(
                f"Conflicting encryption for target backup {backup.name}"
            )
        if previous is None:
            return False
        full = self._target_layout.get_backup(backup.name)
        if full is None:
            return False
        if (
            full.copy_encryption != target.copy_encryption
            or full.copied_from != target.copied_from
            or full.start_time != backup.start_time
            or full.hostname != backup.hostname
        ):
            raise ClickhouseBackupError(
                f"Conflicting full metadata for backup {backup.name}"
            )
        if full.state != BackupState.CREATED:
            return False
        if full.cloud_storage.enabled and (
            full.cloud_storage.data_encryption != self._target_encryption
            or not full.cloud_storage.encrypted
        ):
            raise ClickhouseBackupError(
                f"Conflicting cloud encryption for backup {backup.name}"
            )
        if (
            full.cloud_storage.requires_source_bucket
            and CopyScope.CLOUD_STORAGE in self._scopes
        ):
            storage, path = self._get_target_cloud_storage()
            expected = {
                "endpoint_url": storage["credentials"]["endpoint_url"],
                "bucket": storage["credentials"]["bucket"],
                "path": os.path.join(path, backup.name),
            }
            if full.cloud_storage.source != expected:
                raise ClickhouseBackupError(
                    f"Conflicting cloud destination for backup {backup.name}"
                )
        if not self._target_layout.export_is_published(full):
            self._target_layout.upload_backup_metadata(full, light_only=True)
        return True

    def _claim_export(
        self, target: BackupMetadata, backup: BackupMetadata, plan: _CopyPlan
    ) -> None:
        """Reserve the backup and its cloud location before publishing or transferring data."""
        path = self._export_claim_path(backup.name)
        engine = self._engine(self._target_storage)
        cloud = self._external_export_destination(backup, plan)
        identity = {
            "backup": [backup.name, backup.start_time_str, backup.hostname],
            "source": target.copied_from,
            "reencrypt": self._reencrypt,
            "encryption": target.copy_encryption,
        }
        local_claim = json.dumps(identity, sort_keys=True).encode()
        if self._reencrypt:
            self._require_empty_or_claimed(engine, path)
        if engine.path_exists(path) and engine.download_data(path) != local_claim:
            raise StorageError(
                "Conflicting copy parameters: target reserved for export"
            )
        if cloud:
            storage, prefix = cloud
            location_bytes = json.dumps((storage_id(storage), prefix)).encode()
            if (
                self._reencrypt
                and engine.path_exists(path + ".cloud")
                and engine.download_data(path + ".cloud") != location_bytes
            ):
                raise ClickhouseBackupError("Conflicting target cloud destination")
            self._claim_cold_prefix(storage, prefix, path, identity)
        if self._reencrypt:
            engine.claim_object(path, local_claim)
            if cloud:
                engine.claim_object(path + ".cloud", location_bytes)

    def _claim_cold_prefix(
        self, storage: dict, prefix: str, path: str, identity: dict
    ) -> None:
        """Bind a cold directory object to the primary backup that may write its children."""
        engine = self._engine(storage)
        owner = json.dumps([physical_storage_id(self._target_storage), path]).encode()
        marker = prefix + "/"
        if engine.path_exists(marker):
            engine.claim_object(marker, owner)
            return
        with local_copy_locks(
            {("cold-allocation", physical_storage_id(storage))}, persistent=True
        ):
            for ancestor in PurePosixPath(prefix).parents:
                if str(ancestor) == ".":
                    break
                folder = str(ancestor) + "/"
                if engine.path_exists(folder) and engine.get_object_size(folder):
                    raise StorageError(
                        f'Cold prefix "{prefix}" overlaps an occupied directory "{ancestor}"'
                    )
            page, continuation = engine.read_listing_page(prefix)
            if (page or continuation) and not engine.path_exists(marker):
                legacy_marker = os.path.join(prefix, REENCRYPT_CLAIM)
                legacy_claim = json.dumps(
                    {
                        **identity,
                        "target_storage": storage_id(self._target_storage),
                        "target_path": path,
                    },
                    sort_keys=True,
                ).encode()
                primary = self._engine(self._target_storage)
                if not (
                    primary.path_exists(path)
                    and primary.download_data(path)
                    == json.dumps(identity, sort_keys=True).encode()
                    and engine.path_exists(legacy_marker)
                    and engine.get_object_size(legacy_marker) == len(legacy_claim)
                    and engine.download_data(legacy_marker) == legacy_claim
                ):
                    raise ClickhouseBackupError(
                        f'Target prefix "{prefix}" already contains data without matching ownership'
                    )
            engine.claim_object(marker, owner)

    @staticmethod
    def _require_empty_or_claimed(
        engine: PipeLineCompatibleStorageEngine, marker: str
    ) -> None:
        """Refuse to adopt objects left under an unclaimed export prefix."""
        if not engine.path_exists(marker):
            page, continuation = engine.read_listing_page(os.path.dirname(marker))
            if (page or continuation) and not engine.path_exists(marker):
                raise ClickhouseBackupError(
                    f'Target prefix of "{marker}" already contains data'
                )

    def _external_export_destination(
        self, backup: BackupMetadata, plan: _CopyPlan
    ) -> tuple[dict, str] | None:
        """Resolve the physical cold-data claim only when planning external writes."""
        if (
            not backup.cloud_storage.requires_source_bucket
            or not plan[CopyScope.CLOUD_STORAGE].entries
        ):
            return None
        storage, prefix = self._get_target_cloud_storage()
        return storage, os.path.join(prefix, backup.name)

    def _raw_export(self, backup: BackupMetadata) -> bool:
        """Identify a bytewise copy whose payload has its own key identity."""
        return not self._reencrypt and backup.copy_encryption is not None

    def _compatible_export_owner(self, backup: BackupMetadata) -> bool:
        """Reuse only published owners encrypted with the same target key."""
        if not self._reencrypt:
            return backup.copy_encryption is None
        return (
            backup.state == BackupState.CREATED
            and backup.copy_encryption is not None
            and backup.copy_encryption.get("target") == self._target_encryption
            and (
                not backup.cloud_storage.data_copied
                or backup.cloud_storage.data_encryption == self._target_encryption
            )
        )

    def _published_export_owner(self, backup: BackupMetadata) -> bool:
        """Verify an owner's full metadata against the other publication records."""
        return self._compatible_export_owner(
            backup
        ) and self._target_layout.export_is_published(backup)

    def _get_source_backup(
        self, backup_name: str, target_backup: BackupMetadata | None
    ) -> BackupMetadata:
        """
        Return metadata of the source backup.

        Without the scope "files", use the copy's full metadata, which retains the source
        cloud storage location until the copy is published, so source storage is not accessed.
        """
        if (
            target_backup is None
            and self._target_layout.get_cloud_storage_backup(backup_name) is not None
        ):
            raise ClickhouseBackupError(
                f"Backup {backup_name} has cloud data but no readable metadata; delete it before copying"
            )
        if CopyScope.FILES in self._scopes:
            backup = self._layout.get_backup(backup_name)
            if not backup:
                raise BackupNotFound(backup_name)
            _check_backup_state(backup)
            return backup
        if target_backup and target_backup.state == BackupState.CREATED:
            return target_backup
        if not target_backup or not _files_copied(target_backup):
            raise _files_scope_required(backup_name)
        return target_backup

    def _plan_scope(
        self, scope: CopyScope, result: BackupCopyResult, planner: Callable
    ) -> bool:
        """
        Plan objects of the copy scope. Return False if planning failed.

        Failures of a scope that is not copied by this run are only logged:
        the scope is not confirmed then, so the copy is not published.
        """
        if scope in self._scopes or (self._reencrypt and scope == CopyScope.FILES):
            errors = len(result.errors)
            planner(result)
            return len(result.errors) == errors
        scope_result = BackupCopyResult(result.name)
        try:
            planner(scope_result)
        except Exception as e:
            if is_broken_executor(e):
                raise
            scope_result.add_error(e, "planning")
        for error in scope_result.errors:
            logging.warning(
                "Failed to check scope {} of copying of backup {}: {}",
                scope.value,
                result.name,
                error["message"],
            )
        return not scope_result.errors

    def _plan_files_scope(
        self,
        backup: BackupMetadata,
        target_meta: BackupMetadata,
        plan: _ScopePlan,
        result: BackupCopyResult,
    ) -> None:
        """
        Plan copying of files of the backup, its deduplicated data parts and data of object
        storage disks copied into the backup.

        Without the scope "files", objects saved by the run that copied it are checked instead,
        so the source storage is not accessed.
        """
        if CopyScope.FILES not in self._scopes:
            assert self._copy_context is not None
            plan.entries = self._copy_context.load_manifest(CopyScope.FILES)
            return
        self._plan_backup_files(backup, plan, result)
        self._plan_linked_parts(backup, target_meta, plan, result)
        if backup.cloud_storage.data_copied:
            self._plan_full_cloud_storage_objects(backup, target_meta, plan, result)

    def _plan_cloud_storage_scope(
        self,
        backup: BackupMetadata,
        _target_meta: BackupMetadata,
        plan: _ScopePlan,
        result: BackupCopyResult,
    ) -> None:
        """Plan copying of data of object storage disks kept in their buckets."""
        if backup.cloud_storage.requires_source_bucket:
            with self._planning_step(result, backup.name):
                self._plan_cloud_storage_objects(backup, plan, result)

    # pylint: disable=too-many-arguments,too-many-positional-arguments
    def _transfer_backup(
        self,
        backup: BackupMetadata,
        target_meta: BackupMetadata,
        target_data_encryption: dict | None,
        plan: _CopyPlan,
        previous: dict[CopyScope, CopyScopeStatus],
        result: BackupCopyResult,
    ) -> None:
        """
        Copy selected scopes, publishing CREATED only when every scope is confirmed.

        Statuses of scopes saved by previous runs are not trusted. Failures during cleanup
        are preserved.
        """
        if backup.cloud_storage.enabled:
            self._target_layout.claim_cloud_storage(backup.name)
        files = plan[CopyScope.FILES].entries
        self._leave_out_confirmed_objects(backup, plan, result)
        statuses = self._scope_statuses(backup.name, plan, previous)
        if (
            self._reencrypt
            and CopyScope.FILES not in self._scopes
            and statuses[CopyScope.FILES] != CopyScopeStatus.COMPLETED
        ):
            raise ClickhouseBackupError(
                "Export files are incomplete; repeat the files scope"
            )
        stage = "metadata"
        try:
            self._upload_target_metadata(target_meta, result, statuses)
            if (
                CopyScope.FILES in self._scopes
                and plan[CopyScope.FILES].planned
                and backup.cloud_storage.requires_source_bucket
            ):
                assert self._copy_context is not None
                target_meta.files_manifest = self._copy_context.save_manifest(
                    CopyScope.FILES, files
                )
            stage = "copy"
            for scope in self._scopes:
                statuses[scope] = CopyScopeStatus.FAILED
                if self._copy_scope(
                    scope,
                    target_meta if self._reencrypt else backup,
                    plan[scope],
                    result,
                    previous.get(scope) == CopyScopeStatus.FAILED,
                ):
                    statuses[scope] = CopyScopeStatus.COMPLETED
            if result.errors:
                raise ClickhouseBackupError(
                    f"Failed to copy backup {backup.name}: {len(result.errors)} error(s), "
                    f"{result.objects_failed} object transfer(s) failed"
                )
            stage = "metadata"
            if all(status == CopyScopeStatus.COMPLETED for status in statuses.values()):
                target_meta.cloud_storage.data_encryption = target_data_encryption
                target_meta.state = BackupState.CREATED
            self._upload_target_metadata(target_meta, result, statuses)
            self._remember_materialized_parts(backup, target_meta)
            if (
                self._target_deduplication is not None
                and self._compatible_export_owner(target_meta)
            ):
                self._target_deduplication.add_backup(target_meta, published=True)
            if target_meta.state == BackupState.CREATED:
                logging.info("Backup {} is copied", backup.name)
            else:
                logging.info(
                    "Scopes {} of backup {} are copied, other scopes are pending",
                    ", ".join(scope.value for scope in self._scopes),
                    backup.name,
                )
        except (Exception, TerminatingSignal) as e:
            logging.critical("Copying of backup {} failed", backup.name, exc_info=True)
            if stage == "metadata" or not result.errors:
                result.add_error(e, stage)
            target_meta.state = BackupState.FAILED
            target_meta.cloud_storage.source = backup.cloud_storage.source
            target_meta.cloud_storage.data_encryption = (
                backup.cloud_storage.data_encryption
            )
            target_meta.exception = result.errors[0]["message"]
            try:
                self._upload_target_metadata(target_meta, result, statuses)
            except (Exception, TerminatingSignal) as metadata_error:
                logging.exception(
                    "Saving FAILED metadata for backup {} failed", backup.name
                )
                result.add_error(metadata_error, "metadata")
                if not isinstance(metadata_error, Exception):
                    raise
                if is_broken_executor(metadata_error):
                    raise e from metadata_error
            raise
        finally:
            self._target_backups.pop(backup.name, None)

    def _upload_target_metadata(
        self,
        target_meta: BackupMetadata,
        result: BackupCopyResult,
        statuses: dict[CopyScope, CopyScopeStatus],
    ) -> None:
        """Save scope statuses, switching the cloud source only for a completed copy."""
        if (
            target_meta.state == BackupState.CREATED
            and target_meta.cloud_storage.requires_source_bucket
        ):
            storage, path = self._get_target_cloud_storage()
            target_meta.cloud_storage.source = {
                "endpoint_url": storage["credentials"]["endpoint_url"],
                "bucket": storage["credentials"]["bucket"],
                "path": os.path.join(path, target_meta.name),
            }
        scopes = {scope.value: status.value for scope, status in statuses.items()}
        target_meta.copy_scopes = dict(statuses)
        target_meta.copy_updated_at = now()
        result.scopes = dict(scopes)
        self._target_layout.upload_backup_metadata(target_meta)
        result.backup_state = target_meta.state.value

    def _scope_statuses(
        self,
        backup_name: str,
        plan: _CopyPlan,
        previous: dict[CopyScope, CopyScopeStatus],
    ) -> dict[CopyScope, CopyScopeStatus]:
        """
        Return statuses of scopes before copying.

        A scope that is not copied by this run is completed only if all its objects are confirmed.
        """
        statuses = {}
        for scope in CopyScope:
            if scope in self._scopes:
                statuses[scope] = CopyScopeStatus.PENDING
            elif plan[scope].planned and not plan[scope].count():
                statuses[scope] = CopyScopeStatus.COMPLETED
            else:
                if previous.get(scope) == CopyScopeStatus.COMPLETED:
                    logging.warning(
                        "Completed scope {} of backup {} is not confirmed by the copy context "
                        "and target storage, the scope must be copied again",
                        scope.value,
                        backup_name,
                    )
                statuses[scope] = (
                    CopyScopeStatus.FAILED
                    if previous.get(scope) == CopyScopeStatus.FAILED
                    else CopyScopeStatus.PENDING
                )
        return statuses

    def _target_prefixes(
        self, backup: BackupMetadata, plan: _CopyPlan
    ) -> list[tuple[dict, str]]:
        """
        Return storages and paths that hold all planned objects of the copied backup.

        The target location of object storage data is needed only when its objects are planned.
        """
        prefixes = [
            (self._target_storage, self._target_layout.get_backup_path(backup.name)),
            (
                self._target_storage,
                self._target_layout.get_cloud_storage_path(backup.name),
            ),
        ]
        if (
            backup.cloud_storage.requires_source_bucket
            and plan[CopyScope.CLOUD_STORAGE].entries
        ):
            storage, path = self._get_target_cloud_storage()
            prefixes.append((storage, os.path.join(path, backup.name)))
        return prefixes

    def _validate_destinations(self, backup: BackupMetadata, plan: _CopyPlan) -> None:
        """Check the entire plan before publishing metadata or copying any objects."""
        prefixes = self._target_prefixes(backup, plan)
        for _, prefix in prefixes:
            _validate_relative_key(_key(prefix).rstrip("/"))

        def validate(storage: dict, path: str) -> None:
            """Reject destinations outside the expected target prefixes."""
            key = _key(path)
            _validate_relative_key(key)
            if not any(
                storage_id(storage) == storage_id(target_storage)
                and key.startswith(_key(prefix).rstrip("/") + "/")
                for target_storage, prefix in prefixes
            ):
                raise ClickhouseBackupError(
                    f'Copy destination "{path}" is outside the target backup prefixes'
                )

        destinations = CopyIndex() if self._reencrypt else None
        if self._reencrypt:
            if len(prefixes) == 3 and any(
                _locations_overlap(*prefixes[-1], storage, path)
                for storage, path in prefixes[:-1]
            ):
                raise ClickhouseBackupError("Cloud export overlaps the target backup")
            if CopyScope.FILES in self._scopes:
                source_paths = (
                    self._layout.get_backup_path(backup.name),
                    self._layout.get_cloud_storage_path(backup.name),
                )
                for storage, target in prefixes:
                    if any(
                        _locations_overlap(storage, target, self._storage, path)
                        for path in source_paths
                    ):
                        raise ClickhouseBackupError(
                            f'Target prefix "{target}" overlaps the source backup'
                        )
        for scope_plan in plan.values():
            continuation = None
            while True:
                batch, continuation = scope_plan.entries.read_batch(
                    continuation, limit=128
                )
                for entry in batch:
                    validate(entry.dst_storage, entry.dst_path)
                    if destinations is not None:
                        self._validate_export_entry(entry, prefixes, destinations)
                if continuation is None:
                    break
            for path, _ in scope_plan.rewritten_cloud_storage_metadata:
                validate(self._target_storage, path)
                if destinations is not None:
                    _reserve_copy_destination(destinations, self._target_storage, path)

    @staticmethod
    def _validate_export_entry(
        entry: CopyEntry, prefixes: Sequence[tuple[dict, str]], destinations: CopyIndex
    ) -> None:
        """Reject overwrites of source data and duplicate destination paths."""
        for storage, prefix in prefixes:
            if _locations_overlap(storage, prefix, entry.src_storage, entry.src_path):
                raise ClickhouseBackupError(
                    f'Target prefix "{prefix}" overlaps source "{entry.src_path}"'
                )
        _reserve_copy_destination(destinations, entry.dst_storage, entry.dst_path)

    def _make_target_metadata(self, backup: BackupMetadata) -> BackupMetadata:
        """Prepare creating metadata, preserving the original source of a staged copy."""
        target_meta = copy.deepcopy(backup)
        # DEPRECATED: ``path`` is populated for backward compatibility
        # with older ch-backup versions and is not used by new code.
        target_meta.path = self._target_layout.get_backup_path(backup.name)
        if CopyScope.FILES in self._scopes:
            target_meta.copied_from = {
                "endpoint_url": self._storage["credentials"]["endpoint_url"],
                "bucket": self._storage["credentials"]["bucket"],
                "path_root": self._config["backup"]["path_root"],
            }
        target_meta.state = BackupState.CREATING
        target_meta.exception = None
        return target_meta

    def _plan_backup_files(
        self, backup: BackupMetadata, plan: _ScopePlan, result: BackupCopyResult
    ) -> None:
        """
        Plan copying of all files from the backup directory except metadata files.

        Disk metadata is planned by the scope "cloud_storage". With data in buckets of object
        storage disks, original disk metadata is also copied into the copy
        context, so the scope "cloud_storage" can be copied without the source storage.
        """
        exclude = {
            BACKUP_META_FNAME,
            BACKUP_LIGHT_META_FNAME,
            COPY_CONTEXT_DIR,
            "delete_pending.json",
            "cloud_storage_info.json",
            ".cloud_storage_owner.json",
            REENCRYPT_CLAIM,
            REENCRYPT_CLAIM + ".cloud",
        }
        if backup.cloud_storage.data_copied:
            # Plan these from disk metadata, including objects held by older backups.
            exclude.update({"disks", "cloud_storage"})
        elif backup.cloud_storage.enabled:
            exclude.add("disks")
        with self._planning_step(result, backup.name):
            if self._reencrypt:
                self._plan_export_files(backup, plan, exclude)
            else:
                self._plan_directory(backup.name, plan, result, exclude=exclude)
            if backup.cloud_storage.requires_source_bucket:
                self._plan_source_metadata(backup, plan, result)

    def _plan_source_metadata(
        self, backup: BackupMetadata, plan: _ScopePlan, result: BackupCopyResult
    ) -> None:
        """Plan copying of original disk metadata into the copy context."""
        dst_dir = self._target_layout.get_backup_path(
            os.path.join(backup.name, source_dir(backup))
        )
        name = backup.get_sanitized_name()
        src_dir = _key(self._layout.get_backup_path(name)).rstrip("/")
        files = self._get_source_backup_files(name)
        position = 0
        while batch := files.read_batch(after=position, limit=128):
            for position, file_path, blob in batch:
                size = int(blob)
                if not file_path.startswith("disks/") or file_path.endswith("/"):
                    continue
                with self._planning_step(result, f"{src_dir}/{file_path}"):
                    _validate_relative_key(file_path)
                    plan.entries.append(
                        self._backup_file_entry(
                            f"{src_dir}/{file_path}",
                            os.path.join(dst_dir, file_path),
                            size,
                            backup.cloud_storage.encrypted,
                        )
                    )

    def _backup_file_entry(
        self, source: str, target: str, size: int, encrypted: bool
    ) -> CopyEntry:
        """Plan transfer of a backup file, preserving its encoded payload."""
        destination_size = size
        if self._reencrypt and not encrypted:
            destination_size = calc_encrypted_size_from_config(
                self._target_config["encryption"], size
            )
        return CopyEntry(
            self._storage,
            self._target_storage,
            source,
            target,
            size,
            destination_size,
            encrypt=self._reencrypt,
            decrypt=self._reencrypt and encrypted,
            backup_encryption=self._reencrypt,
        )

    # pylint: disable=too-many-locals
    def _plan_export_files(
        self, backup: BackupMetadata, plan: _ScopePlan, exclude: set[str]
    ) -> None:
        """Match the source inventory to the files expected by restore."""
        files = self._get_source_backup_files(backup.name)
        source = self._layout.get_backup_path(backup.name).rstrip("/")
        target = self._target_layout.get_backup_path(backup.name)
        planned = CopyIndex()

        def add(path: str, encrypted: bool) -> int:
            """Require a file and schedule its transformation exactly once."""
            _validate_relative_key(path)
            size = _listed_size(files, path)
            if size is None:
                raise ClickhouseBackupError(f'Missing backup file "{source}/{path}"')
            if planned.put_if_absent(path, b"") is not None:
                raise ClickhouseBackupError(f'Ambiguous backup file "{source}/{path}"')
            plan.entries.append(
                self._backup_file_entry(
                    f"{source}/{path}", os.path.join(target, path), size, encrypted
                )
            )
            return size

        for part in backup.get_parts():
            if part.link or part.disk_name in backup.cloud_storage.disks:
                continue
            names = get_data_part_file_names(part)
            directories = get_data_part_relative_dirs(part)
            directory = next(
                (
                    path
                    for path in directories
                    if all(files.contains(f"{path}/{name}") for name in names)
                ),
                directories[0],
            )
            sizes = [add(f"{directory}/{name}", part.encrypted) for name in names]
            self._check_export_part_size(part, directory, sizes)

        for detached in backup.get_detached_parts():
            directories = [
                _detached_part_path(
                    "",
                    detached.database,
                    detached.table,
                    detached.disk_name,
                    detached.name,
                    escape_names=escape,
                )
                for escape in (True, False)
            ]
            name = detached.name + ".tar"
            directory = next(
                (path for path in directories if files.contains(f"{path}/{name}")),
                directories[0],
            )
            size = add(f"{directory}/{name}", detached.encrypted)
            self._check_export_part_size(detached, directory, [size])

        self._plan_export_definitions(backup, files, add)
        position = 0
        while batch := files.read_batch(after=position, limit=128):
            for position, path, size_blob in batch:
                if path.split("/", 1)[0] in exclude or planned.contains(path):
                    continue
                if int(size_blob) == 0 and (not path or path.endswith("/")):
                    continue
                raise ClickhouseBackupError(
                    f'Unrecognized backup file "{source}/{path}"'
                )

    def _check_export_part_size(
        self,
        part: PartMetadata | DetachedPartMetadata,
        directory: str,
        sizes: Sequence[int],
    ) -> None:
        """Verify part sizes using the metadata independently of the S3 listing."""
        encryption = self._config["encryption"]
        if part.tarball or isinstance(part, DetachedPartMetadata):
            expected = calc_tarball_size(list(part.files), part.size)
            if part.encrypted:
                expected = calc_encrypted_size_from_config(encryption, expected)
            if sizes == [expected] or (
                isinstance(part, PartMetadata)
                and self._layout.matches_legacy_tar_size(part, sizes[0])
            ):
                return
        else:
            overhead = get_encryption_class(encryption["type"]).metadata_size()
            chunk = encryption["chunk_size"] + overhead
            plain = [
                size - (-(-size // chunk)) * overhead if part.encrypted else size
                for size in sizes
            ]
            if all(size >= 0 for size in plain) and part.size in (
                sum(plain),
                sum(-(-size // BLOCKSIZE) * BLOCKSIZE for size in plain),
            ):
                return
        raise ClickhouseBackupError(f'Wrong size of backup part "{directory}"')

    @staticmethod
    def _plan_export_definitions(
        backup: BackupMetadata, files: CopyIndex, add: Callable[[str, bool], int]
    ) -> None:
        """Resolve schema and private definition files in their restore layout."""
        databases = backup.get_databases()
        archive = _db_metadata_path_tar("")
        if files.contains(archive):
            add(archive, backup.encrypted)
        else:
            for db in databases:
                if not backup.get_database(db).has_embedded_metadata():
                    add(_db_metadata_path("", db), backup.encrypted)
        for db in databases:
            if backup.get_database(db).is_external_db_engine():
                continue
            archive = _table_metadata_path_tar("", db)
            if files.contains(archive):
                add(archive, backup.encrypted)
            else:
                for table in backup.get_tables(db):
                    add(_table_metadata_path("", db, table.name), backup.encrypted)
        if backup.access_control.acl_ids:
            acl_files = (
                [ACCESS_CONTROL_FNAME]
                if backup.access_control.backup_format == BackupStorageFormat.TAR
                else [f"{acl}.sql" for acl in backup.access_control.acl_ids]
            )
            for name in acl_files:
                add(f"access_control/{name}", True)
        definitions: Sequence[tuple[Sequence[str], Callable[[str, str], str]]] = (
            (backup.get_udf(), _udf_data_path),
            (backup.get_named_collections(), _named_collections_data_path),
            (backup.get_workload_entities(), _workload_entities_data_path),
            (backup.get_http_handlers(), _http_handlers_data_path),
        )
        for names, make_path in definitions:
            for name in names:
                path = make_path("", name)
                if make_path is _udf_data_path and not files.contains(path):
                    path = _udf_data_path("", name, escape_names=False)
                add(path, True)

    def _plan_directory(
        self,
        relative_path: str,
        plan: _ScopePlan,
        result: BackupCopyResult,
        exclude: set[str],
    ) -> None:
        """Plan source files outside excluded top-level paths."""
        src_dir = _key(self._layout.get_backup_path(relative_path)).rstrip("/")
        dst_dir = self._target_layout.get_backup_path(relative_path)

        files = self._get_source_backup_files(relative_path)
        position = 0
        while batch := files.read_batch(after=position, limit=128):
            for position, file_path, blob in batch:
                size = int(blob)
                # S3 consoles may create a zero-byte object for the directory itself.
                if not file_path and size == 0:
                    continue
                if file_path.split("/", 1)[0] in exclude:
                    continue
                path = f"{src_dir}/{file_path}"
                with self._planning_step(result, path):
                    _validate_relative_key(file_path)
                    plan.entries.append(
                        CopyEntry(
                            self._storage,
                            self._target_storage,
                            path,
                            os.path.join(dst_dir, file_path),
                            size,
                            size,
                        )
                    )

    def _plan_linked_parts(
        self,
        backup: BackupMetadata,
        target_meta: BackupMetadata,
        plan: _ScopePlan,
        result: BackupCopyResult,
    ) -> None:
        """
        Resolve local parts against the independent target window when enabled.

        Without a target window, keep usable source links. Materialize unresolved links
        from source and omit files of newly linked own parts from the copy plan.
        """

        @lru_cache(maxsize=1)
        def read_owner(name: str) -> BackupMetadata | None:
            """Reuse the last source owner's metadata while planning this backup."""
            return self._layout.get_backup(name, allow_pending=True)

        excluded_dirs = set()
        for part in backup.get_parts():
            if part.disk_name in backup.cloud_storage.disks:
                # Cloud storage links refer to disk metadata, not local part tarballs.
                continue
            with self._planning_step(
                result,
                f"{part.link or backup.name}/{part.database}/{part.table}/{part.name}",
            ):
                if self._target_deduplication is not None and not self._raw_export(
                    backup
                ):
                    owner = self._target_deduplication.find_part(part, backup.name)
                    if owner is not None:
                        target_meta.link_part(part, owner.backup_name, owner.part_name)
                        if part.link is None:
                            excluded_dirs.update(get_data_part_relative_dirs(part))
                        continue
                if part.link is not None:
                    self._plan_linked_part(
                        backup, target_meta, plan, part, result, read_owner
                    )
        if excluded_dirs:
            # Drop only the matched local part directories, preserving schema, detached and cloud data.
            prefix = _key(self._layout.get_backup_path(backup.name)).rstrip("/") + "/"
            plan.entries = plan.entries.filtered(
                lambda entry: not entry.src_path.startswith(prefix)
                or not any(
                    str(parent) in excluded_dirs
                    for parent in PurePosixPath(
                        entry.src_path.removeprefix(prefix)
                    ).parents
                )
            )

    # pylint: disable=too-many-arguments,too-many-positional-arguments,too-many-locals
    def _plan_linked_part(
        self,
        backup: BackupMetadata,
        target_meta: BackupMetadata,
        plan: _ScopePlan,
        part: PartMetadata,
        result: BackupCopyResult,
        read_owner: Callable[[str], BackupMetadata | None],
    ) -> None:
        """Materialize one local part when its target link is unusable."""
        if (
            not self._raw_export(backup)
            and self._target_deduplication is None
            and (
                self._is_link_valid_in_target(part, local=True)
                or self._reuse_materialized_part(part, target_meta)
            )
        ):
            return
        assert part.link
        target_backup_path = self._target_layout.get_backup_path(backup.name)
        source_files = self._get_source_backup_files(part.link)
        physical_part = part
        if self._reencrypt:
            owner = read_owner(part.link)
            owner_part = (
                owner.find_part(part.database, part.table, part.deduplicated_part_name)
                if owner
                else None
            )
            if (
                not owner_part
                or owner_part.link
                or (
                    owner_part.checksum,
                    owner_part.tarball,
                    set(owner_part.files),
                    owner_part.size,
                )
                != (part.checksum, part.tarball, set(part.files), part.size)
            ):
                raise ClickhouseBackupError(
                    f"Missing or incompatible owner of part {part.name}"
                )
            assert target_meta.copy_encryption is not None
            if (
                owner
                and owner.copy_encryption
                and owner.copy_encryption.get("target")
                != target_meta.copy_encryption["source"]
            ):
                raise ClickhouseBackupError(
                    f"Source key does not match owner of part {part.name}"
                )
            physical_part = owner_part
        file_names = get_data_part_file_names(part)
        part_dirs = get_data_part_relative_dirs(part)
        for part_dir in part_dirs:
            if all(
                source_files.contains(os.path.join(part_dir, f)) for f in file_names
            ):
                break
        else:
            if self._reencrypt or not self._keep_going:
                raise ClickhouseBackupError(
                    f'Data of part "{part.name}" of table "{part.database}"."{part.table}" '
                    f'deduplicated with backup "{part.link}" is not found in source storage'
                )
            # Prefer a complete directory above; otherwise salvage one partial
            # directory without mixing escaped and legacy representations.
            part_dir = max(
                part_dirs,
                key=lambda path: sum(
                    source_files.contains(os.path.join(path, f)) for f in file_names
                ),
            )

        logging.debug(
            'Data part {} of "{}"."{}" deduplicated with backup {} will be copied into the backup',
            part.name,
            part.database,
            part.table,
            part.link,
        )
        _validate_relative_key(part_dir)
        src_dir = os.path.join(self._layout.get_backup_path(part.link), part_dir)
        # Always use the escaped target path, even for legacy source layouts.
        # A retry must overwrite partial files rather than leave a shadowing alias.
        dst_dir = os.path.join(
            target_backup_path, os.path.dirname(part_dirs[0]), part.name
        )
        _validate_relative_key(part.name)
        sizes = []
        error_count = len(result.errors)
        for file_name in file_names:
            with self._planning_step(
                result, os.path.join(part.link, part_dir, file_name)
            ):
                _validate_relative_key(file_name)
                size = _listed_size(source_files, os.path.join(part_dir, file_name))
                if size is None:
                    raise ClickhouseBackupError(
                        f'File "{file_name}" of part "{part.name}" of table '
                        f'"{part.database}"."{part.table}" deduplicated with backup '
                        f'"{part.link}" is not found in source storage'
                    )
                if self._reencrypt:
                    sizes.append(size)
                dst_file_name = f"{part.name}.tar" if part.tarball else file_name
                plan.entries.append(
                    self._backup_file_entry(
                        os.path.join(src_dir, file_name),
                        os.path.join(dst_dir, dst_file_name),
                        size,
                        physical_part.encrypted,
                    )
                )
        if len(result.errors) == error_count:
            if self._reencrypt:
                self._check_export_part_size(physical_part, part_dir, sizes)
            target_meta.unlink_part(part)

    @staticmethod
    def _materialized_part_key(part: PartMetadata) -> tuple:
        return (
            part.link,
            part.database,
            part.table,
            part.deduplicated_part_name,
            part.checksum,
            part.disk_name,
            part.tarball,
            part.encrypted,
        )

    def _remember_materialized_parts(
        self, source: BackupMetadata, target: BackupMetadata
    ) -> None:
        """Only publish alternative owners after their complete backup is CREATED."""
        if target.state != BackupState.CREATED or self._raw_export(target):
            return
        for part in source.get_parts():
            if not part.link:
                continue
            if (
                self._target_deduplication is not None
                and part.disk_name not in source.cloud_storage.disks
            ):
                continue
            if part.disk_name in source.cloud_storage.disks and not (
                source.cloud_storage.data_copied and target.cloud_storage.data_copied
            ):
                continue
            stored = target.find_part(part.database, part.table, part.name)
            if not stored or stored.link:
                continue
            if (
                stored.checksum == part.checksum
                and stored.disk_name == part.disk_name
                and stored.tarball == part.tarball
                and (self._reencrypt or stored.encrypted == part.encrypted)
            ):
                self._materialized_parts.setdefault(
                    self._materialized_part_key(part), (target.name, stored.name)
                )

    def _reuse_materialized_part(
        self, part: PartMetadata, target: BackupMetadata
    ) -> bool:
        owner = self._materialized_parts.get(self._materialized_part_key(part))
        if owner is None:
            return False
        name, part_name = owner
        target.link_part(part, name, part_name)
        return True

    def _is_link_valid_in_target(self, part: PartMetadata, local: bool = False) -> bool:
        """
        Check that the backup containing deduplicated data part is present in target storage.

        A local part is also valid in a copy that is not completed once files of the part are
        stored there, as deduplication of new backups accepts: they are copied by its scope
        "files", and delete and purge keep parts referenced by other backups.
        """
        assert part.link
        if self._target_layout.get_delete_pending(part.link) is not None:
            return False
        if part.link not in self._target_backups:
            try:
                owner = self._target_layout.get_backup(part.link)
                if (
                    owner
                    and self._reencrypt
                    and not self._published_export_owner(owner)
                ):
                    owner = None
                self._target_backups[part.link] = owner
            except Exception as e:
                if not self._keep_going or is_broken_executor(e):
                    raise
                logging.warning(
                    "Cannot read target dependency {}, using source data: {}",
                    part.link,
                    e,
                )
                self._target_backups[part.link] = None

        target_backup = self._target_backups[part.link]
        if (
            not target_backup
            or target_backup.name != part.link
            or not self._compatible_export_owner(target_backup)
        ):
            return False
        complete = target_backup.state in (
            BackupState.CREATED,
            BackupState.PARTIALLY_DELETED,
        )
        if not complete and not (
            local and target_backup.state in (BackupState.CREATING, BackupState.FAILED)
        ):
            return False

        target_part = target_backup.find_part(
            part.database, part.table, part.deduplicated_part_name
        )
        return bool(
            target_part
            and not target_part.link
            and target_part.checksum == part.checksum
            and (
                complete
                or _part_data_exists(
                    self._target_layout,
                    target_backup.name,
                    target_part,
                    False,
                    {},
                    self._target_table_files,
                )
            )
        )

    def _get_source_backup_files(
        self,
        backup_name: str,
        layout: BackupLayout | None = None,
        storage: dict | None = None,
    ) -> CopyIndex:
        """
        Return sizes of files of the backup in source storage keyed by path relative to backup directory.

        Files of another layout and storage, such as source metadata in the copy context, are
        listed the same way.
        """
        if backup_name not in self._source_backup_files:
            layout = layout or self._layout
            backup_dir = _key(layout.get_backup_path(backup_name)).rstrip("/")
            files = CopyIndex()
            continuation = None
            while True:
                page, continuation = self._engine(
                    storage or self._storage
                ).read_listing_page(backup_dir, continuation)
                for path, size in page:
                    files.put(path[len(backup_dir) + 1 :], str(size).encode("ascii"))
                if continuation is None:
                    break
            self._source_backup_files[backup_name] = files
        return self._source_backup_files[backup_name]

    # pylint: disable=too-many-locals
    def _plan_full_cloud_storage_objects(
        self,
        backup: BackupMetadata,
        target_meta: BackupMetadata,
        plan: _ScopePlan,
        result: BackupCopyResult,
    ) -> None:
        """
        Copy full cloud backups into the target backup's own storage layout.

        Materialize linked parts whose owning backup is absent at the destination.
        Each disk keeps its own object prefix, as expected by full-backup restore.
        """
        entries: EntryIndex = EntryIndex()
        for table in backup.get_tables():
            for disk_name in backup.cloud_storage.disks:
                parts = [p for p in table.get_parts() if p.disk_name == disk_name]
                if not parts:
                    continue
                with self._planning_step(
                    result, f"{backup.name}/{disk_name}/{table.database}/{table.name}"
                ):
                    dst_prefix = _key(
                        self._target_layout.get_cloud_storage_data_path(
                            backup.name, disk_name
                        )
                    )
                    files = self._full_cloud_storage_metadata(
                        backup,
                        target_meta,
                        table,
                        disk_name,
                        parts,
                        dst_prefix,
                        entries,
                        result,
                    )
                    if files.count_files():
                        remote_path = _disk_metadata_path(
                            self._target_layout.get_cloud_storage_path(backup.name),
                            table.database,
                            table.name,
                            disk_name,
                            backup.cloud_storage.compressed,
                        )
                        plan.rewritten_cloud_storage_metadata.append(
                            _RewrittenTarball(remote_path, files)
                        )
        plan.entries.extend_index(entries)

    # pylint: disable=too-many-arguments,too-many-positional-arguments,too-many-locals
    def _full_cloud_storage_metadata(
        self,
        backup: BackupMetadata,
        target_meta: BackupMetadata,
        table: TableMetadata,
        disk_name: str,
        parts: list[PartMetadata],
        dst_prefix: str,
        entries: EntryIndex,
        copy_result: BackupCopyResult,
    ) -> TemporaryMetadataArchive:
        """Rewrite metadata and plan objects from each owning backup."""
        sources: dict[str, list[PartMetadata]] = {}
        for part in parts:
            if (
                part.link
                and not self._raw_export(backup)
                and self._is_link_valid_in_target(part)
            ):
                target_owner = self._target_backups[part.link]
                if target_owner and target_owner.cloud_storage.data_copied:
                    continue
            if (
                part.link
                and not self._raw_export(backup)
                and self._reuse_materialized_part(part, target_meta)
            ):
                continue
            sources.setdefault(part.link or backup.name, []).append(part)

        with closing(MetadataArchiveWriter()) as writer:
            for source_name, source_parts in sources.items():
                with self._planning_step(copy_result, source_name):
                    source: BackupMetadata | CloudStorageBackup | None = backup
                    if source_name != backup.name:
                        try:
                            source = self._layout.get_backup(
                                source_name, allow_pending=True
                            )
                        except (StorageError, S3RetryingError):
                            source = None
                        if source is None:
                            source = self._layout.get_cloud_storage_backup(source_name)
                    if (
                        isinstance(source, CloudStorageBackup)
                        and disk_name not in source.cloud_storage.disks
                    ):
                        raise ClickhouseBackupError(
                            f"Missing cloud disk {disk_name} in backup {source_name}"
                        )
                    if (
                        self._reencrypt
                        and source
                        and source.cloud_storage.data_encryption is not None
                    ):
                        raise ClickhouseBackupError(
                            f'Cloud storage data of backup "{source_name}" is already encrypted'
                        )
                    if source is None or not source.cloud_storage.data_copied:
                        raise ClickhouseBackupError(
                            f'Backup "{source_name}" holding cloud storage data is missing or holds no copied data'
                        )
                    src_prefix = _key(
                        self._layout.get_cloud_storage_data_path(source_name, disk_name)
                    )
                    names = {part.deduplicated_part_name: part for part in source_parts}
                    found = set()
                    planned_members = CopyIndex() if self._reencrypt else None
                    table_paths = cloud_table_paths(table)
                    for (
                        remote_path
                    ) in self._layout.get_cloud_storage_metadata_remote_paths(
                        source.get_sanitized_name(),
                        disk_name,
                        source.cloud_storage.compressed,
                        [table],
                    ):
                        with self._planning_step(copy_result, remote_path):
                            metadata_files = (
                                self._layout.download_cloud_storage_metadata_archive(
                                    source, remote_path
                                )
                            )
                            continuation = None
                            while True:
                                batch, continuation = metadata_files.read_batch(
                                    continuation
                                )
                                for file_name, data in batch:
                                    with self._planning_step(
                                        copy_result, f"{remote_path}:{file_name}"
                                    ):
                                        member = cloud_part_member(file_name)
                                        if member and member[0] in table_paths:
                                            table_path, owner_name, filename = member
                                            if owner_name not in names:
                                                continue
                                            part = names[owner_name]
                                            if planned_members is not None:
                                                planned_members.put(
                                                    f"{owner_name}/{filename}", b""
                                                )
                                            target_file = (
                                                f"{table_path}/{part.name}/{filename}"
                                            )
                                        elif planned_members is not None:
                                            continue
                                        else:
                                            components = file_name.split("/")
                                            for index, name in enumerate(
                                                components[:-1]
                                            ):
                                                if name in names:
                                                    break
                                            else:
                                                continue
                                            part = names[components[index]]
                                            components[index] = part.name
                                            target_file = "/".join(components)
                                        found.add(part.name)
                                        data = self._plan_metadata_objects(
                                            data,
                                            file_name,
                                            remote_path,
                                            self._storage,
                                            src_prefix,
                                            self._target_storage,
                                            dst_prefix,
                                            entries,
                                            copy_result,
                                            source_data_encryption=source.cloud_storage.data_encryption,
                                        )
                                        writer.append((target_file, data))
                                if continuation is None:
                                    break
                    for part in source_parts:
                        with self._planning_step(
                            copy_result, f"{source_name}/{part.name}"
                        ):
                            if part.name not in found:
                                raise ClickhouseBackupError(
                                    f'Metadata of cloud storage part "{part.name}" is missing in backup "{source_name}"'
                                )
                            if planned_members is not None:
                                for name in part.files:
                                    if (
                                        name != FROZEN_METADATA_FNAME
                                        and not planned_members.contains(
                                            f"{part.deduplicated_part_name}/{name}"
                                        )
                                    ):
                                        raise ClickhouseBackupError(
                                            f"Missing cloud metadata of {source_name}/{part.name}/{name}"
                                        )
                            if part.link:
                                target_meta.unlink_part(part)
            return writer.finish()

    # pylint: disable=too-many-arguments,too-many-positional-arguments
    def _plan_metadata_objects(
        self,
        data: bytes,
        file_name: str,
        remote_path: str,
        src_storage: dict,
        src_prefix: str | None,
        dst_storage: dict,
        dst_prefix: str,
        entries: EntryIndex,
        result: BackupCopyResult,
        *,
        source_data_encryption: dict | None = None,
        src_path_override: str | None = None,
    ) -> bytes:
        """Plan referenced objects and rewrite their destination keys."""
        _validate_relative_key(file_name)
        if os.path.basename(file_name) == FROZEN_METADATA_FNAME:
            return data
        metadata = _parse_metadata_file(data, file_name, remote_path)
        dst_keys = []
        for obj in metadata.objects:
            if self._reencrypt and obj.size < 0:
                raise ClickhouseBackupError(
                    f'Invalid object size in "{remote_path}:{file_name}"'
                )
            with self._planning_step(result, f"{remote_path}:{file_name}:{obj.key}"):
                entry = _cloud_storage_copy_entry(
                    src_storage,
                    src_prefix,
                    dst_storage,
                    dst_prefix,
                    obj,
                    metadata.full_object_keys,
                    encrypt=self._encrypt_cloud_storage_data,
                    data_encryption=(
                        self._cloud_storage_data_encryption
                        if self._encrypt_cloud_storage_data
                        else source_data_encryption
                    ),
                    src_path_override=src_path_override,
                )
                _add_cloud_storage_copy_entry(entries, entry)
                dst_keys.append(entry.dst_path)
        # An incomplete file must not be rewritten with a shortened object list.
        # Collected planning errors keep the backup FAILED until a successful retry.
        return (
            replace_object_keys(data, dst_keys)
            if metadata.full_object_keys and len(dst_keys) == len(metadata.objects)
            else data
        )

    # pylint: disable=too-many-locals
    def _plan_cloud_storage_objects(
        self,
        backup: BackupMetadata,
        plan: _ScopePlan,
        result: BackupCopyResult,
    ) -> None:
        """
        Plan copying of objects storing data of object storage disks.

        Objects are referenced from metadata files of frozen parts saved in the backup.
        Objects of the backup are copied under "<target path>/<backup name>/" keeping keys relative
        to the disk key prefix. Metadata files with full object keys are rewritten to reference
        the copied objects. Other disk metadata is copied as is from the copy context, where the
        scope "files" copies it, unless it failed to be planned: it could overwrite rewritten disk
        metadata uploaded by an earlier run. Without the scope "files", disk metadata is also read
        from the copy context, so the source storage is not accessed.
        """
        target_storage, target_path = self._get_target_cloud_storage()
        dst_prefix = os.path.join(target_path, backup.name)
        source_name = os.path.join(backup.name, source_dir(backup))
        layout, storage, name = (
            (self._layout, self._storage, backup.get_sanitized_name())
            if CopyScope.FILES in self._scopes
            else (self._target_layout, self._target_storage, source_name)
        )
        disks_dir = _key(os.path.join(layout.get_backup_path(name), "disks"))
        source_disks_dir = os.path.join(
            self._target_layout.get_backup_path(source_name), "disks"
        )
        dst_dir = os.path.join(
            self._target_layout.get_cloud_storage_path(backup.name), "disks"
        )
        sizes = self._get_source_backup_files(name, layout, storage)

        entries: EntryIndex = EntryIndex()
        for disk_name in backup.cloud_storage.disks:
            with self._planning_step(result, disk_name):
                src_storage, src_prefix, src_path_override = (
                    self._get_source_cloud_storage(backup, disk_name)
                )
                source_prefix = (
                    src_prefix if src_path_override is None else src_path_override
                )
                if (
                    self._reencrypt
                    and source_prefix is not None
                    and _locations_overlap(
                        src_storage, source_prefix, target_storage, dst_prefix
                    )
                ):
                    raise ClickhouseBackupError(
                        "Target cloud prefix overlaps the source disk"
                    )
                paths = layout.get_cloud_storage_metadata_remote_paths(
                    name, disk_name, backup.cloud_storage.compressed, "all"
                )
                planned_members = CopyIndex() if self._reencrypt else None
                for remote_path in paths:
                    with self._planning_step(result, remote_path):
                        errors = len(result.errors)
                        relative_path = _key(remote_path)[len(disks_dir) + 1 :]
                        dst_path = os.path.join(dst_dir, relative_path)
                        source_files = layout.download_cloud_storage_metadata_archive(
                            backup, remote_path
                        )
                        with closing(MetadataArchiveWriter()) as writer:
                            rewritten = False
                            continuation = None
                            while True:
                                batch, continuation = source_files.read_batch(
                                    continuation
                                )
                                for file_name, data in batch:
                                    rewritten_data = data
                                    with self._planning_step(
                                        result, f"{remote_path}:{file_name}"
                                    ):
                                        if planned_members is not None:
                                            _validate_relative_key(file_name)
                                            planned_members.put(file_name, b"")
                                        rewritten_data = self._plan_metadata_objects(
                                            data,
                                            file_name,
                                            remote_path,
                                            src_storage,
                                            src_prefix,
                                            target_storage,
                                            dst_prefix,
                                            entries,
                                            result,
                                            source_data_encryption=backup.cloud_storage.data_encryption,
                                            src_path_override=src_path_override,
                                        )
                                        if rewritten_data != data:
                                            rewritten = True
                                    writer.append((file_name, rewritten_data))
                                if continuation is None:
                                    break
                            files = writer.finish()

                        if rewritten or (
                            self._reencrypt and len(result.errors) == errors
                        ):
                            plan.rewritten_cloud_storage_metadata.append(
                                _RewrittenTarball(dst_path, files)
                            )
                        elif len(result.errors) == errors:
                            size = _listed_size(sizes, f"disks/{relative_path}")
                            if size is None:
                                raise KeyError(f"disks/{relative_path}")
                            plan.entries.append(
                                CopyEntry(
                                    self._target_storage,
                                    self._target_storage,
                                    os.path.join(source_disks_dir, relative_path),
                                    dst_path,
                                    size,
                                    size,
                                )
                            )

                if planned_members is not None:
                    self._validate_cloud_members(backup, disk_name, planned_members)

        plan.entries.extend_index(entries)

    @staticmethod
    def _validate_cloud_members(
        backup: BackupMetadata, disk: str, files: CopyIndex
    ) -> None:
        """Require disk metadata for every backed-up part file."""
        for table in backup.get_tables():
            paths = cloud_table_paths(table)
            for part in table.get_parts():
                if part.disk_name != disk:
                    continue
                expected = set(part.files) - {FROZEN_METADATA_FNAME}
                if not any(
                    all(
                        files.contains(f"{path}/{part.name}/{name}")
                        for name in expected
                    )
                    for path in paths
                ):
                    raise ClickhouseBackupError(
                        f"Missing cloud metadata of {table.database}.{table.name}/{disk}/{part.name}"
                    )

    def _get_target_cloud_storage(self) -> tuple[dict, str]:
        """
        Return storage config and path where objects of object storage disks are copied to.
        """
        conf = self._copy_config["cloud_storage"]["target"]
        if not conf.get("bucket"):
            raise ConfigurationError(
                "Backup contains data on object storage disks, the bucket to copy it to must be set "
                '(option --target-cloud-storage-bucket or "copy.cloud_storage.target.bucket" config option)'
            )

        credentials = self._target_storage["credentials"]
        if conf.get("access_key_id"):
            access_key_id, secret_access_key = (
                conf["access_key_id"],
                conf.get("secret_access_key"),
            )
        else:
            access_key_id, secret_access_key = (
                credentials["access_key_id"],
                credentials["secret_access_key"],
            )

        storage = make_storage_config(
            self._target_storage,
            endpoint_url=conf.get("endpoint_url") or credentials["endpoint_url"],
            bucket=conf["bucket"],
            access_key_id=access_key_id,
            secret_access_key=secret_access_key,
        )
        return storage, _key(conf.get("path") or "").rstrip("/")

    def _get_source_cloud_storage(
        self, backup: BackupMetadata, disk_name: str
    ) -> tuple[dict, str | None, str | None]:
        """
        Return storage config, original key prefix (None if unknown), and optional path override.
        """
        conf = self._copy_config["cloud_storage"]["source"]
        disk_location = self._get_disk_location(disk_name)
        if backup.cloud_storage.source:
            # Data was copied out of the disk by previous copying of the backup.
            source = backup.cloud_storage.source
            endpoint_url = conf.get("endpoint_url")
            if not endpoint_url:
                # Metadata is not authenticated. Only use an endpoint from trusted config.
                trusted_endpoints = [self._storage["credentials"]["endpoint_url"]]
                if disk_location:
                    trusted_endpoints.append(disk_location[0])
                stored_endpoint = source.get("endpoint_url", "").rstrip("/")
                endpoint_url = next(
                    (
                        endpoint
                        for endpoint in trusted_endpoints
                        if endpoint and endpoint.rstrip("/") == stored_endpoint
                    ),
                    None,
                )
                if not endpoint_url:
                    raise ConfigurationError(
                        "Cloud storage source endpoint in backup metadata does not match "
                        "a configured storage or disk endpoint; set "
                        '"copy.cloud_storage.source.endpoint_url" explicitly'
                    )
            bucket, path = source["bucket"], source["path"]
        elif disk_location:
            endpoint_url, bucket, path = disk_location
        elif conf.get("bucket"):
            endpoint_url = self._storage["credentials"]["endpoint_url"]
            bucket, path = conf["bucket"], None
        else:
            raise ConfigurationError(
                f'Failed to determine location of data of object storage disk "{disk_name}" from ClickHouse config, '
                "set it explicitly (options --cloud-storage-source-bucket, --cloud-storage-source-path "
                'or "copy.cloud_storage.source" config section)'
            )

        endpoint_url = conf.get("endpoint_url") or endpoint_url
        bucket = conf.get("bucket") or bucket

        if conf.get("access_key_id"):
            credentials = (conf["access_key_id"], conf.get("secret_access_key"))
        else:
            credentials = self._get_disk_credentials(disk_name) or (
                self._storage["credentials"]["access_key_id"],
                self._storage["credentials"]["secret_access_key"],
            )

        storage = make_storage_config(
            self._storage,
            endpoint_url=endpoint_url,
            bucket=bucket,
            access_key_id=credentials[0],
            secret_access_key=credentials[1],
        )
        return (
            storage,
            _key(path).rstrip("/") if path is not None else None,
            _key(conf["path"]).rstrip("/") if conf.get("path") is not None else None,
        )

    def _get_disk_config(self, disk_name: str) -> dict | None:
        try:
            disks = self._ch_config.config.get("storage_configuration", {}).get(
                "disks", {}
            )
        except OSError as e:
            logging.debug(f"Failed to load ClickHouse config: {e}")
            return None
        return disks.get(disk_name) if disks else None

    def _get_disk_location(self, disk_name: str) -> tuple[str, str, str] | None:
        """
        Return endpoint URL, bucket and key prefix of the object storage disk from ClickHouse config.
        Support path-style and S3 virtual-host endpoints.
        """
        return get_s3_disk_location(self._get_disk_config(disk_name) or {})

    def _get_disk_credentials(self, disk_name: str) -> tuple[str, str] | None:
        disk_config = self._get_disk_config(disk_name) or {}
        access_key_id = disk_config.get("access_key_id")
        secret_access_key = disk_config.get("secret_access_key")
        if isinstance(access_key_id, str) and isinstance(secret_access_key, str):
            return access_key_id, secret_access_key
        return None

    def _leave_out_confirmed_objects(
        self, backup: BackupMetadata, plan: _CopyPlan, result: BackupCopyResult
    ) -> None:
        """
        Leave out objects of each scope copied by previous runs.

        A matching size alone does not prove that an existing object belongs to this copy,
        and a record of the copy context alone does not prove that the object was not
        deleted since then, e.g. by a failed or partial delete. Only objects with both
        are left out. Without resuming, only scopes that are not copied by this run are checked.
        """
        context = self._copy_context
        assert context is not None
        if not context.has_records():
            return
        listing = self._list_target_objects(backup, plan)

        def pending(entry: CopyEntry, sizes: CopyIndex = listing) -> bool:
            """Keep objects unless both their target size and journal record match."""
            blob = sizes.get(
                json.dumps([storage_id(entry.dst_storage), _key(entry.dst_path)])
            )
            return (
                blob is None
                or int(blob) != entry.dst_size
                or not context.is_recorded(entry)
            )

        for scope, scope_plan in plan.items():
            if scope in self._scopes and not self._copy_config["resume"]:
                continue
            scope_plan.entries = scope_plan.entries.filtered(pending)
            scope_plan.rewritten_cloud_storage_metadata = [
                _RewrittenTarball(path, files)
                for path, files in scope_plan.rewritten_cloud_storage_metadata
                if not context.is_archive_recorded(
                    self._target_storage,
                    path,
                    files,
                    _listed_size(
                        listing,
                        json.dumps([storage_id(self._target_storage), _key(path)]),
                    ),
                )
            ]
        del listing, pending
        context.forget()
        total = result.objects_total or 0
        result.objects_skipped = total - sum(
            plan[scope].count() for scope in self._scopes
        )
        logging.info(
            "Resuming copying of backup {}: {} of {} objects are confirmed and skipped",
            backup.name,
            result.objects_skipped,
            total,
        )

    def _copy_scope(
        self,
        scope: CopyScope,
        backup: BackupMetadata,
        plan: _ScopePlan,
        result: BackupCopyResult,
        failed_before: bool,
    ) -> bool:
        """
        Copy objects of the scope. Return True if the scope is planned and copied completely.

        Objects that failed to copy are listed in the copy context. The list is replaced
        when objects fail and removed only once the scope succeeds, also when objects left
        by the failed run are confirmed copied.
        """
        context = self._copy_context
        assert context is not None
        errors = len(result.errors)
        copied = False
        with FailedObjects() as failures:
            try:
                self._copy_objects(backup, plan, result, failures)
                copied = True
            except Exception as e:
                if len(result.errors) == errors:
                    result.add_error(e, "copy")
                if not self._keep_going or is_broken_executor(e):
                    raise
            finally:
                if failures or (
                    copied and plan.planned and (plan.count() or failed_before)
                ):
                    context.save_failures(scope, failures)
        return plan.planned and len(result.errors) == errors

    # pylint: disable=too-many-locals,too-many-statements
    def _copy_objects(
        self,
        backup: BackupMetadata,
        plan: _ScopePlan,
        result: BackupCopyResult,
        failures: FailedObjects,
    ) -> None:
        """
        Copy objects and upload rewritten metadata tarballs, recording them in the copy context.

        Objects that failed to copy are added to failures.
        """
        context = self._copy_context
        assert context is not None
        errors = len(result.errors)
        total_size = 0
        continuation = None
        while True:
            batch, continuation = plan.entries.read_batch(continuation, limit=128)
            total_size += sum(entry.size for entry in batch)
            if continuation is None:
                break
        logging.info("Copying {} objects ({} bytes)", len(plan.entries), total_size)

        def completed(entry: CopyEntry) -> None:
            """Count the copied object and record it in the copy context."""
            result.completed(entry.size)
            context.record(entry)
            context.flush()

        def failed(entry: CopyEntry, error: BaseException) -> None:
            """Report the object that failed to copy."""
            result.add_error(
                error,
                "copy",
                source=entry.src_path,
                target=entry.dst_path,
                object_failed=True,
            )
            failures.append(entry, f"{type(error).__name__}: {error}")

        uploaded_archives: list[_RewrittenTarball] = []

        def uploaded(remote_path: str, files: MetadataFiles) -> None:
            """Count the uploaded tarball and keep it to be recorded in the copy context."""
            result.completed()
            uploaded_archives.append(_RewrittenTarball(remote_path, files))

        try:
            self._target_layout.copy_objects(
                self._resolve_copy_mode(plan.entries, result, on_error=failed),
                keep_going=self._keep_going,
                source_encryption=self._source_encryption,
                callback=completed,
                on_error=failed,
            )
            self._upload_rewritten_metadata(backup, plan, result, uploaded)
        except (Exception, TerminatingSignal) as e:
            # Submission can fail after other uploads have already started. Wait before
            # publishing FAILED and releasing the locks, preserving the original error.
            if len(result.errors) == errors:
                result.add_error(e, "copy")
            try:
                self._target_layout.wait(keep_going=True)
            except (Exception, TerminatingSignal) as drain_error:
                result.add_error(drain_error, "wait")
                logging.exception("Failed to drain pending copy operations")
                if not isinstance(drain_error, Exception):
                    raise
                if is_broken_executor(drain_error):
                    raise e from drain_error
            finally:
                context.flush(force=True)
            raise
        try:
            self._target_layout.wait(keep_going=self._keep_going)
            self._record_archives(backup, uploaded_archives)
        except BaseException:
            context.flush(force=True)
            raise
        context.flush(force=True, required=self._reencrypt)
        if len(result.errors) > errors:
            raise ClickhouseBackupError(
                f"Failed to copy backup {backup.name}: {len(result.errors)} error(s), "
                f"{result.objects_failed} object transfer(s) failed"
            )

    def _upload_rewritten_metadata(
        self,
        backup: BackupMetadata,
        plan: _ScopePlan,
        result: BackupCopyResult,
        uploaded: Callable,
    ) -> None:
        """Schedule metadata uploads after object copies, waiting every 256 archives."""
        for index, (remote_path, files) in enumerate(
            plan.rewritten_cloud_storage_metadata, 1
        ):
            errors_before_upload = len(result.errors)
            try:
                self._target_layout.upload_cloud_storage_metadata_files(
                    backup,
                    remote_path,
                    files,
                    callback=partial(uploaded, remote_path, files),
                    on_error=partial(
                        result.add_error,
                        stage="copy_metadata",
                        target=remote_path,
                        object_failed=True,
                    ),
                )
            except Exception as e:
                # Validation can fail before the upload registers its error callback.
                # Synchronous pipeline errors may already have called it.
                if len(result.errors) == errors_before_upload:
                    result.add_error(
                        e,
                        "copy_metadata",
                        target=remote_path,
                        object_failed=True,
                    )
                if not self._keep_going or is_broken_executor(e):
                    raise
            if index % 256 == 0:
                self._target_layout.wait(keep_going=self._keep_going)

    def _record_archives(
        self,
        backup: BackupMetadata,
        archives: list[_RewrittenTarball],
    ) -> None:
        """
        Record uploaded rewritten metadata tarballs in the copy context with their sizes in the target storage.

        Tarballs missing in the listing or uploaded when the listing fails are not recorded,
        so the next run uploads them again.
        """
        context = self._copy_context
        assert context is not None
        if not archives:
            return
        try:
            sizes = self._list(
                self._target_storage,
                _key(
                    os.path.join(
                        self._target_layout.get_cloud_storage_path(backup.name), "disks"
                    )
                ),
            )
        except Exception as e:
            if is_broken_executor(e):
                raise
            logging.warning(
                "Failed to list disk metadata of backup {}: {}", backup.name, e
            )
            return
        for path, files in archives:
            size = _listed_size(sizes, _key(path))
            if size is not None:
                context.record_archive(self._target_storage, path, files, size)

    def _resolve_copy_mode(
        self,
        entries: CopyEntries,
        copy_result: BackupCopyResult | None = None,
        on_error: Callable[[CopyEntry, BaseException], None] | None = None,
    ) -> Iterator[CopyEntry]:
        """
        Mark objects that can be copied on the server side.

        Availability of server-side copy is checked by copying the first object of each pair
        of source and target buckets with the same credentials. Such objects are excluded from the result.
        """
        continuation = None
        while True:
            batch, continuation = entries.read_batch(continuation, limit=128)
            for entry in batch:
                if self._server_side_copy_mode == ServerSideCopyMode.DISABLED:
                    yield entry
                    continue
                try:
                    resolved = self._resolve_object_copy_mode(entry, copy_result)
                except Exception as e:
                    if (
                        self._server_side_copy_mode != ServerSideCopyMode.STRICT
                        or is_broken_executor(e)
                    ):
                        raise
                    if on_error is not None:
                        on_error(entry, e)
                    if not self._keep_going:
                        raise
                    continue
                if resolved is not None:
                    yield resolved
            if continuation is None:
                break

    def _resolve_object_copy_mode(
        self, entry: CopyEntry, copy_result: BackupCopyResult | None
    ) -> CopyEntry | None:
        """Resolve one object's mode, returning None when the probe already copied it."""
        if entry.encrypt or entry.decrypt:
            if self._server_side_copy_mode == ServerSideCopyMode.STRICT:
                raise StorageError(
                    "Transformed objects cannot be copied on the server side"
                )
            return entry._replace(server_side=False)

        pair = (storage_id(entry.src_storage), storage_id(entry.dst_storage))
        cache_key = (
            _storage_cache_key(entry.src_storage),
            _storage_cache_key(entry.dst_storage),
        )
        available = self._server_side_copy.get(cache_key)
        if available is None:
            available = False
            if server_side_copy_possible(entry.src_storage, entry.dst_storage):
                try:
                    available = self._engine(entry.dst_storage).try_copy_object(
                        entry.src_storage["credentials"]["bucket"],
                        entry.src_path,
                        entry.dst_path,
                    )
                except Exception as e:
                    if (
                        self._server_side_copy_mode == ServerSideCopyMode.STRICT
                        or is_broken_executor(e)
                    ):
                        raise
                    logging.warning(
                        "Server-side copy probe failed, using streamed copy: {}", e
                    )
                    # A failed probe does not establish that this pair cannot copy server-side.
                    return entry._replace(server_side=False)
            self._server_side_copy[cache_key] = available
            if available:
                if copy_result is not None:
                    copy_result.completed(entry.size)
                if self._copy_context is not None:
                    self._copy_context.record(entry)
                logging.info(
                    "Objects are copied on the server side from {} to {}",
                    *pair,
                )
                return None
            if self._server_side_copy_mode != ServerSideCopyMode.STRICT:
                logging.warning(
                    "Server-side copy from {} to {} is not available, objects are copied through ch-backup",
                    *pair,
                )

        if self._server_side_copy_mode == ServerSideCopyMode.STRICT and not available:
            raise StorageError(
                f"Server-side copy from {pair[0]} to {pair[1]} is not available"
            )
        return entry._replace(server_side=available)

    def _list_target_objects(
        self, backup: BackupMetadata, plan: _CopyPlan
    ) -> CopyIndex:
        """
        Return sizes of objects of the copied backup in target storages.

        On failure nothing is returned, so all objects are copied again.
        """
        listing: CopyIndex = CopyIndex()
        listed = set()
        try:
            for storage, prefix in self._target_prefixes(backup, plan):
                key = (storage_id(storage), _key(prefix).rstrip("/"))
                if key in listed:
                    continue
                listed.add(key)
                continuation = None
                while True:
                    page, continuation = self._engine(storage).read_listing_page(
                        key[1], continuation
                    )
                    for path, size in page:
                        listing.put(
                            json.dumps([key[0], path]), str(size).encode("ascii")
                        )
                    if continuation is None:
                        break
        except Exception as e:
            if is_broken_executor(e):
                raise
            logging.warning(
                "Failed to list copied objects of backup {}, copying all objects: {}",
                backup.name,
                e,
            )
            listing.clear()
        return listing

    def _list(self, storage: dict, prefix: str) -> CopyIndex:
        """Index a listing without retaining all keys in RAM."""
        listing: CopyIndex = CopyIndex()
        continuation = None
        while True:
            page, continuation = self._engine(storage).read_listing_page(
                prefix, continuation
            )
            for path, size in page:
                listing.put(path, str(size).encode("ascii"))
            if continuation is None:
                break
        return listing

    def _engine(self, storage: dict) -> PipeLineCompatibleStorageEngine:
        """Reuse a client for the same storage credentials and configuration."""
        key = _storage_cache_key(storage)
        if key not in self._engines:
            self._engines[key] = get_storage_engine(storage)
        return self._engines[key]


def _storage_cache_key(storage: dict) -> tuple:
    """Identify a storage client, including its credentials. Never log this key."""
    credentials = storage["credentials"]
    return (
        storage_id(storage),
        credentials.get("access_key_id"),
        credentials.get("secret_access_key"),
        json.dumps(
            {
                name: storage.get(name, {})
                for name in ("boto_config", "module_config", "proxy_resolver")
            },
            sort_keys=True,
        ),
    )


def _parse_metadata_file(
    data: bytes, file_name: str, remote_path: str
) -> ObjectStorageMetadata:
    try:
        return parse_object_storage_metadata(data)
    except ObjectStorageMetadataError as e:
        raise ClickhouseBackupError(
            f'Failed to parse metadata file "{file_name}" from "{remote_path}": {e}'
        ) from e


# pylint: disable=too-many-positional-arguments
def _cloud_storage_copy_entry(
    src_storage: dict,
    src_prefix: str | None,
    dst_storage: dict,
    dst_prefix: str,
    obj: StorageObject,
    full_object_key: bool,
    *,
    encrypt: bool = False,
    data_encryption: dict | None = None,
    src_path_override: str | None = None,
) -> CopyEntry:
    """
    Make copy entry for the object referenced from metadata file of object storage disk.
    Object is copied to the target prefix keeping its key relative to the disk key prefix.
    """
    _validate_relative_key(obj.key)
    if full_object_key:
        src_path = obj.key
        if src_prefix and obj.key.startswith(src_prefix + "/"):
            relative_key = obj.key[len(src_prefix) + 1 :]
        else:
            relative_key = obj.key
        if src_path_override is not None:
            if src_prefix is None:
                raise ConfigurationError(
                    f'Cannot apply source path override to full object key "{obj.key}": '
                    "original source prefix is unknown; provide the original ClickHouse disk configuration"
                )
            if src_prefix and not obj.key.startswith(src_prefix + "/"):
                raise ConfigurationError(
                    f'Cannot apply source path override to full object key "{obj.key}": '
                    f'key is outside original source prefix "{src_prefix}"'
                )
            src_path = os.path.join(src_path_override, relative_key)
    else:
        read_prefix = (
            src_path_override if src_path_override is not None else src_prefix or ""
        )
        src_path = os.path.join(read_prefix, obj.key)
        relative_key = obj.key

    _validate_relative_key(relative_key)
    destination = os.path.join(dst_prefix, relative_key)
    if not _key(destination).startswith(_key(dst_prefix).rstrip("/") + "/"):
        raise ClickhouseBackupError(
            f'Copy destination "{destination}" escapes "{dst_prefix}"'
        )
    dst_size = (
        calc_encrypted_size_from_config(data_encryption, obj.size)
        if data_encryption is not None
        else obj.size
    )
    return CopyEntry(
        src_storage,
        dst_storage,
        src_path,
        destination,
        obj.size,
        dst_size,
        encrypt=encrypt,
    )


def _locations_overlap(
    storage: dict, prefix: str, other_storage: dict, other_prefix: str
) -> bool:
    """Check whether two paths share a bucket and overlap on a key boundary."""
    if physical_storage_id(storage) != physical_storage_id(other_storage):
        return False
    first = _key(prefix).rstrip("/")
    second = _key(other_prefix).rstrip("/")
    return (
        not first
        or not second
        or first == second
        or first.startswith(second + "/")
        or second.startswith(first + "/")
    )


def _reserve_copy_destination(index: CopyIndex, storage: dict, path: str) -> None:
    """Require a single writer for each exported object."""
    key = json.dumps([storage_id(storage), _key(path)])
    if index.put_if_absent(key, b"") is not None:
        raise ClickhouseBackupError(f'Multiple export objects map to "{path}"')


def _validate_relative_key(key: str) -> None:
    """Reject unsafe relative paths without changing the identity of S3 keys."""
    if not key or key.startswith("/") or any(p in (".", "..") for p in key.split("/")):
        raise ClickhouseBackupError(f'Unsafe copy object key "{key}"')


def _add_cloud_storage_copy_entry(entries: EntryIndex, entry: CopyEntry) -> None:
    """Deduplicate object destinations and reject conflicting sources or sizes."""
    planned = entries.get_or_insert(entry.dst_path, entry)
    if (storage_id(planned.src_storage), planned.src_path, planned.size) != (
        storage_id(entry.src_storage),
        entry.src_path,
        entry.size,
    ):
        raise ClickhouseBackupError(
            f'Objects "{planned.src_path}" and "{entry.src_path}" can\'t be copied to the same location'
        )


def _check_backup_state(backup: BackupMetadata) -> None:
    if backup.state != BackupState.CREATED:
        raise ClickhouseBackupError(
            f'Backup {backup.name} is in the state "{backup.state.value}", '
            f'only backups in the state "{BackupState.CREATED.value}" can be copied'
        )


def _files_scope_required(backup_name: str) -> ClickhouseBackupError:
    """Return the error for copying of the scope "cloud_storage" before the scope "files"."""
    return ClickhouseBackupError(
        f"Source metadata of backup {backup_name} is not found in target storage, "
        'its scope "files" must be copied first'
    )


def _files_copied(backup: BackupMetadata) -> bool:
    """Check that the backup is an incomplete copy with the scope "files" completed."""
    return (
        backup.state in (BackupState.CREATING, BackupState.FAILED)
        and backup.copy_scopes.get(CopyScope.FILES) == CopyScopeStatus.COMPLETED
    )


def _ready_for_cloud_storage_copy(backup: BackupMetadata) -> bool:
    """Check that the scope "cloud_storage" of the copy is not completed and can be copied alone."""
    return (
        _files_copied(backup)
        and backup.cloud_storage.requires_source_bucket
        and backup.copy_scopes.get(CopyScope.CLOUD_STORAGE) != CopyScopeStatus.COMPLETED
    )


def _key(path: str | None) -> str:
    """
    Return object key for the path.
    """
    return (path or "").lstrip("/")


def _listed_size(index: CopyIndex, key: str) -> int | None:
    """Decode a listed size while preserving absent objects."""
    blob = index.get(key)
    return None if blob is None else int(blob)
