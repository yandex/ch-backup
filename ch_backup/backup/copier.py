"""
Copying of backups from one storage to another.
"""

# pylint: disable=too-many-lines

import copy
import os
import re
from contextlib import contextmanager
from datetime import datetime
from functools import partial
from itertools import chain
from pathlib import PurePosixPath
from typing import Iterator, Sequence
from urllib.parse import quote, urlparse

from ch_backup import logging
from ch_backup.backup.copy_context import COPY_CONTEXT_DIR, CopyContext
from ch_backup.backup.copy_deduplication import (
    CopyDeduplication,
    copy_deduplication_cutoff,
)
from ch_backup.backup.copy_report import BackupCopyResult, CopyReport
from ch_backup.backup.layout import (
    BACKUP_LIGHT_META_FNAME,
    BACKUP_META_FNAME,
    BackupLayout,
    get_data_part_file_names,
    get_data_part_relative_dirs,
)
from ch_backup.backup.metadata import (
    BackupMetadata,
    BackupState,
    PartMetadata,
    TableMetadata,
    sanitize_backup_name,
)
from ch_backup.clickhouse.config import ClickhouseConfig
from ch_backup.clickhouse.object_storage_metadata import (
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
)
from ch_backup.exceptions import (
    BackupNotFound,
    ClickhouseBackupError,
    ConfigurationError,
    TerminatingSignal,
    is_broken_executor,
)
from ch_backup.storage.engine import get_storage_engine
from ch_backup.storage.engine.base import PipeLineCompatibleStorageEngine
from ch_backup.storage.object_copy import (
    CopyEntry,
    server_side_copy_possible,
    storage_id,
)
from ch_backup.util import utcnow

FROZEN_METADATA_FNAME = "frozen_metadata.txt"


class _CopyPlan:
    """
    Objects to copy and target storage locations where they are put.
    """

    def __init__(self) -> None:
        self.entries: list[CopyEntry] = []
        # Tarballs with cloud storage metadata that are uploaded with modified content
        # instead of copying as is: (target path, files).
        self.rewritten_cloud_storage_metadata: list[
            tuple[str, list[tuple[str, bytes]]]
        ] = []


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
        self._config = config
        self._target_config = target_config
        self._layout = layout
        self._target_layout = target_layout
        self._ch_config = ch_config
        self._storage: dict = config["storage"]
        self._target_storage: dict = target_config["storage"]
        self._copy_config: dict = target_config["copy"]
        self._encrypt_cloud_storage_data = bool(
            self._copy_config["cloud_storage"].get("encrypt_data")
        )
        self._cloud_storage_data_encryption = (
            get_cloud_storage_data_encryption_config(target_config)
            if self._encrypt_cloud_storage_data
            else None
        )

        self._engines: dict[tuple, PipeLineCompatibleStorageEngine] = {}
        # Metadata of backups in target storage used to resolve deduplication links.
        self._target_backups: dict[str, BackupMetadata | None] = {}
        # Files of backups in source storage used to resolve deduplication links.
        self._source_backup_files: dict[str, dict[str, int]] = {}
        # Original part identity -> successfully materialized target owner and name.
        self._materialized_parts: dict[tuple, tuple[str, str]] = {}
        self._target_deduplication: CopyDeduplication | None = None
        # Availability of server-side copy for bucket pairs with particular credentials.
        self._server_side_copy: dict[tuple, bool] = {}
        self._copy_context: CopyContext | None = None
        self._keep_going = False
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
        With keep_going, attempt every viable backup before raising an aggregate error.
        """
        self._keep_going = keep_going
        self._report = report if report is not None else CopyReport()
        self._report.error = None
        self._report.backups.clear()
        self._materialized_parts.clear()
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
                        result.status = "copied"
                        copied.append(backup.name)
                    else:
                        result.status = "skipped"
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
                if backup.cloud_storage.requires_source_bucket:
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
        for name in self._target_layout.get_backup_names():
            try:
                backup = self._target_layout.get_backup(name, use_light_meta=True)
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
            self._target_deduplication.load(candidates)
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
        cutoff = copy_deduplication_cutoff(self._copy_config, command_start)
        self._target_deduplication = (
            CopyDeduplication(self._target_layout, cutoff, command_start)
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

    def _select_backups(
        self, backup_names: Sequence[str] | None
    ) -> list[BackupMetadata]:
        backups = []
        names = (
            sorted(self._layout.get_backup_names())
            if backup_names is None
            else backup_names
        )
        results = {name: BackupCopyResult(name) for name in dict.fromkeys(names)}
        self._report.backups.extend(results.values())
        for name, result in results.items():
            try:
                backup = self._layout.get_backup(name, use_light_meta=True)
                if backup_names is None and (
                    not backup or backup.state != BackupState.CREATED
                ):
                    self._report.backups.remove(result)
                    continue
                if not backup:
                    raise BackupNotFound(name)
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
        self._report.backups[:] = [results[backup.name] for backup in backups] + [
            result for result in self._report.backups if result.status == "failed"
        ]
        return backups

    def _copy_backup(self, backup_name: str, result: BackupCopyResult) -> bool:
        """
        Copy backup. Return False if the backup is already present in the target storage.
        """
        self._source_backup_files.clear()
        self._target_backups.clear()
        backup = self._layout.get_backup(backup_name)
        if not backup:
            raise BackupNotFound(backup_name)
        _check_backup_state(backup)
        if backup.name != backup_name:
            raise ClickhouseBackupError(
                f'Backup metadata name does not match "{backup_name}"'
            )
        _validate_relative_key(backup.name)
        for disk_name in backup.cloud_storage.disks:
            _validate_relative_key(disk_name)
        if self._encrypt_cloud_storage_data and backup.cloud_storage.data_encrypted:
            raise ClickhouseBackupError(
                f'Cloud storage data of backup "{backup.name}" is already encrypted'
            )

        target_meta = self._make_target_metadata(backup)
        target_backup = self._target_layout.get_backup(backup_name, use_light_meta=True)
        if target_backup:
            if self._check_existing_target(backup, target_backup, target_meta):
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

        if self._target_deduplication is not None:
            # A PARTIALLY_DELETED owner may be retried. If it fails, later backups
            # must not reuse the old index entry after its state changes to FAILED.
            self._target_deduplication.discard_backup(backup.name)
        data_encryption = target_meta.cloud_storage.data_encryption
        self._copy_context = CopyContext(
            self._engine(self._target_storage),
            self._target_layout.get_backup_path(backup.name),
            backup,
            data_encryption=data_encryption,
        )
        if target_backup and self._copy_config["resume"]:
            if target_backup.cloud_storage.data_encryption == data_encryption:
                self._copy_context.load()
            else:
                logging.info(
                    "Cloud storage data encryption changed for backup {}; copying all objects",
                    backup.name,
                )

        plan = _CopyPlan()
        self._plan_backup_files(backup, plan, result)
        self._plan_linked_parts(backup, target_meta, plan, result)
        if backup.cloud_storage.data_copied:
            self._plan_full_cloud_storage_objects(backup, target_meta, plan, result)
        elif backup.cloud_storage.enabled:
            with self._planning_step(result, backup.name):
                self._plan_cloud_storage_objects(backup, target_meta, plan, result)
        try:
            self._validate_destinations(backup, plan)
        except Exception as e:
            result.add_error(e, "validation")
            raise

        rewritten_paths = {
            _key(path) for path, _ in plan.rewritten_cloud_storage_metadata
        }
        plan.entries = [
            entry
            for entry in plan.entries
            if storage_id(entry.dst_storage) != storage_id(self._target_storage)
            or _key(entry.dst_path) not in rewritten_paths
        ]
        result.objects_total = len(plan.entries) + len(
            plan.rewritten_cloud_storage_metadata
        )
        self._transfer_backup(backup, target_meta, plan, result)
        logging.info("Backup {} is copied", backup_name)
        return True

    def _check_existing_target(
        self,
        backup: BackupMetadata,
        target_backup: BackupMetadata,
        target_meta: BackupMetadata,
    ) -> bool:
        """Validate an existing target backup and return whether it is complete."""
        if (
            target_backup.start_time != backup.start_time
            or target_backup.hostname != backup.hostname
        ):
            raise ClickhouseBackupError(
                f"Backup {backup.name} already exists in target storage and it is not a copy of the source backup"
            )
        if target_backup.state != BackupState.CREATED:
            return False
        if (
            target_backup.cloud_storage.data_encryption
            != target_meta.cloud_storage.data_encryption
        ):
            raise ClickhouseBackupError(
                f"Backup {backup.name} already exists in target storage with "
                "different cloud storage data encryption"
            )
        if any(
            part.link
            and (
                self._target_deduplication is None
                or part.disk_name in backup.cloud_storage.disks
            )
            for part in backup.get_parts()
        ):
            existing = self._target_layout.get_backup(backup.name)
            if existing:
                self._remember_materialized_parts(backup, existing)
        logging.info(
            "Backup {} is already present in target storage, skipping it",
            backup.name,
        )
        return True

    def _transfer_backup(
        self,
        backup: BackupMetadata,
        target_meta: BackupMetadata,
        plan: _CopyPlan,
        result: BackupCopyResult,
    ) -> None:
        """Publish CREATED only after transfer succeeds, preserving failures during cleanup."""
        stage = "metadata"
        try:
            self._target_layout.upload_backup_metadata(target_meta)
            stage = "copy"
            self._copy_objects(backup, plan, result)
            stage = "metadata"
            target_meta.state = BackupState.CREATED
            self._target_layout.upload_backup_metadata(target_meta)
            self._remember_materialized_parts(backup, target_meta)
            if self._target_deduplication is not None:
                self._target_deduplication.add_backup(target_meta, published=True)
        except (Exception, TerminatingSignal) as e:
            logging.critical("Copying of backup {} failed", backup.name, exc_info=True)
            if stage == "metadata" or not result.errors:
                result.add_error(e, stage)
            target_meta.state = BackupState.FAILED
            target_meta.exception = result.errors[0]["message"]
            try:
                self._target_layout.upload_backup_metadata(target_meta)
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

    def _target_prefixes(self, backup: BackupMetadata) -> list[tuple[dict, str]]:
        """Return storages and paths that hold all objects of the copied backup."""
        prefixes = [
            (self._target_storage, self._target_layout.get_backup_path(backup.name)),
            (
                self._target_storage,
                self._target_layout.get_cloud_storage_path(backup.name),
            ),
        ]
        if backup.cloud_storage.requires_source_bucket:
            storage, path = self._get_target_cloud_storage()
            prefixes.append((storage, os.path.join(path, backup.name)))
        return prefixes

    def _validate_destinations(self, backup: BackupMetadata, plan: _CopyPlan) -> None:
        """Check the entire plan before publishing metadata or copying any objects."""
        prefixes = self._target_prefixes(backup)
        for _, prefix in prefixes:
            _validate_relative_key(_key(prefix).rstrip("/"))
        destinations = chain(
            ((entry.dst_storage, entry.dst_path) for entry in plan.entries),
            (
                (self._target_storage, path)
                for path, _ in plan.rewritten_cloud_storage_metadata
            ),
        )
        for storage, path in destinations:
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

    def _make_target_metadata(self, backup: BackupMetadata) -> BackupMetadata:
        target_meta = copy.deepcopy(backup)
        # DEPRECATED: ``path`` is populated for backward compatibility
        # with older ch-backup versions and is not used by new code.
        target_meta.path = self._target_layout.get_backup_path(backup.name)
        target_meta.copied_from = {
            "endpoint_url": self._storage["credentials"]["endpoint_url"],
            "bucket": self._storage["credentials"]["bucket"],
            "path_root": self._config["backup"]["path_root"],
        }
        target_meta.state = BackupState.CREATING
        target_meta.exception = None
        if self._cloud_storage_data_encryption and backup.cloud_storage.enabled:
            target_meta.cloud_storage.data_encryption = (
                get_cloud_storage_data_encryption_metadata(
                    self._cloud_storage_data_encryption
                )
            )
        return target_meta

    def _plan_backup_files(
        self, backup: BackupMetadata, plan: _CopyPlan, result: BackupCopyResult
    ) -> None:
        """
        Plan copying of all files from the backup directory except metadata files.
        """
        exclude = {BACKUP_META_FNAME, BACKUP_LIGHT_META_FNAME, COPY_CONTEXT_DIR}
        if backup.cloud_storage.data_copied:
            # Plan these from disk metadata, including objects held by older backups.
            exclude.update({"disks", "cloud_storage"})
        with self._planning_step(result, backup.name):
            self._plan_directory(backup.name, plan, result, exclude=exclude)

        sanitized_name = backup.get_sanitized_name()
        if (
            backup.cloud_storage.enabled
            and not backup.cloud_storage.data_copied
            and sanitized_name != backup.name
        ):
            # Cloud storage metadata is stored in the directory named after sanitized backup name.
            relative_path = os.path.join(sanitized_name, "disks")
            with self._planning_step(result, relative_path):
                self._plan_directory(relative_path, plan, result)

    def _plan_directory(
        self,
        relative_path: str,
        plan: _CopyPlan,
        result: BackupCopyResult,
        exclude: set[str] | None = None,
    ) -> None:
        src_dir = _key(self._layout.get_backup_path(relative_path)).rstrip("/")
        dst_dir = self._target_layout.get_backup_path(relative_path)

        for path, size in self._list(self._storage, src_dir).items():
            # S3 consoles may create a zero-byte object for the directory itself.
            if path == src_dir + "/" and size == 0:
                continue
            file_path = path[len(src_dir) + 1 :]
            if exclude and file_path.split("/", 1)[0] in exclude:
                continue
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
        plan: _CopyPlan,
        result: BackupCopyResult,
    ) -> None:
        """
        Resolve local parts against the independent target window when enabled.

        Without a target window, keep usable source links. Materialize unresolved links
        from source and omit files of newly linked own parts from the copy plan.
        """
        excluded_dirs = set()
        for part in backup.get_parts():
            if part.disk_name in backup.cloud_storage.disks:
                # Cloud storage links refer to disk metadata, not local part tarballs.
                continue
            with self._planning_step(
                result,
                f"{part.link or backup.name}/{part.database}/{part.table}/{part.name}",
            ):
                if self._target_deduplication is not None:
                    owner = self._target_deduplication.find_part(part, backup.name)
                    if owner is not None:
                        target_meta.link_part(part, owner.backup_name, owner.part_name)
                        if part.link is None:
                            excluded_dirs.update(get_data_part_relative_dirs(part))
                        continue
                if part.link is not None:
                    self._plan_linked_part(backup, target_meta, plan, part, result)
        if excluded_dirs:
            # Drop only the matched local part directories, preserving schema, detached and cloud data.
            prefix = _key(self._layout.get_backup_path(backup.name)).rstrip("/") + "/"
            plan.entries = [
                entry
                for entry in plan.entries
                if not entry.src_path.startswith(prefix)
                or not any(
                    str(parent) in excluded_dirs
                    for parent in PurePosixPath(
                        entry.src_path.removeprefix(prefix)
                    ).parents
                )
            ]

    # pylint: disable=too-many-arguments,too-many-positional-arguments,too-many-locals
    def _plan_linked_part(
        self,
        backup: BackupMetadata,
        target_meta: BackupMetadata,
        plan: _CopyPlan,
        part: PartMetadata,
        result: BackupCopyResult,
    ) -> None:
        """Materialize one local part when its target link is unusable."""
        if self._target_deduplication is None and (
            self._is_link_valid_in_target(part)
            or self._reuse_materialized_part(part, target_meta)
        ):
            return
        assert part.link
        target_backup_path = self._target_layout.get_backup_path(backup.name)
        source_files = self._get_source_backup_files(part.link)
        file_names = get_data_part_file_names(part)
        part_dirs = get_data_part_relative_dirs(part)
        for part_dir in part_dirs:
            if all(os.path.join(part_dir, f) in source_files for f in file_names):
                break
        else:
            if not self._keep_going:
                raise ClickhouseBackupError(
                    f'Data of part "{part.name}" of table "{part.database}"."{part.table}" '
                    f'deduplicated with backup "{part.link}" is not found in source storage'
                )
            # Prefer a complete directory above; otherwise salvage one partial
            # directory without mixing escaped and legacy representations.
            part_dir = max(
                part_dirs,
                key=lambda path: sum(
                    os.path.join(path, f) in source_files for f in file_names
                ),
            )

        logging.debug(
            'Data part {} of "{}"."{}" deduplicated with backup {} will be copied into the backup',
            part.name,
            part.database,
            part.table,
            part.link,
        )
        src_dir = os.path.join(self._layout.get_backup_path(part.link), part_dir)
        # Always use the escaped target path, even for legacy source layouts.
        # A retry must overwrite partial files rather than leave a shadowing alias.
        dst_dir = os.path.join(
            target_backup_path, os.path.dirname(part_dirs[0]), part.name
        )
        _validate_relative_key(part.name)
        error_count = len(result.errors)
        for file_name in file_names:
            with self._planning_step(
                result, os.path.join(part.link, part_dir, file_name)
            ):
                _validate_relative_key(file_name)
                if os.path.join(part_dir, file_name) not in source_files:
                    raise ClickhouseBackupError(
                        f'File "{file_name}" of part "{part.name}" of table '
                        f'"{part.database}"."{part.table}" deduplicated with backup '
                        f'"{part.link}" is not found in source storage'
                    )
                dst_file_name = f"{part.name}.tar" if part.tarball else file_name
                plan.entries.append(
                    CopyEntry(
                        self._storage,
                        self._target_storage,
                        os.path.join(src_dir, file_name),
                        os.path.join(dst_dir, dst_file_name),
                        source_files[os.path.join(part_dir, file_name)],
                        source_files[os.path.join(part_dir, file_name)],
                    )
                )
        if len(result.errors) == error_count:
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
        if target.state != BackupState.CREATED:
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
                and stored.encrypted == part.encrypted
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

    def _is_link_valid_in_target(self, part: PartMetadata) -> bool:
        """
        Check that the backup containing deduplicated data part is present in target storage.
        """
        assert part.link
        if part.link not in self._target_backups:
            try:
                self._target_backups[part.link] = self._target_layout.get_backup(
                    part.link
                )
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
            or target_backup.state
            not in (
                BackupState.CREATED,
                BackupState.PARTIALLY_DELETED,
            )
        ):
            return False

        target_part = target_backup.find_part(
            part.database, part.table, part.deduplicated_part_name
        )
        return bool(
            target_part
            and not target_part.link
            and target_part.checksum == part.checksum
        )

    def _get_source_backup_files(self, backup_name: str) -> dict[str, int]:
        """
        Return sizes of files of the backup in source storage keyed by path relative to backup directory.
        """
        if backup_name not in self._source_backup_files:
            backup_dir = _key(self._layout.get_backup_path(backup_name)).rstrip("/")
            self._source_backup_files[backup_name] = {
                path[len(backup_dir) + 1 :]: size
                for path, size in self._list(self._storage, backup_dir).items()
            }
        return self._source_backup_files[backup_name]

    # pylint: disable=too-many-locals
    def _plan_full_cloud_storage_objects(
        self,
        backup: BackupMetadata,
        target_meta: BackupMetadata,
        plan: _CopyPlan,
        result: BackupCopyResult,
    ) -> None:
        """
        Copy full cloud backups into the target backup's own storage layout.

        Materialize linked parts whose owning backup is absent at the destination.
        Each disk keeps its own object prefix, as expected by full-backup restore.
        """
        entries: dict[str, CopyEntry] = {}
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
                    if files:
                        extension = (
                            ".tar.gz" if backup.cloud_storage.compressed else ".tar"
                        )
                        remote_path = os.path.join(
                            self._target_layout.get_cloud_storage_path(backup.name),
                            "disks",
                            disk_name,
                            quote(table.database, safe=""),
                            quote(table.name, safe="") + extension,
                        )
                        plan.rewritten_cloud_storage_metadata.append(
                            (remote_path, files)
                        )
        plan.entries.extend(entries.values())

    # pylint: disable=too-many-arguments,too-many-positional-arguments,too-many-locals
    def _full_cloud_storage_metadata(
        self,
        backup: BackupMetadata,
        target_meta: BackupMetadata,
        table: TableMetadata,
        disk_name: str,
        parts: list[PartMetadata],
        dst_prefix: str,
        entries: dict[str, CopyEntry],
        copy_result: BackupCopyResult,
    ) -> list[tuple[str, bytes]]:
        sources: dict[str, list[PartMetadata]] = {}
        for part in parts:
            if part.link and self._is_link_valid_in_target(part):
                target_owner = self._target_backups[part.link]
                if target_owner and target_owner.cloud_storage.data_copied:
                    continue
            if part.link and self._reuse_materialized_part(part, target_meta):
                continue
            sources.setdefault(part.link or backup.name, []).append(part)

        files = []
        for source_name, source_parts in sources.items():
            with self._planning_step(copy_result, source_name):
                source = (
                    backup
                    if source_name == backup.name
                    else self._layout.get_backup(source_name)
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
                for remote_path in self._layout.get_cloud_storage_metadata_remote_paths(
                    source.get_sanitized_name(),
                    disk_name,
                    source.cloud_storage.compressed,
                    [table],
                ):
                    with self._planning_step(copy_result, remote_path):
                        metadata_files = self._layout.get_cloud_storage_metadata_files(
                            source, remote_path
                        )
                        for file_name, data in metadata_files:
                            with self._planning_step(
                                copy_result, f"{remote_path}:{file_name}"
                            ):
                                components = file_name.split("/")
                                # A part may contain projection subdirectories; match its directory
                                # anywhere above the file rather than just its immediate parent.
                                index = next(
                                    (
                                        i
                                        for i, name in enumerate(components[:-1])
                                        if name in names
                                    ),
                                    None,
                                )
                                if index is None:
                                    continue
                                part = names[components[index]]
                                found.add(part.name)
                                components[index] = part.name
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
                                files.append(("/".join(components), data))
                for part in source_parts:
                    with self._planning_step(copy_result, f"{source_name}/{part.name}"):
                        if part.name not in found:
                            raise ClickhouseBackupError(
                                f'Metadata of cloud storage part "{part.name}" is missing in backup "{source_name}"'
                            )
                        if part.link:
                            target_meta.unlink_part(part)
        return files

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
        entries: dict[str, CopyEntry],
        result: BackupCopyResult,
        *,
        source_data_encryption: dict | None = None,
        src_path_override: str | None = None,
    ) -> bytes:
        _validate_relative_key(file_name)
        if os.path.basename(file_name) == FROZEN_METADATA_FNAME:
            return data
        metadata = _parse_metadata_file(data, file_name, remote_path)
        dst_keys = []
        for obj in metadata.objects:
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
        target_meta: BackupMetadata,
        plan: _CopyPlan,
        result: BackupCopyResult,
    ) -> None:
        """
        Plan copying of objects storing data of object storage disks.

        Objects are referenced from metadata files of frozen parts saved in the backup.
        Objects of the backup are copied under "<target path>/<backup name>/" keeping keys relative
        to the disk key prefix. Metadata files with full object keys are rewritten to reference
        the copied objects.
        """
        target_storage, target_path = self._get_target_cloud_storage()
        dst_prefix = os.path.join(target_path, backup.name)

        entries: dict[str, CopyEntry] = {}
        for disk_name in backup.cloud_storage.disks:
            with self._planning_step(result, disk_name):
                src_storage, src_prefix, src_path_override = (
                    self._get_source_cloud_storage(backup, disk_name)
                )
                for remote_path in self._layout.get_cloud_storage_metadata_remote_paths(
                    backup.get_sanitized_name(),
                    disk_name,
                    backup.cloud_storage.compressed,
                    "all",
                ):
                    with self._planning_step(result, remote_path):
                        files = self._layout.get_cloud_storage_metadata_files(
                            backup, remote_path
                        )
                        rewritten = False
                        for i, (file_name, data) in enumerate(files):
                            with self._planning_step(
                                result, f"{remote_path}:{file_name}"
                            ):
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
                                    files[i] = (file_name, rewritten_data)
                                    rewritten = True

                        if rewritten:
                            plan.rewritten_cloud_storage_metadata.append(
                                (self._get_target_path(remote_path), files)
                            )

        plan.entries.extend(entries.values())
        target_meta.cloud_storage.source = {
            "endpoint_url": target_storage["credentials"]["endpoint_url"],
            "bucket": target_storage["credentials"]["bucket"],
            "path": dst_prefix,
        }

    def _get_target_path(self, path: str) -> str:
        """
        Map path in source backup storage to the path in target backup storage.
        """
        src_root = _key(self._layout.get_backup_path("")).rstrip("/")
        relative_path = _key(path)[len(src_root) + 1 :] if src_root else _key(path)
        return self._target_layout.get_backup_path(relative_path)

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

        storage = _make_storage_config(
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

        storage = _make_storage_config(
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
        disk_config = self._get_disk_config(disk_name) or {}
        endpoint = disk_config.get("endpoint")
        if not isinstance(endpoint, str):
            return None

        url = urlparse(endpoint)
        # Follow ClickHouse's S3 host recognition; AWS PrivateLink uses path-style buckets.
        virtual_host = re.fullmatch(r"(.+)\.(s3[.-][a-z0-9.:-]+)", url.netloc)
        private_link = re.fullmatch(
            r"bucket\.vpce-[a-z0-9.-]+\.vpce\.amazonaws\.com(:\d{1,5})?", url.netloc
        )
        if virtual_host and not private_link:
            bucket, host = virtual_host.groups()
            return f"{url.scheme}://{host}", bucket, url.path.strip("/")

        bucket, _, path = url.path.strip("/").partition("/")
        return f"{url.scheme}://{url.netloc}", bucket, path

    def _get_disk_credentials(self, disk_name: str) -> tuple[str, str] | None:
        disk_config = self._get_disk_config(disk_name) or {}
        access_key_id = disk_config.get("access_key_id")
        secret_access_key = disk_config.get("secret_access_key")
        if isinstance(access_key_id, str) and isinstance(secret_access_key, str):
            return access_key_id, secret_access_key
        return None

    def _copy_objects(
        self, backup: BackupMetadata, plan: _CopyPlan, result: BackupCopyResult
    ) -> None:
        """
        Copy objects of the plan, skipping ones copied by previous runs.

        A matching size alone does not prove that an existing object belongs to this copy,
        and a record of the copy context alone does not prove that the object was not
        deleted since then, e.g. by a failed or partial delete. Only objects with both
        are skipped.
        """
        context = self._copy_context
        assert context is not None
        pending = plan.entries
        if context.has_records():
            listing = self._list_target_objects(backup)
            pending = [
                entry
                for entry in plan.entries
                if listing.get((storage_id(entry.dst_storage), _key(entry.dst_path)))
                != entry.dst_size
                or not context.is_recorded(entry)
            ]
            del listing
            context.forget()
            result.objects_skipped = len(plan.entries) - len(pending)
            logging.info(
                "Resuming copying of backup {}: {} of {} objects are confirmed and skipped",
                backup.name,
                result.objects_skipped,
                len(plan.entries),
            )
        entries = self._resolve_copy_mode(pending, result)

        logging.info(
            "Copying {} objects ({} bytes)",
            len(pending),
            sum(entry.size for entry in pending),
        )

        def completed(entry: CopyEntry) -> None:
            """Count the copied object and record it in the copy context."""
            result.completed(entry.size)
            context.record(entry)
            context.flush()

        try:
            self._target_layout.copy_objects(
                entries,
                keep_going=self._keep_going,
                callback=completed,
                on_error=lambda entry, error: result.add_error(
                    error,
                    "copy",
                    source=entry.src_path,
                    target=entry.dst_path,
                    object_failed=True,
                ),
            )
            for index, (remote_path, files) in enumerate(
                plan.rewritten_cloud_storage_metadata, 1
            ):
                errors_before_upload = len(result.errors)
                try:
                    self._target_layout.upload_cloud_storage_metadata_files(
                        backup,
                        remote_path,
                        files,
                        callback=result.completed,
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
        except (Exception, TerminatingSignal) as e:
            # Submission can fail after other uploads have already started. Wait before
            # publishing FAILED and releasing the locks, preserving the original error.
            if not result.errors:
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
        finally:
            context.flush(force=True)
        if result.errors:
            raise ClickhouseBackupError(
                f"Failed to copy backup {backup.name}: {len(result.errors)} error(s), "
                f"{result.objects_failed} object transfer(s) failed"
            )

    def _resolve_copy_mode(
        self, entries: list[CopyEntry], copy_result: BackupCopyResult | None = None
    ) -> Iterator[CopyEntry]:
        """
        Mark objects that can be copied on the server side.

        Availability of server-side copy is checked by copying the first object of each pair
        of source and target buckets with the same credentials. Such objects are excluded from the result.
        """
        if not self._copy_config["server_side_copy"]:
            yield from (entry._replace(server_side=False) for entry in entries)
            return

        for entry in entries:
            if (
                entry.encrypt
                or entry.decrypt
                or not server_side_copy_possible(entry.src_storage, entry.dst_storage)
            ):
                yield entry._replace(server_side=False)
                continue

            pair = (storage_id(entry.src_storage), storage_id(entry.dst_storage))
            cache_key = (
                _storage_cache_key(entry.src_storage),
                _storage_cache_key(entry.dst_storage),
            )
            if cache_key not in self._server_side_copy:
                try:
                    available = self._engine(entry.dst_storage).try_copy_object(
                        entry.src_storage["credentials"]["bucket"],
                        entry.src_path,
                        entry.dst_path,
                    )
                except Exception as e:
                    if not self._keep_going or is_broken_executor(e):
                        raise
                    logging.warning(
                        "Server-side copy probe failed, using streamed copy: {}", e
                    )
                    available = False
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
                    continue
                logging.warning(
                    "Server-side copy from {} to {} is not available, objects are copied through ch-backup",
                    *pair,
                )

            yield entry._replace(server_side=self._server_side_copy[cache_key])

    def _list_target_objects(self, backup: BackupMetadata) -> dict[tuple, int]:
        """
        Return sizes of objects of the copied backup in target storages.

        On failure nothing is returned, so all objects are copied again.
        """
        listing: dict[tuple, int] = {}
        listed = set()
        try:
            for storage, prefix in self._target_prefixes(backup):
                key = (storage_id(storage), _key(prefix).rstrip("/"))
                if key in listed:
                    continue
                listed.add(key)
                for path, size in self._list(storage, key[1]).items():
                    listing[(key[0], path)] = size
        except Exception as e:
            if is_broken_executor(e):
                raise
            logging.warning(
                "Failed to list copied objects of backup {}, copying all objects: {}",
                backup.name,
                e,
            )
            return {}
        return listing

    def _list(self, storage: dict, prefix: str) -> dict[str, int]:
        return self._engine(storage).list_dir_with_sizes(prefix)

    def _engine(self, storage: dict) -> PipeLineCompatibleStorageEngine:
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


def _validate_relative_key(key: str) -> None:
    """Reject unsafe relative paths without changing the identity of S3 keys."""
    if not key or key.startswith("/") or any(p in (".", "..") for p in key.split("/")):
        raise ClickhouseBackupError(f'Unsafe copy object key "{key}"')


def _add_cloud_storage_copy_entry(
    entries: dict[str, CopyEntry], entry: CopyEntry
) -> None:
    planned = entries.setdefault(entry.dst_path, entry)
    if (
        storage_id(planned.src_storage),
        planned.src_path,
        planned.size,
        planned.dst_size,
    ) != (
        storage_id(entry.src_storage),
        entry.src_path,
        entry.size,
        entry.dst_size,
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


def _make_storage_config(
    base: dict,
    endpoint_url: str,
    bucket: str,
    access_key_id: str | None,
    secret_access_key: str | None,
) -> dict:
    """
    Make storage config for the bucket with other settings taken from the base storage config.
    """
    storage = copy.deepcopy(base)
    storage["credentials"] = {
        "endpoint_url": endpoint_url,
        "bucket": bucket,
        "access_key_id": access_key_id,
        "secret_access_key": secret_access_key,
    }
    # Proxy is configured for backup storage only.
    storage["proxy_resolver"] = {"uri": None, "proxy_port": None}
    return storage


def _key(path: str | None) -> str:
    """
    Return object key for the path.
    """
    return (path or "").lstrip("/")
