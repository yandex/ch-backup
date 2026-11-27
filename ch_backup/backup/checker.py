"""
Check of backups: metadata, schemas and data are readable and in place.
"""

import os
import tempfile
import threading
from collections import defaultdict
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass, field
from hashlib import md5
from pathlib import Path
from tarfile import BLOCKSIZE
from typing import Callable, Iterable, Mapping, NamedTuple, Sequence

from ch_backup import logging
from ch_backup.backup.checker_archive import (
    CloudFile,
    LegacyDisk,
    files_of_parts,
    read_legacy_files,
)
from ch_backup.backup.checker_errors import (
    check_failure,
    describe,
    raise_if_check_failed,
)
from ch_backup.backup.copier import FROZEN_METADATA_FNAME
from ch_backup.backup.layout import (
    ACCESS_CONTROL_FNAME,
    BackupLayout,
    validate_access_control_filenames,
)
from ch_backup.backup.metadata import (
    BackupMetadata,
    BackupStorageFormat,
    PartMetadata,
    TableMetadata,
    sanitize_backup_name,
)
from ch_backup.backup.metadata.cloud_storage_metadata import CloudStorageBackup
from ch_backup.calculators import calc_aligned_files_size
from ch_backup.clickhouse.control import ClickhouseCTL
from ch_backup.clickhouse.object_storage_metadata import parse_object_storage_metadata
from ch_backup.config import Config
from ch_backup.encryption import calc_encrypted_size_from_config
from ch_backup.exceptions import (
    BackupCheckError,
    BackupNotFound,
    StorageError,
)
from ch_backup.storage.engine import get_storage_engine
from ch_backup.storage.engine.s3 import S3RetryingError
from ch_backup.util import escape_metadata_file_name, list_dir_files, scan_dir_files


class BackupCheckResult(NamedTuple):
    """
    Result of a backup check.

    `unverified` counts objects whose content can't be verified, `skipped_data` lists tables
    left without data.
    """

    name: str
    state: str | None
    problems: list[str]
    unverified: int
    skipped_data: list[str]
    checked: bool = True

    @property
    def broken(self) -> bool:
        """
        Return True if the backup can't be restored completely.
        """
        return len(self.problems) != 0


@dataclass
class _BackupCheck:
    """Keep the metadata and findings for one selected backup."""

    meta: BackupMetadata | None = None
    light: BackupMetadata | None = None
    pending_state: str | None = None
    problems: list[str] = field(default_factory=list)
    skipped: list[str] = field(default_factory=list)
    unverified: int = 0


class _Listing:
    """
    Listing of stored objects, filled once and dropped after its last user.
    """

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.sizes: dict | None = None
        self.error: str | None = None
        self.users = 0


class _ObjectKey(NamedTuple):
    """
    Key of a stored object: a part tarball or disk metadata of a table.
    """

    kind: str
    source: str
    database: str
    table: str
    item: str
    linked: bool
    fields: tuple | None


class _ObjectCheckResult(NamedTuple):
    """
    Result of a check of a stored object, the same for every backup needing it.
    """

    problem: str | None = None
    unverified: int = 0

    def for_parts(self, _parts: dict[str, tuple[str, ...]]) -> "_ObjectCheckResult":
        """
        Return the result itself, it holds for all parts.
        """
        return self


class _DiskCheckResult(NamedTuple):
    """
    Result of a check of disk metadata of a table, given to a backup by the parts it needs.
    """

    missing_parts: set[str]
    unreadable_parts: dict[str, str]
    differing_files: dict[tuple[str, tuple[str, ...]], str]
    broken_objects: dict[str, set[tuple[str, int]]]
    unverified_objects: dict[str, set[tuple[str, int]]]

    def for_parts(self, parts: dict[str, tuple[str, ...]]) -> _ObjectCheckResult:
        """
        Return the result for the specified parts with their files in backup metadata.
        """
        if missing := sorted(self.missing_parts & parts.keys()):
            return _ObjectCheckResult(f"disk metadata is missing for parts {missing}")
        if unreadable := [
            self.unreadable_parts[part]
            for part in sorted(parts)
            if part in self.unreadable_parts
        ]:
            return _ObjectCheckResult(
                f"disk metadata of parts is unreadable: {'; '.join(unreadable)}"
            )
        if differing := [
            self.differing_files[item]
            for item in sorted(parts.items())
            if item in self.differing_files
        ]:
            return _ObjectCheckResult(
                f"files of parts differ from disk metadata: {'; '.join(differing)}"
            )
        if broken := _objects_of_parts(self.broken_objects, parts):
            return _ObjectCheckResult(
                f"{len(broken)} objects of copied data are missing or have wrong size"
            )
        return _ObjectCheckResult(
            unverified=len(_objects_of_parts(self.unverified_objects, parts))
        )


class BackupChecker:
    """Read backup objects and report each problem to the backups that need them."""

    def __init__(
        self, config: Config, deep: bool, workers: int, tmp_dir: str | None = None
    ) -> None:
        self._config = config.derive(patch={"multiprocessing": {"workers": 0}})
        self._deep = deep
        self._workers = workers
        self._tmp_dir = tmp_dir
        self._local = threading.local()
        self._backups: dict[str, _BackupCheck] = {}
        self._cloud_sources: dict[
            str, Future[BackupMetadata | CloudStorageBackup | None]
        ] = {}
        self._legacy_disks: dict[tuple[str, str], LegacyDisk] = {}
        self._objects: dict[_ObjectKey, dict] = {}
        self._listings: dict[tuple, _Listing] = {}

    def check(
        self, backup_names: Sequence[str] | None = None
    ) -> list[BackupCheckResult]:
        """
        Check specified backups or all backups if names are not specified.
        """
        try:
            existing = set(self._layout().get_backup_names())
            names = (
                list(dict.fromkeys(backup_names))
                if backup_names
                else self._backup_names(existing)
            )
        except Exception as error:
            raise_if_check_failed(error)
            raise
        for name in names:
            if name not in existing:
                raise BackupNotFound(name)

        self._backups = {name: _BackupCheck() for name in names}
        self._objects.clear()
        self._listings.clear()
        self._cloud_sources.clear()
        self._legacy_disks.clear()
        with ThreadPoolExecutor(self._workers) as pool:
            self._collect_objects(dict(zip(names, pool.map(self._load, names))))
            logging.info(
                "Checking {} stored objects of {} backups",
                len(self._objects),
                len(names),
            )
            results = dict(
                zip(self._objects, pool.map(self._check_object, self._objects))
            )

        if not backup_names:
            names = [
                name for name in names if self._backups[name].pending_state is None
            ]
        return self._results(names, results)

    def _backup_names(self, existing: set[str]) -> list[str]:
        """Exclude directories that only contain another backup's cloud payload."""
        companions = _cloud_storage_dirs(existing)
        owners: dict[str, list[str]] = defaultdict(list)
        for name in existing - companions:
            owners[sanitize_backup_name(name)].append(name)
        for name in companions:
            candidates = owners[name]
            if len(candidates) == 1:
                try:
                    if self._layout().is_cloud_storage_companion(
                        name, candidates[0], allow_legacy=True
                    ):
                        existing.remove(name)
                except StorageError as error:
                    raise_if_check_failed(error)
        return sorted(existing)

    def _layout(self) -> BackupLayout:
        """
        Return layout of the current thread, so that storage clients are not shared between threads.
        """
        if not hasattr(self._local, "layout"):
            self._local.layout = BackupLayout(self._config)
        return self._local.layout

    def _load(self, name: str) -> list[tuple]:
        """
        Read light and full metadata and schemas of a backup, and return entries of its parts.
        """
        layout = self._layout()
        check = self._backups[name]
        problems = check.problems
        try:
            marker = layout.get_delete_pending(name)
        except Exception as error:
            problems.append(f"deletion marker: {describe(error)}")
            return []
        if marker is not None:
            check.pending_state = marker["state"]
            return []
        try:
            check.light = layout.get_backup(
                name, use_light_meta=True, allow_pending=True, fallback_to_full=False
            )
            if not check.light:
                problems.append("light metadata: missing")
        except Exception as e:
            check.light = None
            problems.append(f"light metadata: {describe(e)}")

        try:
            backup = layout.get_backup(name, allow_pending=True)
            if not backup:
                problems.append("metadata: missing")
                return []
            check.meta = backup
            entries = _entries(backup)
            check.skipped = _skipped_data(backup)
            self._check_schemas(layout, backup, problems)
            self._check_definitions(layout, backup, problems)
            self._check_detached_parts(layout, backup, problems)
        except Exception as e:
            problems.append(f"metadata: {describe(e)}")
            return []
        try:
            self._check_access_control(layout, backup, problems)
        except Exception as e:
            problems.append(f"access control: {describe(e)}")
        return entries

    @staticmethod
    def _check_definitions(
        layout: BackupLayout, backup: BackupMetadata, problems: list[str]
    ) -> None:
        """Read the stored definitions required by restore."""
        definitions = (
            ("UDF", backup.get_udf(), layout.get_udf_create_statement),
            (
                "named collection",
                backup.get_named_collections(),
                layout.get_named_collection_create_statement,
            ),
            (
                "workload entity",
                backup.get_workload_entities(),
                layout.get_workload_entity_create_statement,
            ),
            (
                "HTTP handler",
                backup.get_http_handlers(),
                layout.get_http_handler_create_statement,
            ),
        )
        for kind, names, read in definitions:
            if not isinstance(names, list) or not all(
                isinstance(name, str) and name for name in names
            ):
                problems.append(f"{kind}: invalid list of definition names")
                continue
            for name in names:
                label = f'{kind} "{name}"'
                try:
                    statement = read(backup, name)
                except Exception as error:
                    raise_if_check_failed(error, operation=f"read {label}")
                    problems.append(f"{label}: unreadable ({type(error).__name__})")
                else:
                    if not statement.strip():
                        problems.append(f"{label}: empty definition")

    def _check_detached_parts(
        self, layout: BackupLayout, backup: BackupMetadata, problems: list[str]
    ) -> None:
        """Check each detached archive and read it in deep mode."""
        for part in backup.get_detached_parts():
            label = f'detached part {part.name} of "{part.database}"."{part.table}" on {part.disk_name}'
            if part.object_storage:
                self._backups[backup.name].unverified += 1
            try:
                if not layout.check_detached_part(backup.name, part, for_restore=True):
                    problems.append(f"{label}: archive is missing or has wrong size")
                elif self._deep:
                    with tempfile.TemporaryDirectory(dir=self._tmp_dir) as tmp_dir:
                        part_path = os.path.join(tmp_dir, "part")
                        layout.download_detached_part(backup, part, part_path)
                        layout.wait()
                        if not ClickhouseCTL.detached_part_matches(
                            part, part_path, strict=True
                        ):
                            problems.append(
                                f"{label}: files differ from backup metadata"
                            )
            except Exception as error:
                problems.append(f"{label}: {describe(error)}")

    @staticmethod
    def _check_schemas(
        layout: BackupLayout, backup: BackupMetadata, problems: list[str]
    ) -> None:
        """
        Check that create statements of databases and tables are readable and present.

        As restore does, databases with embedded metadata and tables of external databases
        are skipped.
        """
        databases = [backup.get_database(name) for name in backup.get_databases()]
        db_names = [db.name for db in databases if not db.has_embedded_metadata()]
        try:
            statements = layout.get_database_create_statements(backup, db_names)
            if missing := _missing(db_names, statements):
                problems.append(f"database schemas: missing for {missing}")
        except Exception as e:
            problems.append(f"database schemas: {describe(e)}")

        for db in databases:
            if db.is_external_db_engine():
                continue
            tables = [table.name for table in backup.get_tables(db.name)]
            try:
                statements = layout.get_table_create_statements(backup, db.name, tables)
                if missing := _missing(tables, statements):
                    problems.append(
                        f'table schemas of "{db.name}": missing for {missing}'
                    )
            except Exception as e:
                problems.append(f'table schemas of "{db.name}": {describe(e)}')

    def _check_access_control(
        self, layout: BackupLayout, backup: BackupMetadata, problems: list[str]
    ) -> None:
        """
        Check that access control entities are present, deep check downloads them as restore does.
        """
        access_control = backup.access_control
        if not isinstance(access_control.acl_ids, list) or not all(
            isinstance(acl_id, str) and acl_id for acl_id in access_control.acl_ids
        ):
            problems.append("access control: invalid list of entity IDs")
            return
        if not access_control.acl_ids:
            return
        acl_meta = access_control.acl_meta
        if acl_meta:
            for index, _ in enumerate(access_control.acl_ids):
                entry = acl_meta.get(str(index)) if isinstance(acl_meta, dict) else None
                if not isinstance(entry, dict) or not all(
                    isinstance(entry.get(field), str) and entry[field]
                    for field in ("name", "char")
                ):
                    problems.append(
                        f"access control: invalid metadata for entity #{index + 1}"
                    )
        else:
            self._backups[backup.name].unverified += 1
        entities = [f"{acl_id}.sql" for acl_id in access_control.acl_ids]
        tarball = access_control.backup_format == BackupStorageFormat.TAR
        file_names = [ACCESS_CONTROL_FNAME] if tarball else sorted(entities)
        validate_access_control_filenames(entities)
        if missing := [
            file_name
            for file_name in file_names
            if not layout.check_access_control_file(backup.name, file_name)
        ]:
            problems.append(f"access control: missing {missing}")
            return
        if not self._deep:
            return

        with tempfile.TemporaryDirectory(
            dir=self._tmp_dir, ignore_cleanup_errors=True
        ) as access_dir:
            if tarball:
                layout.download_access_control(access_dir, backup.name)
            else:
                for file_name in file_names:
                    layout.download_access_control_file(
                        access_dir, backup.name, file_name
                    )
            if missing := sorted(set(entities) - set(list_dir_files(access_dir))):
                problems.append(f"access control: missing entities {missing}")

    def _collect_objects(self, entries: dict[str, list[tuple]]) -> None:
        """
        Group parts of all backups by the stored object holding their data.

        A local part is held by its tarball, parts on a cloud storage disk by disk metadata of
        the table. Linked parts and parts with different metadata are grouped apart, since
        restore reads them differently.
        """
        for name, parts in entries.items():
            backup = self._backups[name].meta
            if backup is None:
                continue
            for table, table_paths, part, key, files in parts:
                if key not in self._objects:
                    self._listings.setdefault(_listing_key(key), _Listing()).users += 1
                obj = self._objects.setdefault(
                    key,
                    {
                        "backup": backup,
                        "part": part,
                        "table": table,
                        "table_paths": set(),
                        "backups": {},
                    },
                )
                if key.kind == "disk":
                    disk_key = (key.source, key.item)
                    if disk_key not in self._legacy_disks:
                        self._legacy_disks[disk_key] = LegacyDisk()
                    self._legacy_disks[disk_key].parts.update(
                        (path, part.deduplicated_part_name) for path in table_paths
                    )
                obj["table_paths"] |= table_paths
                obj["backups"].setdefault(name, {})[part.deduplicated_part_name] = files

    def _check_object(self, key: _ObjectKey) -> _ObjectCheckResult | _DiskCheckResult:
        """
        Check a stored object and return its problem if there is any.
        """
        obj = self._objects[key]
        try:
            if key.kind == "part":
                return self._check_part(key, obj["backup"], obj["part"])
            return self._check_cloud_storage_data(key, obj)
        except Exception as e:
            return _ObjectCheckResult(describe(e))
        finally:
            self._release(key)

    def _check_part(
        self, key: _ObjectKey, backup: BackupMetadata, part: PartMetadata
    ) -> _ObjectCheckResult:
        """
        Check presence and size of a part tarball, listing data of its table once.

        Deep check downloads the part as restore does, not encrypted parts are verified only by
        checksums.txt.
        """
        layout = self._layout()
        stored = self._listing(
            key,
            lambda: layout.get_table_data_files(
                key.source, key.database, key.table, strict=True
            ),
        )
        files_in_storage = stored.get(part.deduplicated_part_name, {})
        exact_size = layout.check_data_part_files(part, files_in_storage)
        if not exact_size:
            tar_size = files_in_storage.get(f"{part.deduplicated_part_name}.tar")
            if (
                not part.tarball
                or tar_size is None
                or not layout.matches_legacy_tar_size(part, tar_size)
            ):
                return _ObjectCheckResult("data is missing or has wrong size")
        if not self._deep:
            return _ObjectCheckResult(unverified=int(not exact_size))

        with tempfile.TemporaryDirectory(
            dir=self._tmp_dir, ignore_cleanup_errors=True
        ) as part_dir:
            layout.download_data_part(backup, part, part_dir, callback=lambda *_: None)
            root = Path(part_dir)
            files = set(scan_dir_files(root))
            if files != set(part.files):
                return _ObjectCheckResult(
                    f"files differ from metadata: {_difference(set(part.files), files)}"
                )
            paths = [root / file for file in files]
            if (
                calc_aligned_files_size(paths, alignment=BLOCKSIZE) != part.size
                and calc_aligned_files_size(paths) != part.size
            ):
                return _ObjectCheckResult("files have wrong size")
            if (
                "checksums.txt" not in files
                or md5((root / "checksums.txt").read_bytes()).hexdigest()
                != part.checksum
            ):  # nosec
                return _ObjectCheckResult("checksums.txt differs from metadata")
            return _ObjectCheckResult(
                unverified=int(not part.encrypted or not part.tarball)
            )

    def _check_cloud_storage_data(
        self, key: _ObjectKey, obj: dict
    ) -> _ObjectCheckResult | _DiskCheckResult:
        """
        Check disk metadata of a table and presence and size of copied objects it refers to.

        Not copied objects stay in the bucket of the disk and are not checked. As restore does,
        linked data is read using the restore metadata of the holding backup.
        """
        backup = (
            self._cloud_source(key.source)
            if key.linked
            else self._backups[key.source].meta
        )
        if not backup or (key.linked and not backup.cloud_storage.data_copied):
            return _ObjectCheckResult(
                f'backup "{key.source}" holding the data is missing or holds no copied data'
            )

        layout = self._layout()
        parts: set[str] = set().union(*obj["backups"].values())
        if (
            isinstance(backup, CloudStorageBackup)
            and key.item not in backup.cloud_storage.disks
        ):
            return _ObjectCheckResult(
                f'Missing cloud disk "{key.item}" in backup "{key.source}"'
            )
        prefix = layout.get_cloud_storage_data_path(key.source, key.item).strip("/")
        found = self._read_cloud_files(key, backup, obj, parts, prefix=prefix)
        stored, unreadable = _cloud_part_results(found)
        broken, unverified = {}, stored
        if backup.cloud_storage.data_copied:
            listed = self._listing(
                key,
                lambda: get_storage_engine(self._config["storage"]).list_dir_with_sizes(
                    prefix
                ),
            )
            encryption = backup.cloud_storage.data_encryption
            broken = {
                part: {
                    (path, size)
                    for path, size in items
                    if listed.get(path)
                    != (
                        calc_encrypted_size_from_config(encryption, size)
                        if encryption is not None
                        else size
                    )
                }
                for part, items in stored.items()
            }
            unverified = {}
        return _DiskCheckResult(
            parts - set(found),
            unreadable,
            _differing_files(found, obj["backups"].values()),
            broken,
            unverified,
        )

    def _read_cloud_files(
        self,
        key: _ObjectKey,
        backup: BackupMetadata | CloudStorageBackup,
        obj: dict,
        parts: set[str],
        *,
        prefix: str,
    ) -> dict[str, dict[str, CloudFile]]:
        """Find the archive restore reads and return its parsed part files."""
        layout = self._layout()
        remote_paths = layout.list_restore_metadata_archives(
            backup, key.item, [obj["table"]]
        )
        legacy_path = layout.get_legacy_disk_metadata_path(
            backup.get_sanitized_name(), key.item, backup.cloud_storage.compressed
        )
        if legacy_path in remote_paths:
            metadata = self._legacy_files(key, backup, legacy_path, prefix)
            if isinstance(metadata, str):
                raise StorageError(metadata)
            found: dict[str, dict[str, CloudFile]] = defaultdict(dict)
            for path in sorted(obj["table_paths"]):
                for part in parts:
                    if (path, part) in metadata:
                        found[part].update(metadata[(path, part)])
        else:
            files = [
                entry
                for remote_path in remote_paths
                for entry in layout.read_cloud_storage_metadata_archive(
                    backup, remote_path
                )
            ]
            found = {
                part: {
                    name: _cloud_file(data, prefix) for name, data in members.items()
                }
                for part, members in files_of_parts(
                    files, parts, obj["table_paths"]
                ).items()
            }
        return found

    def _legacy_files(
        self,
        key: _ObjectKey,
        backup: BackupMetadata | CloudStorageBackup,
        remote_path: str,
        prefix: str,
    ) -> dict[tuple[str, str], dict[str, CloudFile]] | str:
        """Download and scan the shared archive once for all requested tables."""
        legacy = self._legacy_disks[(key.source, key.item)]
        with legacy.lock:
            if legacy.failure:
                raise BackupCheckError(legacy.failure)
            archive_key = (remote_path, backup.cloud_storage.encrypted)
            if archive_key not in legacy.archives:
                try:
                    legacy.archives[archive_key] = read_legacy_files(
                        self._layout(),
                        backup,
                        remote_path,
                        prefix,
                        legacy.parts,
                        parse_file=_cloud_file,
                    )
                except Exception as error:
                    legacy.failure = check_failure(error, "read cloud metadata")
                    if legacy.failure:
                        raise BackupCheckError(legacy.failure) from None
                    legacy.archives[archive_key] = describe(error)
            return legacy.archives[archive_key]

    def _listing(self, key: _ObjectKey, list_objects: Callable[[], dict]) -> dict:
        """
        Return the listing of stored objects for an object, listing them once for all its users.
        """
        listing = self._listings[_listing_key(key)]
        with listing.lock:
            if listing.error is not None:
                raise BackupCheckError(listing.error)
            if listing.sizes is None:
                try:
                    listing.sizes = list_objects()
                except Exception as error:
                    listing.error = check_failure(error, "list backup data") or (
                        f"Could not list backup data: {type(error).__name__}"
                    )
                    raise BackupCheckError(listing.error) from None
            return listing.sizes

    def _release(self, key: _ObjectKey) -> None:
        """
        Drop the listing used by an object once it has no more users.
        """
        listing = self._listings[_listing_key(key)]
        with listing.lock:
            listing.users -= 1
            if not listing.users:
                listing.sizes = None

    def _cloud_source(self, name: str) -> BackupMetadata | CloudStorageBackup | None:
        """Resolve a linked owner once even when several tables need it."""
        if (check := self._backups.get(name)) and check.light:
            return check.light
        pending: Future[BackupMetadata | CloudStorageBackup | None] = Future()
        existing = self._cloud_sources.setdefault(name, pending)
        if existing is pending:
            try:
                pending.set_result(self._resolve_cloud_source(name))
            except BaseException as error:
                pending.set_exception(error)
        return existing.result()

    def _resolve_cloud_source(
        self, name: str
    ) -> BackupMetadata | CloudStorageBackup | None:
        """Resolve owner metadata in the order used by restore."""
        layout = self._layout()
        try:
            source = layout.get_backup(name, use_light_meta=True, allow_pending=True)
        except (StorageError, S3RetryingError) as error:
            descriptor = layout.get_cloud_storage_backup(name)
            if descriptor is None:
                raise_if_check_failed(error)
            return descriptor
        return source or layout.get_cloud_storage_backup(name)

    def _results(
        self,
        names: list[str],
        results: dict[_ObjectKey, _ObjectCheckResult | _DiskCheckResult],
    ) -> list[BackupCheckResult]:
        """
        Attribute results of stored objects to the backups linking to them.
        """
        problems = {name: list(self._backups[name].problems) for name in names}
        unverified = {name: self._backups[name].unverified for name in names}
        for key, object_result in results.items():
            for name, parts in self._objects[key]["backups"].items():
                result = object_result.for_parts(parts)
                if result.problem:
                    problems[name].append(f"{_object_name(key)}: {result.problem}")
                else:
                    unverified[name] += result.unverified

        results_by_name = []
        for name in names:
            check = self._backups[name]
            backup = check.meta or check.light
            state = backup.state.value if backup else check.pending_state
            results_by_name.append(
                BackupCheckResult(
                    name,
                    state,
                    problems[name],
                    unverified[name],
                    check.skipped,
                    checked=check.pending_state is None,
                )
            )
        return results_by_name


def _object_name(key: _ObjectKey) -> str:
    """
    Return the name of a stored object to report its problem with.
    """
    return f'{key.kind} {key.item} of "{key.database}"."{key.table}" in backup "{key.source}"'


def _listing_key(key: _ObjectKey) -> tuple:
    """
    Return the key of the listing of stored objects: data of a table or copied data of a disk.
    """
    if key.kind == "part":
        return (key.source, key.database, key.table)
    return (key.source, key.item)


def _cloud_storage_dirs(names: set[str]) -> set[str]:
    """
    Return directories holding only cloud storage data, named after sanitized backup names.
    """
    return names & {
        sanitized for name in names if (sanitized := sanitize_backup_name(name)) != name
    }


def _skipped_data(backup: BackupMetadata | None) -> list[str]:
    """
    Return tables of a backup whose data or some parts were left out of it.
    """
    if not backup:
        return []
    skipped = []
    for table in backup.get_tables():
        reason = table.data_skipped_reason
        if not isinstance(table.skipped_parts, list) or not all(
            isinstance(part, str) for part in table.skipped_parts
        ):
            raise ValueError("Invalid skipped parts in table metadata")
        if not reason and table.skipped_parts:
            reason = f"skipped parts {sorted(table.skipped_parts)}"
        if reason:
            skipped.append(f'"{table.database}"."{table.name}": {reason}')
    return skipped


def _missing(names: list[str], statements: list[tuple[str, str]]) -> list[str]:
    """
    Return names that have no create statement.
    """
    return sorted(
        set(names) - {name for name, sql in dict(statements).items() if sql.strip()}
    )


def _table_paths(table: TableMetadata) -> set[str]:
    """
    Return possible paths of a table on disk, as ClickHouse lays them out.
    """
    paths = {
        f"data/{escape_metadata_file_name(table.database)}/"
        f"{escape_metadata_file_name(table.name)}"
    }
    if table.uuid:
        paths.add(f"store/{table.uuid[:3]}/{table.uuid}")
    return paths


def _differing_files(
    found: Mapping[str, Mapping[str, object]],
    files_of_backups: Iterable[dict[str, tuple[str, ...]]],
) -> dict[tuple[str, tuple[str, ...]], str]:
    """
    Return differences of files of parts in disk metadata from part metadata of each backup.
    """
    differing = {}
    for part, part_files in {
        item for parts in files_of_backups for item in parts.items()
    }:
        if part not in found:
            continue
        expected = set(part_files) - {FROZEN_METADATA_FNAME}
        if expected != (actual := set(found[part])):
            differing[(part, part_files)] = f"{part}: {_difference(expected, actual)}"
    return differing


def _difference(expected: set[str], actual: set[str]) -> str:
    """
    Return files missing from and unexpected in the actual files.
    """
    return (
        f"missing {sorted(expected - actual)}, unexpected {sorted(actual - expected)}"
    )


def _cloud_file(data: bytes, prefix: str) -> CloudFile:
    """Parse a disk metadata file without retaining its bytes."""
    try:
        return CloudFile(_stored_objects([data], prefix))
    except Exception as error:
        return CloudFile(set(), describe(error))


def _cloud_part_results(
    found: dict[str, dict[str, CloudFile]],
) -> tuple[dict[str, set[tuple[str, int]]], dict[str, str]]:
    """Combine object references and attribute parse errors to their parts."""
    stored, unreadable = {}, {}
    for part, members in found.items():
        for _, problem in members.values():
            if problem:
                unreadable[part] = f"{part}: {problem}"
                break
        else:
            stored[part] = set().union(*(item.objects for item in members.values()))
    return stored, unreadable


def _stored_objects(files: Iterable[bytes], prefix: str) -> set[tuple[str, int]]:
    """
    Return full keys and sizes of objects referenced from disk metadata files.

    A relative key is relative to the key prefix of the disk, for copied data the data path
    of the backup.
    """
    stored = set()
    for data in files:
        metadata = parse_object_storage_metadata(data)
        stored |= {
            (
                item.key if metadata.full_object_keys else f"{prefix}/{item.key}",
                item.size,
            )
            for item in metadata.objects
        }
    return stored


def _objects_of_parts(
    objects: dict[str, set[tuple[str, int]]], parts: Iterable[str]
) -> set[tuple[str, int]]:
    """
    Return objects referenced from the specified parts.
    """
    return set().union(*(objects.get(part, set()) for part in parts))


def _entries(backup: BackupMetadata) -> list[tuple]:
    """
    Return parts of a backup with their tables and keys of the stored objects holding them.
    """
    entries = []
    for table in backup.get_tables():
        table_paths = _table_paths(table)
        for part in table.get_parts():
            valid = (
                isinstance(part.checksum, str),
                isinstance(part.size, int)
                and not isinstance(part.size, bool)
                and part.size >= 0,
                isinstance(part.disk_name, str),
                isinstance(part.tarball, bool),
                isinstance(part.encrypted, bool),
                isinstance(part.files, (list, tuple))
                and all(isinstance(file, str) and file for file in part.files),
            )
            if not all(valid):
                raise ValueError("Invalid data part metadata")
            key = _object_key(backup, table, part)
            entries.append((table, table_paths, part, key, tuple(part.files)))
    return entries


def _object_key(
    backup: BackupMetadata, table: TableMetadata, part: PartMetadata
) -> _ObjectKey:
    """
    Return the key of the stored object holding data of a part.
    """
    cloud = part.disk_name in backup.cloud_storage.disks
    return _ObjectKey(
        "disk" if cloud else "part",
        part.link or backup.name,
        table.database,
        table.name,
        part.disk_name if cloud else part.deduplicated_part_name,
        cloud and part.link is not None,
        None if cloud else _part_fields(part),
    )


def _part_fields(part: PartMetadata) -> tuple:
    """
    Return fields of part metadata that restore reads the part data by.
    """
    return (part.checksum, part.size, tuple(part.files), part.tarball, part.encrypted)
