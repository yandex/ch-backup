"""
Check of backups: metadata, schemas and data are readable and in place.
"""

import hashlib
import os
import tempfile
import threading
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from typing import Callable, Iterable, NamedTuple, Sequence

from ch_backup import logging
from ch_backup.backup.copier import FROZEN_METADATA_FNAME
from ch_backup.backup.layout import ACCESS_CONTROL_FNAME, BackupLayout
from ch_backup.backup.metadata import (
    BackupMetadata,
    BackupStorageFormat,
    PartMetadata,
    TableMetadata,
    sanitize_backup_name,
)
from ch_backup.clickhouse.object_storage_metadata import parse_object_storage_metadata
from ch_backup.config import Config
from ch_backup.exceptions import BackupNotFound
from ch_backup.storage.engine import get_storage_engine
from ch_backup.util import escape_metadata_file_name, list_dir_files


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

    @property
    def broken(self) -> bool:
        """
        Return True if the backup can't be restored completely.
        """
        return len(self.problems) != 0


class _Listing:
    """
    Listing of stored objects, filled once and dropped after its last user.
    """

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.sizes: dict | None = None
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
    """
    Checks backups reading them only.

    Each stored object is checked once, and its problem goes to every backup needing it.
    """

    def __init__(
        self, config: Config, deep: bool, workers: int, tmp_dir: str | None = None
    ) -> None:
        self._config = config.derive(patch={"multiprocessing": {"workers": 0}})
        self._deep = deep
        self._workers = workers
        self._tmp_dir = tmp_dir
        self._local = threading.local()
        self._metas: dict[str, BackupMetadata] = {}
        self._light_metas: dict[str, BackupMetadata | None] = {}
        self._problems: dict[str, list[str]] = defaultdict(list)
        self._objects: dict[_ObjectKey, dict] = {}
        self._listings: dict[tuple, _Listing] = {}

    def check(
        self, backup_names: Sequence[str] | None = None
    ) -> list[BackupCheckResult]:
        """
        Check specified backups or all backups if names are not specified.
        """
        existing = set(self._layout().get_backup_names())
        if backup_names:
            names = list(backup_names)
            for name in names:
                if name not in existing:
                    raise BackupNotFound(name)
        else:
            names = sorted(existing - _cloud_storage_dirs(existing))

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

        return self._results(names, results)

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
        problems = self._problems[name]
        try:
            self._light_metas[name] = layout.get_backup(name, use_light_meta=True)
            if not self._light_metas[name]:
                problems.append("light metadata: missing")
        except Exception as e:
            self._light_metas[name] = None
            problems.append(f"light metadata: {_describe(e)}")

        try:
            backup = layout.get_backup(name)
            entries = _entries(backup) if backup else []
        except Exception as e:
            problems.append(f"metadata: {_describe(e)}")
            return []
        if not backup:
            problems.append("metadata: missing")
            return []

        self._metas[name] = backup
        self._check_schemas(layout, backup, problems)
        try:
            self._check_access_control(layout, backup, problems)
        except Exception as e:
            problems.append(f"access control: {_describe(e)}")
        return entries

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
            problems.append(f"database schemas: {_describe(e)}")

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
                problems.append(f'table schemas of "{db.name}": {_describe(e)}')

    def _check_access_control(
        self, layout: BackupLayout, backup: BackupMetadata, problems: list[str]
    ) -> None:
        """
        Check that access control entities are present, deep check downloads them as restore does.
        """
        access_control = backup.access_control
        if not access_control.acl_ids:
            return
        entities = {f"{acl_id}.sql" for acl_id in access_control.acl_ids}
        tarball = access_control.backup_format == BackupStorageFormat.TAR
        file_names = [ACCESS_CONTROL_FNAME] if tarball else sorted(entities)
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
            if missing := sorted(entities - set(list_dir_files(access_dir))):
                problems.append(f"access control: missing entities {missing}")

    def _collect_objects(self, entries: dict[str, list[tuple]]) -> None:
        """
        Group parts of all backups by the stored object holding their data.

        A local part is held by its tarball, parts on a cloud storage disk by disk metadata of
        the table. Linked parts and parts with different metadata are grouped apart, since
        restore reads them differently.
        """
        for name, backup in self._metas.items():
            for table, table_paths, part, key, files in entries[name]:
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
            return _ObjectCheckResult(_describe(e))
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
            lambda: layout.get_table_data_files(key.source, key.database, key.table),
        )
        if not layout.check_data_part_files(
            part, stored.get(part.deduplicated_part_name, {})
        ):
            return _ObjectCheckResult("data is missing or has wrong size")
        if not self._deep:
            return _ObjectCheckResult()

        with tempfile.TemporaryDirectory(
            dir=self._tmp_dir, ignore_cleanup_errors=True
        ) as part_dir:
            layout.download_data_part(backup, part, part_dir, callback=lambda *_: None)
            files = set(list_dir_files(part_dir))
            if files != set(part.files):
                return _ObjectCheckResult(
                    f"files differ from metadata: {_difference(set(part.files), files)}"
                )
            if "checksums.txt" in files:
                with open(os.path.join(part_dir, "checksums.txt"), "rb") as f:
                    if hashlib.md5(f.read()).hexdigest() != part.checksum:  # nosec
                        return _ObjectCheckResult("checksums.txt differs from metadata")
            return _ObjectCheckResult(unverified=int(not part.encrypted))

    def _check_cloud_storage_data(
        self, key: _ObjectKey, obj: dict
    ) -> _ObjectCheckResult | _DiskCheckResult:
        """
        Check disk metadata of a table and presence and size of copied objects it refers to.

        Not copied objects stay in the bucket of the disk and are not checked. As restore does,
        linked data is read by light metadata of the holding backup.
        """
        backup = self._light_meta(key.source) if key.linked else self._metas[key.source]
        if not backup or (key.linked and not backup.cloud_storage.data_copied):
            return _ObjectCheckResult(
                f'backup "{key.source}" holding the data is missing or holds no copied data'
            )

        layout = self._layout()
        files = []
        for remote_path in layout.get_cloud_storage_metadata_remote_paths(
            backup.get_sanitized_name(),
            key.item,
            backup.cloud_storage.compressed,
            [obj["table"]],
        ):
            files += layout.get_cloud_storage_metadata_files(backup, remote_path)
        parts: set[str] = set().union(*obj["backups"].values())
        found = _files_of_parts(files, parts, obj["table_paths"])
        prefix = layout.get_cloud_storage_data_path(key.source, key.item).strip("/")
        stored, unreadable = _stored_objects_of_parts(found, prefix)
        broken, unverified = {}, stored
        if backup.cloud_storage.data_copied:
            listed = self._listing(
                key,
                lambda: get_storage_engine(self._config["storage"]).list_dir_with_sizes(
                    prefix
                ),
            )
            broken = {
                part: {item for item in items if listed.get(item[0]) != item[1]}
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

    def _listing(self, key: _ObjectKey, list_objects: Callable[[], dict]) -> dict:
        """
        Return the listing of stored objects for an object, listing them once for all its users.
        """
        listing = self._listings[_listing_key(key)]
        with listing.lock:
            if listing.sizes is None:
                listing.sizes = list_objects()
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

    def _light_meta(self, name: str) -> BackupMetadata | None:
        """
        Return light metadata of a backup, or None if it's missing or unreadable.
        """
        if name not in self._light_metas:
            try:
                self._light_metas[name] = self._layout().get_backup(
                    name, use_light_meta=True
                )
            except Exception:
                self._light_metas[name] = None
        return self._light_metas[name]

    def _results(
        self,
        names: list[str],
        results: dict[_ObjectKey, _ObjectCheckResult | _DiskCheckResult],
    ) -> list[BackupCheckResult]:
        """
        Attribute results of stored objects to the backups linking to them.
        """
        problems = {name: list(self._problems[name]) for name in names}
        unverified: dict[str, int] = defaultdict(int)
        for key, object_result in results.items():
            for name, parts in self._objects[key]["backups"].items():
                result = object_result.for_parts(parts)
                if result.problem:
                    problems[name].append(f"{_object_name(key)}: {result.problem}")
                else:
                    unverified[name] += result.unverified

        results_by_name = []
        for name in names:
            meta = self._metas.get(name)
            backup = meta or self._light_metas.get(name)
            state = backup.state.value if backup else None
            results_by_name.append(
                BackupCheckResult(
                    name, state, problems[name], unverified[name], _skipped_data(meta)
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
        if not reason and table.skipped_parts:
            reason = f"skipped parts {sorted(table.skipped_parts)}"
        if reason:
            skipped.append(f'"{table.database}"."{table.name}": {reason}')
    return skipped


def _missing(names: list[str], statements: list[tuple[str, str]]) -> list[str]:
    """
    Return names that have no create statement.
    """
    return sorted(set(names) - {name for name, _ in statements})


def _files_of_parts(
    files: list[tuple[str, bytes]], parts: set[str], table_paths: set[str]
) -> dict[str, dict[str, bytes]]:
    """
    Return files of disk metadata keyed by part name and path relative to the part.

    Paths start with the path of the table on disk. Old backups store disk metadata of all
    tables of a disk in one tarball, then only files under the path of the table are taken.
    """
    entries = []
    for file_name, data in files:
        path = file_name.split("/")
        for i, component in enumerate(path[:-1]):
            if component in parts:
                entries.append(("/".join(path[:i]), component, path[i + 1 :], data))
                break
    if len({table_path for table_path, *_ in entries}) > 1:
        entries = [entry for entry in entries if entry[0] in table_paths]

    result: dict[str, dict[str, bytes]] = {}
    for _, part, path, data in entries:
        files_of_part = result.setdefault(part, {})
        if path[-1] != FROZEN_METADATA_FNAME:
            files_of_part["/".join(path)] = data
    return result


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
    found: dict[str, dict[str, bytes]],
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


def _stored_objects_of_parts(
    found: dict[str, dict[str, bytes]], prefix: str
) -> tuple[dict[str, set[tuple[str, int]]], dict[str, str]]:
    """
    Return objects referenced from disk metadata of each part, and problems of unreadable parts.
    """
    stored, unreadable = {}, {}
    for part, files in found.items():
        try:
            stored[part] = _stored_objects(files.values(), prefix)
        except Exception as e:
            unreadable[part] = f"{part}: {_describe(e)}"
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


def _describe(error: BaseException) -> str:
    """
    Return the root cause of an error in a single line.
    """
    while error.__cause__ is not None:
        error = error.__cause__
    message = (str(error).strip().splitlines() or [""])[0]
    return f"{type(error).__name__}: {message}"
