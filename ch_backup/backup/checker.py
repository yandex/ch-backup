"""
Check of backups: metadata, schemas and data are readable and in place.
"""

import hashlib
import os
import shutil
import tempfile
import threading
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from typing import NamedTuple, Sequence

from ch_backup import logging
from ch_backup.backup.copier import FROZEN_METADATA_FNAME
from ch_backup.backup.layout import BackupLayout
from ch_backup.backup.metadata import (
    BackupMetadata,
    PartMetadata,
    TableMetadata,
    sanitize_backup_name,
)
from ch_backup.clickhouse.object_storage_metadata import parse_object_storage_metadata
from ch_backup.config import Config
from ch_backup.exceptions import BackupNotFound
from ch_backup.storage.engine import get_storage_engine
from ch_backup.util import list_dir_files


class BackupCheckResult(NamedTuple):
    """
    Result of a backup check.

    Objects that are in place but whose content can't be verified are counted in `unverified`.
    """

    name: str
    state: str | None
    problems: list[str]
    unverified: int

    @property
    def broken(self) -> bool:
        """
        Return True if the backup can't be restored completely.
        """
        return bool(self.problems)


class _Listing:
    """
    Listing of copied objects of a disk, filled once.
    """

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.sizes: dict[str, int] | None = None


class _ObjectCheckResult(NamedTuple):
    problem: str | None = None
    unverified: int = 0


class BackupChecker:
    """
    Checks backups reading them only.

    Data of a part may be stored in another backup it links to. Each stored object is checked once,
    and its problem is reported for every backup that links to it.
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
        self._objects: dict[tuple, dict] = {}
        self._listings: dict[str, _Listing] = {}

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
            list(pool.map(self._load, names))
            self._collect_objects()
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

    def _load(self, name: str) -> None:
        """
        Read light and full metadata and schemas of a backup.
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
        except Exception as e:
            problems.append(f"metadata: {_describe(e)}")
            return
        if not backup:
            problems.append("metadata: missing")
            return

        self._metas[name] = backup
        self._check_schemas(layout, backup, problems)

    @staticmethod
    def _check_schemas(
        layout: BackupLayout, backup: BackupMetadata, problems: list[str]
    ) -> None:
        """
        Check that create statements of databases and tables are readable and present.

        Databases and tables are taken as restore takes them: create statements of databases
        with embedded metadata are not stored, tables of external databases are not restored.
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

    def _collect_objects(self) -> None:
        """
        Group parts of all backups by the stored object holding their data.

        Data of a part on a local disk is a tarball of the part, data of parts on a cloud storage
        disk is described by disk metadata of the whole table. Linked and own parts on a cloud
        storage disk are checked apart, since restore reads data of linked parts differently.
        Restore reads a part by metadata of the restored backup, so parts linking to the same
        tarball with different metadata are checked apart too.
        """
        for backup in self._metas.values():
            for table in backup.get_tables():
                for part in table.get_parts():
                    cloud = part.disk_name in backup.cloud_storage.disks
                    key = (
                        "disk" if cloud else "part",
                        part.link or backup.name,
                        table.database,
                        table.name,
                        part.disk_name if cloud else part.deduplicated_part_name,
                        cloud and part.link is not None,
                        None if cloud else _part_fields(part),
                    )
                    obj = self._objects.setdefault(
                        key,
                        {
                            "backup": backup,
                            "part": part,
                            "table": table,
                            "parts": set(),
                            "backups": set(),
                        },
                    )
                    obj["parts"].add(part.deduplicated_part_name)
                    obj["backups"].add(backup.name)

    def _check_object(self, key: tuple) -> _ObjectCheckResult:
        """
        Check a stored object and return its problem if there is any.
        """
        obj = self._objects[key]
        try:
            if key[0] == "part":
                return self._check_part(obj["backup"], obj["part"])
            return self._check_cloud_storage_data(key, obj)
        except Exception as e:
            return _ObjectCheckResult(_describe(e))

    def _check_part(
        self, backup: BackupMetadata, part: PartMetadata
    ) -> _ObjectCheckResult:
        """
        Check presence and size of a part tarball.

        Deep check downloads the part as restore does, so encrypted data is verified completely.
        Content of not encrypted data is verified only for checksums.txt of the part.
        """
        layout = self._layout()
        if not layout.check_data_part(backup.name, part):
            return _ObjectCheckResult("data is missing or has wrong size")
        if not self._deep:
            return _ObjectCheckResult()

        part_dir = tempfile.mkdtemp(dir=self._tmp_dir)
        try:
            layout.download_data_part(backup, part, part_dir, callback=lambda *_: None)
            files = set(list_dir_files(part_dir))
            if files != set(part.files):
                return _ObjectCheckResult(
                    f"files differ from metadata: missing {sorted(set(part.files) - files)},"
                    f" unexpected {sorted(files - set(part.files))}"
                )
            if "checksums.txt" in files:
                with open(os.path.join(part_dir, "checksums.txt"), "rb") as f:
                    if hashlib.md5(f.read()).hexdigest() != part.checksum:  # nosec
                        return _ObjectCheckResult("checksums.txt differs from metadata")
            return _ObjectCheckResult(unverified=int(not part.encrypted))
        finally:
            shutil.rmtree(part_dir, ignore_errors=True)

    def _check_cloud_storage_data(self, key: tuple, obj: dict) -> _ObjectCheckResult:
        """
        Check disk metadata of a table and, if the data is copied into the backup, presence and
        size of the objects it refers to.

        Objects of data that is not copied stay in the bucket of the disk and are not checked.
        As restore does, the backup holding data of linked parts is read by its light metadata
        and must hold copied data.
        """
        _, source, _, _, disk_name, linked, _ = key
        table: TableMetadata = obj["table"]
        parts: set[str] = obj["parts"]
        backup = self._light_meta(source) if linked else self._metas[source]
        if not backup or (linked and not backup.cloud_storage.data_copied):
            return _ObjectCheckResult(
                f'backup "{source}" holding the data is missing or holds no copied data'
            )

        layout = self._layout()
        files = []
        for remote_path in layout.get_cloud_storage_metadata_remote_paths(
            backup.get_sanitized_name(),
            disk_name,
            backup.cloud_storage.compressed,
            [table],
        ):
            files += layout.get_cloud_storage_metadata_files(backup, remote_path)
        files = [
            (file_name, data)
            for file_name, data in files
            if set(file_name.split("/")[:-1]) & parts
        ]

        found = {c for file_name, _ in files for c in file_name.split("/")[:-1]} & parts
        if found != parts:
            return _ObjectCheckResult(
                f"disk metadata is missing for parts {sorted(parts - found)}"
            )

        prefix = layout.get_cloud_storage_data_path(source, disk_name).strip("/")
        stored = _stored_objects(files, prefix)
        if not backup.cloud_storage.data_copied:
            return _ObjectCheckResult(unverified=len(stored))

        objects = self._list_cloud_storage_data(prefix)
        broken = sum(objects.get(full_key) != size for full_key, size in stored)
        if broken:
            return _ObjectCheckResult(
                f"{broken} objects of copied data are missing or have wrong size"
            )
        return _ObjectCheckResult()

    def _list_cloud_storage_data(self, prefix: str) -> dict[str, int]:
        """
        Return sizes of copied objects of a disk keyed by their full keys.

        The listing may be huge, so threads needing it at once wait for the one listing it.
        """
        listing = self._listings.setdefault(prefix, _Listing())
        with listing.lock:
            if listing.sizes is None:
                engine = get_storage_engine(self._config["storage"])
                listing.sizes = engine.list_dir_with_sizes(prefix)
            return listing.sizes

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
        self, names: list[str], results: dict[tuple, _ObjectCheckResult]
    ) -> list[BackupCheckResult]:
        """
        Attribute results of stored objects to the backups linking to them.
        """
        problems = {name: list(self._problems[name]) for name in names}
        unverified: dict[str, int] = defaultdict(int)
        for key, result in results.items():
            kind, source, database, table, item, *_ = key
            for name in self._objects[key]["backups"]:
                if result.problem:
                    problems[name].append(
                        f'{kind} {item} of "{database}"."{table}" in backup "{source}": {result.problem}'
                    )
                else:
                    unverified[name] += result.unverified

        results_by_name = []
        for name in names:
            backup = self._metas.get(name) or self._light_metas.get(name)
            state = backup.state.value if backup else None
            results_by_name.append(
                BackupCheckResult(name, state, problems[name], unverified[name])
            )
        return results_by_name


def _cloud_storage_dirs(names: set[str]) -> set[str]:
    """
    Return directories holding only cloud storage data of backups.

    Such a directory is named after the sanitized name of a backup whose name differs from it.
    """
    return names & {
        sanitize_backup_name(name)
        for name in names
        if sanitize_backup_name(name) != name
    }


def _missing(names: list[str], statements: list[tuple[str, str]]) -> list[str]:
    """
    Return names that have no create statement.
    """
    return sorted(set(names) - {name for name, _ in statements})


def _stored_objects(
    files: list[tuple[str, bytes]], prefix: str
) -> set[tuple[str, int]]:
    """
    Return full keys and sizes of objects referenced from disk metadata files.

    A key in disk metadata is either full in its bucket or relative to the key prefix of the disk.
    ClickHouse writes copied data through a disk whose key prefix is the data path of the backup,
    so full keys of copied data start with it.
    """
    stored = set()
    for file_name, data in files:
        if os.path.basename(file_name) == FROZEN_METADATA_FNAME:
            continue
        metadata = parse_object_storage_metadata(data)
        stored |= {
            (
                item.key if metadata.full_object_keys else f"{prefix}/{item.key}",
                item.size,
            )
            for item in metadata.objects
        }
    return stored


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
