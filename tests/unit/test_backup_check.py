"""
Unit tests for check of backups.

Backups are created and checked by the real backup layout on top of an in-memory storage, so that
encryption, compression and tarballs are the same as in a real storage.
"""

import hashlib
import json
import os
import threading
from pathlib import Path
from typing import Any, Iterator, Sequence
from unittest.mock import MagicMock, patch

import pytest
import yaml
from click import Group
from click.testing import CliRunner

from ch_backup import cli
from ch_backup.backup.checker import BackupChecker, BackupCheckResult
from ch_backup.backup.layout import BackupLayout
from ch_backup.backup.metadata import (
    BackupMetadata,
    BackupState,
    PartMetadata,
    TableMetadata,
)
from ch_backup.clickhouse.models import Database, FrozenPart
from ch_backup.config import Config
from ch_backup.exceptions import BackupNotFound

PATH_ROOT = "ch_backup"
DISK = "s3"


class MemoryEngine:
    """
    Storage engine keeping objects in memory with the semantics of the S3 engine.
    """

    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}
        self._uploads: dict[str, dict[int, bytes]] = {}
        self._downloads: dict[str, list] = {}
        self._lock = threading.Lock()

    def upload_data(self, data: bytes, remote_path: str) -> str:
        self.objects[remote_path.lstrip("/")] = bytes(data)
        return remote_path

    def download_data(self, remote_path: str) -> bytes:
        return self.objects[remote_path.lstrip("/")]

    def list_dir(
        self, remote_path: str, recursive: bool = False, absolute: bool = False
    ) -> list[str]:
        prefix = remote_path.strip("/") + "/"
        result = set()
        for key in self.objects:
            if not key.startswith(prefix):
                continue
            path = key
            if not recursive and "/" in key[len(prefix) :]:
                path = prefix + key[len(prefix) :].split("/")[0] + "/"
            result.add(path if absolute else os.path.relpath(path, prefix))
        return sorted(result)

    def list_dir_with_sizes(self, remote_path: str) -> dict[str, int]:
        prefix = remote_path.strip("/") + "/"
        return {k: len(v) for k, v in self.objects.items() if k.startswith(prefix)}

    def path_exists(self, remote_path: str, is_dir: bool = False) -> bool:
        key = remote_path.strip("/")
        if is_dir:
            return any(k.startswith(key + "/") for k in self.objects)
        return key in self.objects

    def get_object_size(self, remote_path: str) -> int:
        return len(self.objects[remote_path.lstrip("/")])

    def create_multipart_upload(self, remote_path: str) -> str:
        with self._lock:
            self._uploads[remote_path] = {}
        return remote_path

    def upload_part(
        self, data: bytes, remote_path: str, upload_id: str, part_num: int = None
    ) -> None:
        assert upload_id == remote_path
        with self._lock:
            parts = self._uploads[upload_id]
            parts[len(parts) if part_num is None else part_num] = bytes(data)

    def complete_multipart_upload(self, remote_path: str, upload_id: str) -> None:
        with self._lock:
            parts = self._uploads.pop(upload_id)
            self.upload_data(b"".join(parts[num] for num in sorted(parts)), remote_path)

    def create_multipart_download(self, remote_path: str) -> str:
        download_id = f"{remote_path}#{id(object())}"
        with self._lock:
            self._downloads[download_id] = [self.download_data(remote_path), 0]
        return download_id

    def download_part(self, download_id: str, part_len: int = None) -> bytes | None:
        with self._lock:
            download = self._downloads[download_id]
        data, start = download
        if start >= len(data):
            return None
        part_len = part_len or 8 * 1024 * 1024
        download[1] = start + part_len
        return data[start : start + part_len]

    def complete_multipart_download(self, download_id: str) -> None:
        with self._lock:
            self._downloads.pop(download_id, None)


class Storage:
    """
    Creates and damages backups in the in-memory storage.
    """

    def __init__(self, engine: MemoryEngine, config: Config, tmp_path: Path) -> None:
        self.engine = engine
        self.config = config
        self._tmp_path = tmp_path
        self.layout = BackupLayout(config)

    def backup(
        self,
        name: str,
        parts: Sequence[str] = (),
        links: dict[str, str] | None = None,
        encrypted: bool = True,
        state: BackupState = BackupState.CREATED,
    ) -> BackupMetadata:
        """
        Create a backup of the table db.t with own parts and parts linked to other backups.
        """
        meta = BackupMetadata(
            name, "1", "24.8", "%Y-%m-%d %H:%M:%S %z", encrypted=encrypted
        )
        meta.state = state
        self._schemas(meta, ["t"])
        meta.add_table(TableMetadata("db", "t", "MergeTree", None))
        for part_name in parts:
            meta.add_part(self._part(meta, part_name))
        for part_name, source in (links or {}).items():
            own = self.meta(source).find_part("db", "t", part_name)
            assert own
            meta.add_part(_copy_part(own, link=source))
        self.layout.upload_backup_metadata(meta)
        return meta

    def cloud_backup(
        self,
        name: str,
        objects: dict[str, int] | None = None,
        link: str | None = None,
        data_copied: bool = True,
        full_keys: bool = False,
    ) -> BackupMetadata:
        """
        Create a backup of the table db.c with a part on a cloud storage disk.
        """
        meta = BackupMetadata(name, "1", "24.8", "%Y-%m-%d %H:%M:%S %z")
        meta.state = BackupState.CREATED
        meta.cloud_storage.add_disk(DISK)
        meta.cloud_storage.encrypted = True
        meta.cloud_storage.compressed = True
        meta.cloud_storage.data_copied = data_copied
        self._schemas(meta, ["c"])
        meta.add_table(TableMetadata("db", "c", "MergeTree", None))
        meta.add_part(
            PartMetadata(
                database="db",
                table="c",
                name="all_1_1_0",
                checksum="keys",
                size=15,
                files=["data.bin"],
                tarball=True,
                link=link,
                disk_name=DISK,
            )
        )
        if objects is not None:
            data_path = self.layout.get_cloud_storage_data_path(name, DISK)
            version, tail = ("5", "0\n0\n\n") if full_keys else ("2", "0\n")
            key_prefix = f"{data_path.strip('/')}/" if full_keys else ""
            self.disk_metadata(
                meta,
                (
                    f"{version}\n{len(objects)}\t{sum(objects.values())}\n"
                    + "".join(
                        f"{size}\t{key_prefix}{key}\n" for key, size in objects.items()
                    )
                    + tail
                ).encode(),
            )
            for key, size in objects.items():
                self.engine.upload_data(b"x" * size, os.path.join(data_path, key))
        self.layout.upload_backup_metadata(meta)
        return meta

    def disk_metadata(self, meta: BackupMetadata, data: bytes) -> None:
        """
        Upload disk metadata of the part of the table db.c.
        """
        self.layout.upload_cloud_storage_metadata_files(
            meta,
            os.path.join(
                self.layout.get_cloud_storage_path(meta.name),
                "disks",
                DISK,
                "db",
                "c.tar.gz",
            ),
            [("all_1_1_0/data.bin", data)],
        )
        self.layout.wait()

    def adjust_part(self, backup_name: str, part_name: str, **fields: Any) -> None:
        """
        Change fields of a part in backup metadata.
        """
        meta = self.meta(backup_name)
        part = meta.find_part("db", "t", part_name)
        assert part
        meta.remove_parts(meta.get_table("db", "t"), [part])
        meta.add_part(_copy_part(part, **fields))
        self.layout.upload_backup_metadata(meta)

    def meta(self, backup_name: str) -> BackupMetadata:
        """
        Return full metadata of a backup.
        """
        meta = self.layout.get_backup(backup_name)
        assert meta
        return meta

    def part_path(self, backup_name: str, part_name: str) -> str:
        """
        Return the key of a part tarball.
        """
        return f"{PATH_ROOT}/{backup_name}/data/db/t/{part_name}/{part_name}.tar"

    def _schemas(self, meta: BackupMetadata, tables: list[str]) -> None:
        meta.add_database(Database("default", "Atomic", "/default.sql", None, None))
        meta.add_database(Database("db", "Atomic", "/db.sql", None, None))
        backup_path = self.layout.get_backup_path(meta.name)
        loader = self.layout._storage_loader  # pylint: disable=protected-access
        loader.upload_data_tarball(
            ["db"],
            [b"CREATE DATABASE db"],
            f"{backup_path}/metadata/databases.tar",
            encryption=meta.encrypted,
        )
        loader.upload_data_tarball(
            tables,
            [b"CREATE TABLE db.t" for _ in tables],
            f"{backup_path}/metadata/db.tar",
            encryption=meta.encrypted,
        )

    def _part(self, meta: BackupMetadata, name: str) -> PartMetadata:
        part_dir = self._tmp_path / f"{meta.name}_{name}"
        part_dir.mkdir()
        files = {
            "checksums.txt": os.urandom(64),
            "columns.txt": b"columns",
            "data.bin": os.urandom(3000),
        }
        for file_name, data in files.items():
            (part_dir / file_name).write_bytes(data)
        frozen = FrozenPart(
            "db",
            "t",
            name,
            "default",
            str(part_dir),
            hashlib.md5(files["checksums.txt"]).hexdigest(),  # nosec
            sum(-(-len(data) // 512) * 512 for data in files.values()),
            sorted(files),
        )
        self.layout.upload_data_part(meta, frozen, lambda *_: None)
        self.layout.wait()
        return PartMetadata.from_frozen_part(frozen, meta.encrypted)


def _copy_part(part: PartMetadata, **fields: Any) -> PartMetadata:
    return PartMetadata(
        **{
            "database": part.database,
            "table": part.table,
            "name": part.name,
            "checksum": part.checksum,
            "size": part.size,
            "files": part.files,
            "tarball": True,
            "disk_name": part.disk_name,
            "encrypted": part.encrypted,
            **fields,
        }
    )


@pytest.fixture(name="storage")
def _storage(tmp_path: Path) -> Iterator[Storage]:
    config_file = tmp_path / "ch-backup.conf"
    config_file.write_text(
        yaml.dump(
            {
                "backup": {"path_root": PATH_ROOT},
                "encryption": {"key": "0123456789abcdef0123456789abcdef"},
                "multiprocessing": {"workers": 0},
            }
        )
    )
    engine = MemoryEngine()
    with (
        patch("ch_backup.storage.loader.get_storage_engine", return_value=engine),
        patch(
            "ch_backup.storage.async_pipeline.pipeline_builder.get_storage_engine",
            return_value=engine,
        ),
        patch("ch_backup.backup.checker.get_storage_engine", return_value=engine),
    ):
        yield Storage(engine, Config(str(config_file)), tmp_path)


def _check(storage: Storage, deep: bool, names: list[str] | None = None) -> dict:
    checker = BackupChecker(storage.config, deep=deep, workers=4)
    return {result.name: result for result in checker.check(names)}


def _broken(results: dict) -> set[str]:
    return {name for name, result in results.items() if result.broken}


@pytest.mark.parametrize("deep", [False, True])
def test_intact_backups(storage: Storage, deep: bool) -> None:
    storage.backup("b1", parts=["all_1_1_0", "all_2_2_0"])
    storage.backup("b2", parts=["all_3_3_0"], links={"all_1_1_0": "b1"})
    storage.cloud_backup("c1", objects={"abc/obj1": 10, "abc/obj2": 5})
    storage.cloud_backup("c2", link="c1")

    results = _check(storage, deep)

    assert set(results) == {"b1", "b2", "c1", "c2"}
    assert _broken(results) == set()
    assert all(r.state == "created" and r.unverified == 0 for r in results.values())


@pytest.mark.parametrize("deep", [False, True])
def test_missing_part_breaks_linking_backups(storage: Storage, deep: bool) -> None:
    storage.backup("b1", parts=["all_1_1_0", "all_2_2_0"])
    storage.backup("b2", links={"all_1_1_0": "b1"})
    storage.backup("b3", links={"all_2_2_0": "b1"})
    del storage.engine.objects[storage.part_path("b1", "all_1_1_0")]

    results = _check(storage, deep)

    assert _broken(results) == {"b1", "b2"}
    assert results["b2"].problems == [
        'part all_1_1_0 of "db"."t" in backup "b1": data is missing or has wrong size'
    ]


def test_corrupted_part_content_is_found_by_deep_check(storage: Storage) -> None:
    storage.backup("b1", parts=["all_1_1_0"])
    storage.backup("b2", links={"all_1_1_0": "b1"})
    key = storage.part_path("b1", "all_1_1_0")
    storage.engine.objects[key] = os.urandom(len(storage.engine.objects[key]))

    assert _broken(_check(storage, deep=False)) == set()
    results = _check(storage, deep=True)
    assert _broken(results) == {"b1", "b2"}
    assert "CryptoError" in results["b2"].problems[0]


@pytest.mark.parametrize(
    "fields, problem",
    [
        ({"checksum": "0" * 32}, "checksums.txt differs from metadata"),
        (
            {"files": ["checksums.txt", "columns.txt", "other.bin"]},
            "files differ from metadata: missing ['other.bin'], unexpected ['data.bin']",
        ),
    ],
)
def test_part_differing_from_metadata_is_found_by_deep_check(
    storage: Storage, fields: dict, problem: str
) -> None:
    storage.backup("b1", parts=["all_1_1_0"])
    storage.adjust_part("b1", "all_1_1_0", **fields)

    assert _broken(_check(storage, deep=False)) == set()
    results = _check(storage, deep=True)
    assert results["b1"].problems == [
        f'part all_1_1_0 of "db"."t" in backup "b1": {problem}'
    ]


@pytest.mark.parametrize("deep", [False, True])
def test_metadata_of_each_linking_backup_is_checked(
    storage: Storage, deep: bool
) -> None:
    storage.backup("b1", parts=["all_1_1_0"])
    storage.backup("b2", links={"all_1_1_0": "b1"})
    storage.backup("b3", links={"all_1_1_0": "b1"})
    storage.adjust_part("b3", "all_1_1_0", link="b1", size=512)

    results = _check(storage, deep)

    assert _broken(results) == {"b3"}
    assert results["b3"].problems == [
        'part all_1_1_0 of "db"."t" in backup "b1": data is missing or has wrong size'
    ]


def test_part_stored_in_other_format_than_in_metadata(storage: Storage) -> None:
    storage.backup("b1", parts=["all_1_1_0"])
    storage.adjust_part("b1", "all_1_1_0", tarball=False)

    assert _broken(_check(storage, deep=False)) == {"b1"}


def test_unencrypted_part_content_is_reported_as_unverified(storage: Storage) -> None:
    storage.backup("b1", parts=["all_1_1_0"], encrypted=False)

    results = _check(storage, deep=True)

    assert _broken(results) == set()
    assert results["b1"].unverified == 1


@pytest.mark.parametrize(
    "path, problem",
    [
        ("backup_struct.json", "metadata: "),
        ("backup_light_struct.json", "light metadata: "),
        ("metadata/databases.tar", "database schemas: "),
        ("metadata/db.tar", 'table schemas of "db": '),
    ],
)
def test_unreadable_metadata_and_schemas(
    storage: Storage, path: str, problem: str
) -> None:
    storage.backup("b1", parts=["all_1_1_0"])
    storage.backup("b2", parts=["all_2_2_0"])
    storage.engine.objects[f"{PATH_ROOT}/b1/{path}"] = b"garbage"

    results = _check(storage, deep=False)

    assert _broken(results) == {"b1"}
    assert results["b1"].problems[0].startswith(problem)


def test_parts_linked_to_backup_with_unreadable_metadata_are_checked(
    storage: Storage,
) -> None:
    storage.backup("b1", parts=["all_1_1_0"])
    storage.backup("b2", links={"all_1_1_0": "b1"})
    storage.engine.objects[f"{PATH_ROOT}/b1/backup_struct.json"] = b"garbage"

    assert _broken(_check(storage, deep=True)) == {"b1"}
    del storage.engine.objects[storage.part_path("b1", "all_1_1_0")]
    results = _check(storage, deep=True)
    assert _broken(results) == {"b1", "b2"}
    assert results["b2"].problems == [
        'part all_1_1_0 of "db"."t" in backup "b1": data is missing or has wrong size'
    ]


@pytest.mark.parametrize(
    "damage, problem",
    [
        ("delete", "1 objects of copied data are missing or have wrong size"),
        ("resize", "1 objects of copied data are missing or have wrong size"),
        ("metadata", "nonce must be exactly 24 bytes long"),
    ],
)
def test_broken_cloud_storage_data(storage: Storage, damage: str, problem: str) -> None:
    storage.cloud_backup("c1", objects={"abc/obj1": 10, "abc/obj2": 5})
    storage.cloud_backup("c2", link="c1")
    data_key = f"{PATH_ROOT}/c1/cloud_storage/{DISK}/abc/obj2"
    if damage == "delete":
        del storage.engine.objects[data_key]
    elif damage == "resize":
        storage.engine.objects[data_key] = b"y"
    else:
        storage.engine.objects[f"{PATH_ROOT}/c1/disks/{DISK}/db/c.tar.gz"] = b"garbage"

    results = _check(storage, deep=False)

    assert _broken(results) == {"c1", "c2"}
    assert problem in results["c2"].problems[0]


@pytest.mark.parametrize("full_keys", [False, True])
def test_object_with_the_same_key_suffix_is_not_taken_for_copied_data(
    storage: Storage, full_keys: bool
) -> None:
    storage.cloud_backup("c1", objects={"abc/obj1": 10}, full_keys=full_keys)
    data_path = f"{PATH_ROOT}/c1/cloud_storage/{DISK}"

    assert _broken(_check(storage, deep=False)) == set()
    storage.engine.objects[f"{data_path}/obj1"] = storage.engine.objects.pop(
        f"{data_path}/abc/obj1"
    )
    assert _broken(_check(storage, deep=False)) == {"c1"}


def test_copied_data_of_a_disk_is_listed_once(storage: Storage) -> None:
    storage.cloud_backup("c1", objects={"abc/obj1": 10})
    storage.cloud_backup("c2", link="c1")
    listings = []
    list_dir_with_sizes = storage.engine.list_dir_with_sizes
    storage.engine.list_dir_with_sizes = lambda path: listings.append(  # type: ignore
        path
    ) or list_dir_with_sizes(path)

    assert _broken(_check(storage, deep=False)) == set()
    assert listings == [f"{PATH_ROOT}/c1/cloud_storage/{DISK}"]


def test_missing_disk_metadata_of_linked_part(storage: Storage) -> None:
    storage.cloud_backup("c1", objects={"abc/obj1": 10})
    storage.cloud_backup("c2", link="c1")
    storage.cloud_backup("c3", objects=None)

    results = _check(storage, deep=False)

    assert _broken(results) == {"c3"}
    assert (
        "disk metadata is missing for parts ['all_1_1_0']" in results["c3"].problems[0]
    )


def test_not_copied_cloud_storage_data_is_reported_as_unverified(
    storage: Storage,
) -> None:
    storage.cloud_backup(
        "c1", objects={"abc/obj1": 10, "abc/obj2": 5}, data_copied=False
    )

    results = _check(storage, deep=False)

    assert _broken(results) == set()
    assert results["c1"].unverified == 2


def test_malformed_disk_metadata_of_not_copied_data(storage: Storage) -> None:
    meta = storage.cloud_backup("c1", objects={}, data_copied=False)
    storage.disk_metadata(meta, b"garbage")

    results = _check(storage, deep=False)

    assert _broken(results) == {"c1"}
    assert 'in backup "c1": ValueError: ' in results["c1"].problems[0]


def test_only_specified_backups_are_checked(storage: Storage) -> None:
    storage.backup("b1", parts=["all_1_1_0"])
    storage.backup("b2", parts=["all_2_2_0"])
    del storage.engine.objects[storage.part_path("b2", "all_2_2_0")]

    assert list(_check(storage, deep=False, names=["b1"])) == ["b1"]
    with pytest.raises(BackupNotFound):
        _check(storage, deep=False, names=["b3"])


def test_missing_light_metadata(storage: Storage) -> None:
    storage.backup("b1", parts=["all_1_1_0"])
    del storage.engine.objects[f"{PATH_ROOT}/b1/backup_light_struct.json"]

    results = _check(storage, deep=False)

    assert results["b1"].problems == ["light metadata: missing"]
    assert results["b1"].state == "created"


def test_create_statement_missing_in_schemas(storage: Storage) -> None:
    storage.backup("b1", parts=["all_1_1_0"])
    storage.layout._storage_loader.upload_data_tarball(  # pylint: disable=protected-access
        ["other"],
        [b"CREATE TABLE db.other"],
        f"{PATH_ROOT}/b1/metadata/db.tar",
        encryption=True,
    )

    results = _check(storage, deep=False)

    assert results["b1"].problems == ["table schemas of \"db\": missing for ['t']"]


def test_tables_of_external_databases_are_not_checked(storage: Storage) -> None:
    storage.backup("b1", parts=["all_1_1_0"])
    meta = storage.meta("b1")
    meta.add_database(Database("ext", "MySQL", "/ext.sql", None, None))
    meta.add_table(TableMetadata("ext", "x", "MySQL", None))
    storage.layout.upload_backup_metadata(meta)
    storage.layout._storage_loader.upload_data_tarball(  # pylint: disable=protected-access
        ["db", "ext"],
        [b"CREATE DATABASE db", b"CREATE DATABASE ext ENGINE = MySQL"],
        f"{PATH_ROOT}/b1/metadata/databases.tar",
        encryption=True,
    )

    assert _broken(_check(storage, deep=False)) == set()


def test_cloud_storage_data_linked_from_unchecked_backup(storage: Storage) -> None:
    storage.cloud_backup("c1", objects={"abc/obj1": 10})
    storage.cloud_backup("c2", link="c1")

    assert _broken(_check(storage, deep=False, names=["c2"])) == set()
    storage.engine.objects[f"{PATH_ROOT}/c1/cloud_storage/{DISK}/abc/obj1"] = b"y"
    assert _broken(_check(storage, deep=False, names=["c2"])) == {"c2"}


def test_cloud_storage_directories_of_backups_are_not_checked(storage: Storage) -> None:
    storage.cloud_backup("c-1", objects={"abc/obj1": 10})

    results = _check(storage, deep=False)

    assert list(results) == ["c-1"]
    assert _broken(results) == set()


@pytest.mark.parametrize("damage", ["not copied", "no light metadata"])
def test_linked_cloud_storage_data_must_be_copied(
    storage: Storage, damage: str
) -> None:
    storage.cloud_backup(
        "c1", objects={"abc/obj1": 10}, data_copied=damage != "not copied"
    )
    storage.cloud_backup("c2", link="c1")
    if damage == "no light metadata":
        del storage.engine.objects[f"{PATH_ROOT}/c1/backup_light_struct.json"]

    results = _check(storage, deep=False)

    assert _broken(results) == ({"c2"} if damage == "not copied" else {"c1", "c2"})
    assert results["c2"].problems == [
        'disk s3 of "db"."c" in backup "c1": '
        'backup "c1" holding the data is missing or holds no copied data'
    ]


@pytest.mark.parametrize("format_", ["json", "table"])
def test_check_command(format_: str) -> None:
    backup = MagicMock()
    backup.check.return_value = [
        BackupCheckResult("b1", "created", [], 1),
        BackupCheckResult("b2", "created", ["part all_1_1_0: broken"], 0),
    ]

    result = CliRunner(mix_stderr=False).invoke(
        Group(commands=[cli.check_command]),
        ["check", "b1", "b2", "--deep", "--format", format_],
        obj={"backup": backup},
    )

    assert result.exit_code == 2
    backup.check.assert_called_once_with(
        ["b1", "b2"], deep=True, workers=4, tmp_dir=None
    )
    if format_ == "json":
        rows = json.loads(result.stdout)
        assert [(r["name"], r["status"], r["unverified"]) for r in rows] == [
            ("b1", "ok", 1),
            ("b2", "broken", 0),
        ]
        assert rows[1]["problems"] == ["part all_1_1_0: broken"]
    else:
        rows = [line.split() for line in result.stdout.splitlines()[2:]]
        assert [row[:4] for row in rows] == [
            ["b1", "created", "ok", "1"],
            ["b2", "created", "broken", "0"],
        ]
        assert rows[1][4:] == ["part", "all_1_1_0:", "broken"]


def test_check_command_requires_backups_or_all() -> None:
    backup = MagicMock()

    result = CliRunner().invoke(
        Group(commands=[cli.check_command]), ["check"], obj={"backup": backup}
    )

    assert result.exit_code == 2
    assert "Either BACKUP or --all must be specified" in result.output
    backup.check.assert_not_called()


@pytest.mark.parametrize(
    "state, exit_code",
    [
        ("created", 2),
        (None, 2),
        ("failed", 0),
        ("creating", 0),
        ("partially_deleted", 0),
    ],
)
def test_check_command_exit_code(state: str | None, exit_code: int) -> None:
    backup = MagicMock()
    backup.check.return_value = [BackupCheckResult("b1", state, ["broken"], 0)]

    result = CliRunner().invoke(
        Group(commands=[cli.check_command]), ["check", "--all"], obj={"backup": backup}
    )

    assert result.exit_code == exit_code
