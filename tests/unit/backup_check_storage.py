"""
In-memory storage creating backups by the real backup layout, for tests of check of backups.

Encryption, compression and tarballs are the same as in a real storage.
"""

import hashlib
import os
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator, Sequence
from unittest.mock import patch

import yaml

from ch_backup.backup.layout import BackupLayout
from ch_backup.backup.metadata import (
    BackupMetadata,
    BackupState,
    BackupStorageFormat,
    PartMetadata,
    TableMetadata,
)
from ch_backup.clickhouse.models import Database, FrozenPart
from ch_backup.config import Config

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
        *,
        data_skipped_reason: str | None = None,
    ) -> BackupMetadata:
        """
        Create a backup of the table db.t with own parts and parts linked to other backups.
        """
        meta = BackupMetadata(
            name, "1", "24.8", "%Y-%m-%d %H:%M:%S %z", encrypted=encrypted
        )
        meta.state = state
        self._schemas(meta, ["t"])
        meta.add_table(TableMetadata("db", "t", "MergeTree", None, data_skipped_reason))
        for part_name in parts:
            meta.add_part(self._part(meta, part_name))
        for part_name, source in (links or {}).items():
            own = self.meta(source).find_part("db", "t", part_name)
            assert own
            meta.add_part(copy_part(own, link=source))
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
                {
                    "all_1_1_0/data.bin": (
                        f"{version}\n{len(objects)}\t{sum(objects.values())}\n"
                        + "".join(
                            f"{size}\t{key_prefix}{key}\n"
                            for key, size in objects.items()
                        )
                        + tail
                    ).encode()
                },
            )
            for key, size in objects.items():
                self.engine.upload_data(b"x" * size, os.path.join(data_path, key))
        self.layout.upload_backup_metadata(meta)
        return meta

    def disk_metadata(
        self, meta: BackupMetadata, files: dict[str, bytes], old_style: bool = False
    ) -> None:
        """
        Upload disk metadata of parts of the table db.c keyed by file name.

        Old backups store disk metadata of all tables of a disk in one tarball.
        """
        disks_path = os.path.join(
            self.layout.get_cloud_storage_path(meta.name), "disks"
        )
        self.layout.upload_cloud_storage_metadata_files(
            meta,
            (
                os.path.join(disks_path, f"{DISK}.tar.gz")
                if old_style
                else os.path.join(disks_path, DISK, "db", "c.tar.gz")
            ),
            list(files.items()),
        )
        self.layout.wait()

    def access_control(
        self, backup_name: str, entities: Sequence[str], tarball: bool = True
    ) -> None:
        """
        Add access control entities to a backup, storing them as a tarball or a file per entity.
        """
        meta = self.meta(backup_name)
        meta.set_access_control([{"id": e, "name": e, "char": "U"} for e in entities])
        path = f"{self.layout.get_backup_path(backup_name)}/access_control"
        loader = self.layout._storage_loader  # pylint: disable=protected-access
        if tarball:
            loader.upload_data_tarball(
                [f"{e}.sql" for e in entities],
                [f"CREATE USER {e}".encode() for e in entities],
                f"{path}/access_control.tar",
                encryption=True,
            )
        else:
            meta.access_control.backup_format = BackupStorageFormat.PLAIN
            for e in entities:
                loader.upload_data(
                    f"CREATE USER {e}".encode(), f"{path}/{e}.sql", encryption=True
                )
        self.layout.upload_backup_metadata(meta)

    def adjust_part(self, backup_name: str, part_name: str, **fields: Any) -> None:
        """
        Change fields of a part in backup metadata.
        """
        meta = self.meta(backup_name)
        part = meta.find_part("db", "t", part_name)
        assert part
        meta.remove_parts(meta.get_table("db", "t"), [part])
        meta.add_part(copy_part(part, **fields))
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


def object_metadata(key: str, size: int) -> bytes:
    return f"2\n1\t{size}\n{size}\t{key}\n0\n".encode()


def copy_part(part: PartMetadata, **fields: Any) -> PartMetadata:
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


@contextmanager
def in_memory_storage(tmp_path: Path) -> Iterator[Storage]:
    """
    Create a storage in memory used by backup layout and checker instead of S3.
    """
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
