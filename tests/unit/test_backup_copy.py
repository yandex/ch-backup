"""
Unit tests for copying of backups between storages.
"""

# pylint: disable=too-many-lines

import copy
import gzip
import json
import os
from concurrent.futures import BrokenExecutor
from functools import partial
from typing import Any
from unittest.mock import MagicMock, call, patch

import pytest

from ch_backup import logging as ch_logging
from ch_backup.backup.copier import BackupCopier
from ch_backup.backup.copy_context import CopyContext
from ch_backup.backup.copy_report import CopyReport, print_copy_report
from ch_backup.backup.layout import BackupLayout
from ch_backup.backup.metadata import (
    BackupMetadata,
    BackupState,
    DetachedPartMetadata,
)
from ch_backup.config import DEFAULT_CONFIG
from ch_backup.exceptions import (
    ClickhouseBackupError,
    ConfigurationError,
    StorageError,
    TerminatingSignal,
)
from ch_backup.storage.object_copy import CopyEntry, storage_id

TIME_FORMAT = "%Y-%m-%d %H:%M:%S %z"
SRC_BUCKET = "backups"
DST_BUCKET = "backups-copy"
ENDPOINT = "http://s3.test:9000"


class FakeEngine:
    """
    Storage engine keeping objects in memory.
    """

    def __init__(self, store: "FakeStore", storage: dict) -> None:
        self._store = store
        self._id = storage_id(storage)
        self._objects = store.buckets.setdefault(self._id, {})

    def list_dir_with_sizes(self, remote_path: str) -> dict[str, int]:
        prefix = remote_path.strip("/") + "/"
        if prefix in self._store.list_errors:
            raise StorageError(f"Failed to list {prefix}")
        return {k: v for k, v in self._objects.items() if k.startswith(prefix)}

    def upload_data(self, data: bytes, remote_path: str) -> str:
        self._objects[remote_path] = len(data)
        self._store.data[(self._id, remote_path)] = data
        return remote_path

    def download_data(self, remote_path: str) -> bytes:
        return self._store.data[(self._id, remote_path)]

    def delete_file(self, remote_path: str) -> None:
        self._objects.pop(remote_path, None)
        self._store.data.pop((self._id, remote_path), None)

    def try_copy_object(
        self, source_bucket: str, source_path: str, remote_path: str
    ) -> bool:
        self._store.probes.append((source_bucket, source_path, remote_path))
        return self._store.server_side_copy_available


class FakeStore:
    """
    Objects of all buckets.
    """

    def __init__(self) -> None:
        self.buckets: dict[tuple, dict[str, int]] = {}
        self.probes: list[tuple] = []
        self.server_side_copy_available = True
        self.data: dict[tuple, bytes] = {}
        self.list_errors: set[str] = set()

    def engine(self, storage: dict) -> FakeEngine:
        return FakeEngine(self, storage)

    def bucket(self, bucket: str, endpoint: str = ENDPOINT) -> dict[str, int]:
        return self.buckets.setdefault(("s3", endpoint, bucket), {})


# pylint: disable=unused-argument,too-many-instance-attributes
class FakeLayout:
    """
    Backup layout keeping backup metadata in memory.
    """

    def __init__(self, path_root: str, backups: list[BackupMetadata]) -> None:
        self.path_root = path_root
        self.backups = {b.name: b for b in backups}
        self.uploaded: list[tuple[str, BackupState]] = []
        self.copied: list[CopyEntry] = []
        self.cloud_storage_files: dict[str, list[tuple[str, bytes]]] = {}
        self.copy_error: Exception | None = None
        self.object_errors: dict[str, BaseException] = {}
        self.metadata_errors: dict[str, Exception] = {}
        self.store: FakeStore | None = None
        self.source: FakeLayout | None = None
        self.staged: dict[str, BackupMetadata] = {}
        self.copied_files: dict[str, list[tuple[str, bytes]]] = {}

    def get_backup_names(self):
        return list(self.backups)

    def get_backup_path(self, backup_name: str) -> str:
        return os.path.join(self.path_root, backup_name)

    def get_cloud_storage_path(self, backup_name: str) -> str:
        return self.get_backup_path(backup_name.replace("-", "_"))

    def get_cloud_storage_data_path(self, backup_name: str, disk_name: str) -> str:
        return os.path.join(
            self.get_cloud_storage_path(backup_name), "cloud_storage", disk_name
        )

    def get_backup(self, backup_name, use_light_meta=False):
        backup = self.backups.get(backup_name) or self.staged.get(backup_name)
        return copy.deepcopy(backup) if backup else None

    def get_backups(self, use_light_meta=False):
        return sorted(
            (copy.deepcopy(b) for b in self.backups.values()),
            key=lambda b: b.start_time,
            reverse=True,
        )

    def upload_backup_metadata(self, backup: BackupMetadata) -> None:
        self.uploaded.append((backup.name, backup.state))
        self.backups[backup.name] = copy.deepcopy(backup)

    def copy_objects(self, entries, *, keep_going=False, callback=None, on_error=None):
        for entry in entries:
            error = self.copy_error or self.object_errors.get(entry.src_path)
            if error:
                if on_error:
                    on_error(entry, error)
                if not keep_going:
                    raise error
            else:
                self.copied.append(entry)
                self._copy_content(entry)
                if self.store:
                    self.store.buckets.setdefault(storage_id(entry.dst_storage), {})[
                        entry.dst_path
                    ] = entry.size
                if callback:
                    callback(entry)

    def wait(self, keep_going=False):
        pass

    def _copy_content(self, entry: CopyEntry) -> None:
        """Keep contents of copied disk metadata and backup metadata readable."""
        for layout in (self, self.source):
            if layout and entry.src_path in layout.metadata_files():
                self.copied_files[entry.dst_path] = list(
                    layout.metadata_files()[entry.src_path]
                )
        if self.source and entry.src_path.endswith("/backup_struct.json"):
            name = os.path.relpath(
                os.path.dirname(entry.src_path), self.source.path_root
            )
            staged = os.path.relpath(os.path.dirname(entry.dst_path), self.path_root)
            self.staged[staged] = copy.deepcopy(self.source.backups[name])

    def get_cloud_storage_metadata_remote_paths(
        self, backup_name, disk_name, compression, desired_tables
    ):
        prefix = os.path.join(self.get_backup_path(backup_name), "disks", disk_name)
        return sorted(p for p in self.metadata_files() if p.startswith(prefix))

    def get_cloud_storage_metadata_files(self, backup_meta, remote_path):
        return list(self.metadata_files()[remote_path])

    def metadata_files(self) -> dict[str, list[tuple[str, bytes]]]:
        return {**self.copied_files, **self.cloud_storage_files}

    def upload_cloud_storage_metadata_files(
        self, backup_meta, remote_path, files, callback=None, on_error=None
    ):
        error = self.metadata_errors.get(remote_path)
        if error:
            if on_error:
                on_error(error)
            raise error
        self.cloud_storage_files[remote_path] = files
        if self.store:
            self.store.buckets.setdefault(("s3", ENDPOINT, DST_BUCKET), {})[
                remote_path
            ] = sum(len(data) for _, data in files)
        if callback:
            callback()


def _config(bucket: str, path_root: str) -> dict:
    config: dict[str, Any] = copy.deepcopy(DEFAULT_CONFIG)
    config["storage"]["credentials"] = {
        "endpoint_url": ENDPOINT,
        "access_key_id": "key",
        "secret_access_key": "secret",
        "bucket": bucket,
    }
    config["backup"]["path_root"] = path_root
    config["encryption"]["key"] = "k" * 32
    return config


def _part(
    checksum: str = "checksum1",
    link: str | None = None,
    link_part_name: str | None = None,
    size: int = 100,
) -> dict:
    return {
        "checksum": checksum,
        "bytes": size,
        "files": ["data.bin", "checksums.txt"],
        "tarball": True,
        "link": link,
        "link_part_name": link_part_name,
        "disk_name": "default",
        "encrypted": True,
    }


def _backup(
    name: str,
    *,
    state: BackupState = BackupState.CREATED,
    start_time: str = "2026-01-01 00:00:00 +0000",
    hostname: str = "host1",
    parts: dict | None = None,
    database: str = "db1",
    cloud_storage_disks: list[str] | None = None,
) -> BackupMetadata:
    parts = parts or {}
    size = sum(p["bytes"] for p in parts.values())
    real_size = sum(p["bytes"] for p in parts.values() if not p["link"])
    return BackupMetadata.load(
        {
            "meta": {
                "name": name,
                "path": f"ch_backup/{name}",
                "hostname": hostname,
                "state": state.value,
                "start_time": start_time,
                "end_time": start_time,
                "time_format": TIME_FORMAT,
                "bytes": size,
                "real_bytes": real_size,
                "labels": {},
                "version": "1.0.0",
                "ch_version": "25.3.1.1",
            },
            "databases": {
                database: {
                    "engine": "Atomic",
                    "tables": {
                        "t1": {"engine": "MergeTree", "uuid": None, "parts": parts}
                    },
                }
            },
            "cloud_storage": {
                "encryption": True,
                "compression": True,
                "disks": cloud_storage_disks or [],
            },
        }
    )


class CopyTestContext:
    """
    Source and target storages with copier between them.
    """

    def __init__(
        self,
        source_backups: list[BackupMetadata],
        target_backups: list[BackupMetadata] | None = None,
        target_bucket: str = DST_BUCKET,
    ) -> None:
        self.store = FakeStore()
        self.config = _config(SRC_BUCKET, "ch_backup")
        self.target_config = _config(target_bucket, "ch_backup_copy")
        self.layout = FakeLayout("ch_backup", source_backups)
        self.target_layout = FakeLayout("ch_backup_copy", target_backups or [])
        self.target_layout.source = self.layout
        self.ch_config = MagicMock()
        self.ch_config.config = {"storage_configuration": {"disks": {}}}

    @property
    def src_objects(self) -> dict[str, int]:
        return self.store.bucket(SRC_BUCKET)

    @property
    def dst_objects(self) -> dict[str, int]:
        return self.store.bucket(self.target_config["storage"]["credentials"]["bucket"])

    def copy(
        self,
        backup_names: list[str] | None = None,
        *,
        keep_going: bool = False,
        report: CopyReport | None = None,
        scopes: list[str] | None = None,
    ) -> tuple[list[str], list[str]]:
        for name in self.layout.backups:
            self.src_objects.setdefault(f"ch_backup/{name}/backup_struct.json", 100)
        copier = self.make_copier()
        with patch(
            "ch_backup.backup.copier.get_storage_engine", side_effect=self.store.engine
        ):
            return copier.copy(
                backup_names, keep_going=keep_going, report=report, scopes=scopes
            )

    def make_copier(self) -> BackupCopier:
        return BackupCopier(
            self.config,  # type: ignore[arg-type]
            self.target_config,  # type: ignore[arg-type]
            self.layout,  # type: ignore[arg-type]
            self.target_layout,  # type: ignore[arg-type]
            self.ch_config,
        )

    def copied_paths(self) -> dict[str, str]:
        return {e.src_path: e.dst_path for e in self.target_layout.copied}


def _with_files(ctx: CopyTestContext, backup_name: str, files: dict[str, int]) -> None:
    for path, size in files.items():
        ctx.src_objects[f"ch_backup/{backup_name}/{path}"] = size


def test_copy_backup_with_detached_object_storage_metadata() -> None:
    backup = _backup("b1")
    backup.add_detached_part(
        DetachedPartMetadata(
            database="db1",
            table="t1",
            name="all_1_1_0",
            reason="",
            checksum="checksum",
            size=100,
            files=["checksums.txt"],
            tarball=True,
            disk_name="object_storage",
            object_storage=True,
        )
    )
    ctx = CopyTestContext([backup])
    ctx.target_config["copy"]["server_side_copy"] = False
    _with_files(
        ctx,
        "b1",
        {
            "backup_struct.json": 10,
            "backup_light_struct.json": 10,
            "detached/db1/t1/object_storage/all_1_1_0/all_1_1_0.tar": 100,
        },
    )

    assert ctx.copy() == (["b1"], [])
    assert (
        "ch_backup/b1/detached/db1/t1/object_storage/all_1_1_0/all_1_1_0.tar"
        in ctx.copied_paths()
    )


def test_copy_backup() -> None:
    ctx = CopyTestContext([_backup("b1", parts={"all_1_1_0": _part()})])
    ctx.target_config["copy"]["server_side_copy"] = False
    _with_files(
        ctx,
        "b1",
        {
            "backup_struct.json": 10,
            "backup_light_struct.json": 10,
            "metadata/databases.tar": 20,
            "data/db1/t1/all_1_1_0/all_1_1_0.tar": 100,
        },
    )

    assert ctx.copy(["b1"]) == (["b1"], [])

    assert ctx.copied_paths() == {
        "ch_backup/b1/metadata/databases.tar": "ch_backup_copy/b1/metadata/databases.tar",
        "ch_backup/b1/data/db1/t1/all_1_1_0/all_1_1_0.tar": "ch_backup_copy/b1/data/db1/t1/all_1_1_0/all_1_1_0.tar",
    }
    assert all(not e.server_side for e in ctx.target_layout.copied)
    assert ctx.target_layout.uploaded == [
        ("b1", BackupState.CREATING),
        ("b1", BackupState.CREATED),
    ]
    target_backup = ctx.target_layout.backups["b1"]
    assert target_backup.path == "ch_backup_copy/b1"
    assert target_backup.copied_from == {
        "endpoint_url": ENDPOINT,
        "bucket": SRC_BUCKET,
        "path_root": "ch_backup",
    }
    assert target_backup.start_time == ctx.layout.backups["b1"].start_time
    assert target_backup.hostname == ctx.layout.backups["b1"].hostname


@pytest.mark.parametrize("server_side", [False, True])
def test_copy_skips_prefix_folder_marker(server_side: bool) -> None:
    ctx = CopyTestContext([_backup("b1")])
    ctx.target_config["copy"]["server_side_copy"] = server_side
    _with_files(ctx, "b1", {"": 0, "metadata/a.tar": 10, "metadata/empty": 0})

    assert ctx.copy(["b1"]) == (["b1"], [])

    copied = ctx.copied_paths()
    copied.update((src, dst) for _, src, dst in ctx.store.probes)
    assert copied == {
        "ch_backup/b1/metadata/a.tar": "ch_backup_copy/b1/metadata/a.tar",
        "ch_backup/b1/metadata/empty": "ch_backup_copy/b1/metadata/empty",
    }
    assert ctx.target_layout.backups["b1"].state == BackupState.CREATED


def test_copy_backup_with_server_side_copy() -> None:
    ctx = CopyTestContext([_backup("b1")])
    _with_files(ctx, "b1", {"metadata/a.tar": 1, "metadata/b.tar": 2})

    ctx.copy(["b1"])

    # The first object is copied when probing server-side copy.
    assert ctx.store.probes == [
        (SRC_BUCKET, "ch_backup/b1/metadata/a.tar", "ch_backup_copy/b1/metadata/a.tar")
    ]
    assert [(e.dst_path, e.server_side) for e in ctx.target_layout.copied] == [
        ("ch_backup_copy/b1/metadata/b.tar", True)
    ]


def test_copy_backup_with_unavailable_server_side_copy() -> None:
    ctx = CopyTestContext([_backup("b1")])
    ctx.store.server_side_copy_available = False
    _with_files(ctx, "b1", {"metadata/a.tar": 1, "metadata/b.tar": 2})

    ctx.copy(["b1"])

    assert len(ctx.store.probes) == 1
    assert [(e.dst_path, e.server_side) for e in ctx.target_layout.copied] == [
        ("ch_backup_copy/b1/metadata/a.tar", False),
        ("ch_backup_copy/b1/metadata/b.tar", False),
    ]


def test_no_server_side_copy_with_different_credentials() -> None:
    ctx = CopyTestContext([_backup("b1")])
    ctx.target_config["storage"]["credentials"]["access_key_id"] = "other"
    _with_files(ctx, "b1", {"metadata/a.tar": 1})

    ctx.copy(["b1"])

    assert not ctx.store.probes
    assert [e.server_side for e in ctx.target_layout.copied] == [False]


def test_server_side_copy_cache_respects_credentials() -> None:
    ctx = _cloud_storage_context()
    ctx.target_config["copy"]["server_side_copy"] = True
    ctx.target_config["copy"]["cloud_storage"]["target"]["bucket"] = DST_BUCKET
    ctx.ch_config.config["storage_configuration"]["disks"]["s3"][
        "endpoint"
    ] = f"{ENDPOINT}/{SRC_BUCKET}/data/"
    _with_files(ctx, "b1", {"metadata/databases.tar": 100})

    ctx.copy(["b1"])

    entries = [e for e in ctx.target_layout.copied if e.dst_path.startswith("copies/")]
    assert len(entries) == 2
    assert all(not entry.server_side for entry in entries)


@pytest.mark.parametrize("credential", ["access_key_id", "secret_access_key"])
@pytest.mark.parametrize("reverse", [False, True])
@pytest.mark.parametrize("second_mode", ["ineligible", "unavailable", "available"])
def test_copy_mode_cache_isolates_credentials(
    credential: str, reverse: bool, second_mode: str
) -> None:
    ctx = CopyTestContext([])
    second_source = copy.deepcopy(ctx.config["storage"])
    second_target = copy.deepcopy(ctx.target_config["storage"])
    second_source["credentials"][credential] = "different-credential"
    if second_mode != "ineligible":
        second_target["credentials"][credential] = "different-credential"
    entries = [
        CopyEntry(source, target, f"{group}/{name}", f"copy/{group}/{name}", 1)
        for group, source, target in (
            ("a", ctx.config["storage"], ctx.target_config["storage"]),
            ("b", second_source, second_target),
        )
        for name in ("first", "second")
    ]
    if reverse:
        entries = entries[2:] + entries[:2]
    engines = []

    def make_engine(storage: dict) -> MagicMock:
        engine = MagicMock()
        engine.try_copy_object.return_value = (
            storage["credentials"][credential] != "different-credential"
            or second_mode == "available"
        )
        engines.append(engine)
        return engine

    with (
        patch("ch_backup.backup.copier.get_storage_engine", side_effect=make_engine),
        patch("ch_backup.backup.copier.logging") as logging,
    ):
        # pylint: disable-next=protected-access
        copied = list(ctx.make_copier()._resolve_copy_mode(entries))

    expected = {"copy/a/second": True, "copy/b/second": second_mode == "available"}
    if second_mode != "available":
        expected["copy/b/first"] = False
    assert {e.dst_path: e.server_side for e in copied} == expected
    assert len(engines) == (1 if second_mode == "ineligible" else 2)
    assert all(engine.try_copy_object.call_count == 1 for engine in engines)
    assert "different-credential" not in str(logging.mock_calls)


def test_copy_all_backups_oldest_first() -> None:
    ctx = CopyTestContext(
        [
            _backup("b2", start_time="2026-01-02 00:00:00 +0000"),
            _backup("b1", start_time="2026-01-01 00:00:00 +0000"),
            _backup("b3", state=BackupState.FAILED),
        ]
    )

    assert ctx.copy() == (["b1", "b2"], [])


def test_skip_already_copied_backup() -> None:
    backup = _backup("b1")
    ctx = CopyTestContext([backup], [copy.deepcopy(backup)])
    _with_files(ctx, "b1", {"metadata/a.tar": 1})

    assert ctx.copy(["b1"]) == ([], ["b1"])
    assert not ctx.target_layout.uploaded
    assert not ctx.target_layout.copied


@pytest.mark.parametrize("state", [None, BackupState.CREATING, BackupState.FAILED])
@pytest.mark.parametrize("server_side", [False, True])
def test_retry_recopies_existing_objects(
    state: BackupState | None, server_side: bool
) -> None:
    backup = _backup("b1")
    target_backup = copy.deepcopy(backup)
    if state:
        target_backup.state = state
    ctx = CopyTestContext([backup], [target_backup] if state else [])
    ctx.target_config["copy"]["server_side_copy"] = server_side
    _with_files(ctx, "b1", {"metadata/a.tar": 1, "metadata/b.tar": 2})
    # Same size does not establish that these are the source object's contents.
    ctx.dst_objects["ch_backup_copy/b1/metadata/a.tar"] = 1
    # Partially copied object
    ctx.dst_objects["ch_backup_copy/b1/metadata/b.tar"] = 1

    assert ctx.copy(["b1"]) == (["b1"], [])
    copied = {e.dst_path for e in ctx.target_layout.copied}
    copied.update(dst for _, _, dst in ctx.store.probes)
    assert copied == {
        "ch_backup_copy/b1/metadata/a.tar",
        "ch_backup_copy/b1/metadata/b.tar",
    }


def test_backup_name_conflict() -> None:
    ctx = CopyTestContext([_backup("b1")], [_backup("b1", hostname="host2")])

    with pytest.raises(ClickhouseBackupError, match="is not a copy"):
        ctx.copy(["b1"])
    assert not ctx.target_layout.uploaded


def test_backup_in_invalid_state() -> None:
    ctx = CopyTestContext([_backup("b1", state=BackupState.PARTIALLY_DELETED)])

    with pytest.raises(ClickhouseBackupError, match='in the state "partially_deleted"'):
        ctx.copy(["b1"])


def test_failed_copying() -> None:
    ctx = CopyTestContext([_backup("b1")])
    ctx.target_layout.copy_error = RuntimeError("network error")
    _with_files(ctx, "b1", {"metadata/a.tar": 1})
    ctx.target_config["copy"]["server_side_copy"] = False

    with pytest.raises(RuntimeError):
        ctx.copy(["b1"])

    assert ctx.target_layout.uploaded == [
        ("b1", BackupState.CREATING),
        ("b1", BackupState.FAILED),
    ]
    assert ctx.target_layout.backups["b1"].exception == "RuntimeError: network error"


@pytest.mark.parametrize("fail_metadata_upload", [False, True])
def test_submission_failure_drains_pending_writes(fail_metadata_upload: bool) -> None:
    ctx = _cloud_storage_context()
    if fail_metadata_upload:
        tarball = "ch_backup/b1/disks/s3/db1/t1.tar.gz"
        ctx.layout.cloud_storage_files[tarball] = [
            (
                "part/data.bin",
                _object_storage_metadata(("data/xyz/aaa", 1000), version=5),
            )
        ]
    else:
        ctx.target_layout.copy_error = RuntimeError("submission failed")

    def drained(keep_going: bool = False) -> None:
        # FAILED must only become visible once all writes have settled.
        assert ctx.target_layout.backups["b1"].state == BackupState.CREATING

    with patch.object(ctx.target_layout, "wait", side_effect=drained) as wait:
        with patch.object(
            ctx.target_layout,
            "upload_cloud_storage_metadata_files",
            side_effect=RuntimeError("submission failed"),
        ):
            with pytest.raises(RuntimeError, match="submission failed"):
                ctx.copy(["b1"])
    assert wait.call_args_list[-1] == call(keep_going=True)
    assert wait.call_args_list.count(call(keep_going=True)) == 1
    assert ctx.target_layout.backups["b1"].state == BackupState.FAILED


def test_async_copy_failure_does_not_publish_created() -> None:
    ctx = CopyTestContext([_backup("b1")])
    _with_files(ctx, "b1", {"metadata/a.tar": 1})
    with patch.object(
        ctx.target_layout, "wait", side_effect=RuntimeError("upload failed")
    ):
        with pytest.raises(RuntimeError, match="upload failed"):
            ctx.copy(["b1"])
    assert ctx.target_layout.uploaded == [
        ("b1", BackupState.CREATING),
        ("b1", BackupState.FAILED),
    ]


@pytest.mark.parametrize(
    ["section", "key", "value"],
    [
        ("encryption", "key", "x" * 32),
        ("encryption", "chunk_size", 1024),
        ("encryption", "type", "noop"),
        ("compression", "type", "lz4"),
    ],
)
def test_incompatible_target_config(section: str, key: str, value: Any) -> None:
    ctx = CopyTestContext([_backup("b1")])
    ctx.target_config[section][key] = value

    with pytest.raises(ConfigurationError, match=f"{section}.{key}"):
        ctx.copy(["b1"])


def test_same_storage() -> None:
    ctx = CopyTestContext([_backup("b1")], target_bucket=SRC_BUCKET)
    ctx.target_config["backup"]["path_root"] = "ch_backup/"

    with pytest.raises(ConfigurationError, match="are the same"):
        ctx.copy(["b1"])


def test_same_bucket_different_path_root() -> None:
    ctx = CopyTestContext([_backup("b1")], target_bucket=SRC_BUCKET)
    ctx.target_config["copy"]["server_side_copy"] = False
    _with_files(ctx, "b1", {"metadata/a.tar": 1})

    assert ctx.copy(["b1"]) == (["b1"], [])
    assert ctx.copied_paths() == {
        "ch_backup/b1/metadata/a.tar": "ch_backup_copy/b1/metadata/a.tar"
    }


@pytest.mark.parametrize(
    ["database", "stored_database", "target_database"],
    [
        pytest.param("db1", "db1", "db1", id="escaped path"),
        pytest.param("my-db", "my-db", "my%2Ddb", id="legacy unescaped path"),
        pytest.param(
            "my-db", "my%2Ddb", "my%2Ddb", id="escaped path with special chars"
        ),
    ],
)
def test_linked_parts_are_copied_into_backup(
    database: str, stored_database: str, target_database: str
) -> None:
    b1 = _backup("b1", database=database, parts={"all_1_1_0": _part()})
    b2 = _backup(
        "b2",
        database=database,
        start_time="2026-01-02 00:00:00 +0000",
        parts={
            "all_1_1_0": _part(link="ch_backup/b1"),
            "all_2_2_0_3": _part(link="b1", link_part_name="all_2_2_0"),
            "all_3_3_0": _part(checksum="checksum3"),
        },
    )
    ctx = CopyTestContext([b1, b2])
    ctx.target_config["copy"]["server_side_copy"] = False
    data_dir = f"data/{stored_database}/t1"
    target_data_dir = f"data/{target_database}/t1"
    _with_files(
        ctx,
        "b1",
        {
            f"{data_dir}/all_1_1_0/all_1_1_0.tar": 101,
            f"{data_dir}/all_2_2_0/all_2_2_0.tar": 102,
        },
    )
    _with_files(ctx, "b2", {f"{data_dir}/all_3_3_0/all_3_3_0.tar": 103})

    assert ctx.copy(["b2"]) == (["b2"], [])

    assert {(e.src_path, e.dst_path, e.size) for e in ctx.target_layout.copied} == {
        (
            f"ch_backup/b2/{data_dir}/all_3_3_0/all_3_3_0.tar",
            f"ch_backup_copy/b2/{data_dir}/all_3_3_0/all_3_3_0.tar",
            103,
        ),
        (
            f"ch_backup/b1/{data_dir}/all_1_1_0/all_1_1_0.tar",
            f"ch_backup_copy/b2/{target_data_dir}/all_1_1_0/all_1_1_0.tar",
            101,
        ),
        (
            f"ch_backup/b1/{data_dir}/all_2_2_0/all_2_2_0.tar",
            f"ch_backup_copy/b2/{target_data_dir}/all_2_2_0_3/all_2_2_0_3.tar",
            102,
        ),
    }
    target_backup = ctx.target_layout.backups["b2"]
    assert all(not part.link for part in target_backup.get_parts())
    assert all(not part.link_part_name for part in target_backup.get_parts())
    assert target_backup.real_size == target_backup.size == 300


def test_links_to_copied_backups_are_kept() -> None:
    b1 = _backup("b1", parts={"all_1_1_0": _part(), "all_2_2_0": _part()})
    b2 = _backup(
        "b2",
        start_time="2026-01-02 00:00:00 +0000",
        parts={
            "all_1_1_0": _part(link="b1"),
            "all_2_2_0_3": _part(link="b1", link_part_name="all_2_2_0"),
        },
    )
    ctx = CopyTestContext([b1, b2])
    ctx.target_config["copy"]["server_side_copy"] = False
    _with_files(
        ctx,
        "b1",
        {
            "data/db1/t1/all_1_1_0/all_1_1_0.tar": 101,
            "data/db1/t1/all_2_2_0/all_2_2_0.tar": 102,
        },
    )

    assert ctx.copy() == (["b1", "b2"], [])

    assert {e.dst_path for e in ctx.target_layout.copied} == {
        "ch_backup_copy/b1/data/db1/t1/all_1_1_0/all_1_1_0.tar",
        "ch_backup_copy/b1/data/db1/t1/all_2_2_0/all_2_2_0.tar",
    }
    target_backup = ctx.target_layout.backups["b2"]
    assert [(p.link, p.link_part_name) for p in target_backup.get_parts()] == [
        ("b1", None),
        ("b1", "all_2_2_0"),
    ]
    assert target_backup.real_size == 0


def test_links_are_not_kept_if_part_differs_in_target() -> None:
    b1 = _backup("b1", parts={"all_1_1_0": _part()})
    b2 = _backup(
        "b2",
        start_time="2026-01-02 00:00:00 +0000",
        parts={"all_1_1_0": _part(link="b1")},
    )
    target_b1 = _backup("b1", parts={"all_1_1_0": _part(checksum="other")})
    ctx = CopyTestContext([b1, b2], [target_b1])
    ctx.target_config["copy"]["server_side_copy"] = False
    _with_files(ctx, "b1", {"data/db1/t1/all_1_1_0/all_1_1_0.tar": 101})

    ctx.copy(["b2"])

    assert [e.dst_path for e in ctx.target_layout.copied] == [
        "ch_backup_copy/b2/data/db1/t1/all_1_1_0/all_1_1_0.tar"
    ]
    assert not ctx.target_layout.backups["b2"].get_parts()[0].link


def test_missing_linked_part() -> None:
    b2 = _backup("b2", parts={"all_1_1_0": _part(link="b1")})
    ctx = CopyTestContext([b2])

    with pytest.raises(ClickhouseBackupError, match="is not found in source storage"):
        ctx.copy(["b2"])
    assert not ctx.target_layout.uploaded


@pytest.mark.parametrize("stored_database", ["my-db", "my%2Ddb"])
@pytest.mark.parametrize("keep_going", [False, True])
def test_copy_partial_linked_part_files_and_retry(
    stored_database: str, keep_going: bool
) -> None:
    part = {
        **_part(link="owner", link_part_name="all_1_1_0"),
        "tarball": False,
        "files": ["checksums.txt", "data.bin", "columns.txt"],
    }
    ctx = CopyTestContext(
        [_backup("child", database="my-db", parts={"all_1_1_0_2": part})]
    )
    ctx.target_config["copy"]["server_side_copy"] = False
    part_dir = f"data/{stored_database}/t1/all_1_1_0"
    _with_files(ctx, "owner", {f"{part_dir}/data.bin": 100})
    report = CopyReport()

    with pytest.raises(ClickhouseBackupError):
        ctx.copy(keep_going=keep_going, report=report)
    if keep_going:
        result = report.backups[0]
        assert (result.objects_total, result.objects_copied) == (1, 1)
        assert result.known_bytes_copied == 100
        assert [e["source"] for e in result.errors] == [
            f"owner/{part_dir}/checksums.txt",
            f"owner/{part_dir}/columns.txt",
        ]
        assert ctx.copied_paths() == {
            f"ch_backup/owner/{part_dir}/data.bin": (
                "ch_backup_copy/child/data/my%2Ddb/t1/all_1_1_0_2/data.bin"
            )
        }
        failed = ctx.target_layout.backups["child"]
        assert failed.state == BackupState.FAILED
        assert failed.get_parts()[0].link == "owner"
    else:
        assert not ctx.target_layout.uploaded
        assert not ctx.target_layout.copied
        assert len(report.backups[0].errors) == 1

    # Repair the other source layout: the retry must reuse the same target path
    # so a partial escaped directory cannot shadow a complete legacy directory.
    retry_database = "my-db" if stored_database == "my%2Ddb" else "my%2Ddb"
    retry_part_dir = f"data/{retry_database}/t1/all_1_1_0"
    _with_files(
        ctx,
        "owner",
        {
            f"{retry_part_dir}/data.bin": 100,
            f"{retry_part_dir}/checksums.txt": 1,
            f"{retry_part_dir}/columns.txt": 2,
        },
    )
    assert ctx.copy(keep_going=keep_going, report=report) == (["child"], [])
    target = ctx.target_layout.backups["child"]
    assert target.state == BackupState.CREATED
    assert target.get_parts()[0].link is None
    assert target.get_parts()[0].link_part_name is None
    assert report.backups[0].objects_copied == 3
    assert report.backups[0].known_bytes_copied == 103
    assert not report.backups[0].errors
    assert all("/my%2Ddb/" in path for path in ctx.copied_paths().values())


def test_copy_linked_part_prefers_complete_legacy_directory() -> None:
    part = {**_part(link="owner"), "tarball": False}
    ctx = CopyTestContext(
        [_backup("child", database="my-db", parts={"all_1_1_0": part})]
    )
    ctx.target_config["copy"]["server_side_copy"] = False
    _with_files(
        ctx,
        "owner",
        {
            "data/my%2Ddb/t1/all_1_1_0/data.bin": 99,
            "data/my-db/t1/all_1_1_0/data.bin": 100,
            "data/my-db/t1/all_1_1_0/checksums.txt": 1,
        },
    )
    assert ctx.copy(keep_going=True) == (["child"], [])
    assert len(ctx.target_layout.copied) == 2
    assert all("/my-db/" in path for path in ctx.copied_paths())
    assert all("/my%2Ddb/" in path for path in ctx.copied_paths().values())


def _object_storage_metadata(*objects: tuple[str, int], version: int = 3) -> bytes:
    lines = [str(version), f"{len(objects)}\t{sum(size for _, size in objects)}"]
    lines.extend(f"{size}\t{key}" for key, size in objects)
    lines.extend(["1", "0", ""])
    if version >= 4:
        lines.append("")
    return "\n".join(lines).encode()


def _cloud_storage_context(backup_name: str = "b1") -> CopyTestContext:
    ctx = CopyTestContext([_backup(backup_name, cloud_storage_disks=["s3"])])
    ctx.target_config["copy"]["server_side_copy"] = False
    ctx.target_config["copy"]["cloud_storage"]["target"]["bucket"] = "disk-copy"
    ctx.target_config["copy"]["cloud_storage"]["target"]["path"] = "copies/"
    ctx.ch_config.config = {
        "storage_configuration": {
            "disks": {
                "s3": {
                    "type": "s3",
                    "endpoint": "http://minio:9000/cloud-storage/data/",
                    "access_key_id": "disk_key",
                    "secret_access_key": "disk_secret",
                }
            }
        }
    }
    sanitized_name = backup_name.replace("-", "_")
    tarball = f"ch_backup/{sanitized_name}/disks/s3/db1/t1.tar.gz"
    _with_files(ctx, sanitized_name, {"disks/s3/db1/t1.tar.gz": 50})
    ctx.layout.cloud_storage_files[tarball] = [
        (
            "store/abc/uuid/all_1_1_0/data.bin",
            _object_storage_metadata(("xyz/aaa", 1000)),
        ),
        (
            "store/abc/uuid/all_1_1_0/checksums.txt",
            _object_storage_metadata(("xyz/bbb", 10)),
        ),
        # Hardlinked file of a mutated part references the same object.
        (
            "store/abc/uuid/all_1_1_0_2/data.bin",
            _object_storage_metadata(("xyz/aaa", 1000)),
        ),
        ("store/abc/uuid/all_1_1_0/frozen_metadata.txt", b"garbage"),
    ]
    return ctx


@pytest.mark.parametrize("backup_name", ["b1", "backup-1"])
def test_cloud_storage(backup_name: str) -> None:
    ctx = _cloud_storage_context(backup_name)
    sanitized_name = backup_name.replace("-", "_")

    assert ctx.copy([backup_name]) == ([backup_name], [])

    disk_entries = {
        (
            e.src_storage["credentials"]["bucket"],
            e.src_storage["credentials"]["endpoint_url"],
            e.src_storage["credentials"]["access_key_id"],
            e.src_path,
            e.dst_storage["credentials"]["bucket"],
            e.dst_storage["credentials"]["endpoint_url"],
            e.dst_path,
            e.size,
        )
        for e in ctx.target_layout.copied
        if e.dst_storage["credentials"]["bucket"] == "disk-copy"
    }
    assert disk_entries == {
        (
            "cloud-storage",
            "http://minio:9000",
            "disk_key",
            "data/xyz/aaa",
            "disk-copy",
            ENDPOINT,
            f"copies/{backup_name}/xyz/aaa",
            1000,
        ),
        (
            "cloud-storage",
            "http://minio:9000",
            "disk_key",
            "data/xyz/bbb",
            "disk-copy",
            ENDPOINT,
            f"copies/{backup_name}/xyz/bbb",
            10,
        ),
    }
    # Tarballs with cloud storage metadata are copied too.
    assert (
        f"ch_backup_copy/{sanitized_name}/disks/s3/db1/t1.tar.gz"
        in ctx.copied_paths().values()
    )
    assert ctx.target_layout.backups[backup_name].cloud_storage.source == {
        "endpoint_url": ENDPOINT,
        "bucket": "disk-copy",
        "path": f"copies/{backup_name}",
    }


def test_copy_skips_sanitized_disk_prefix_folder_marker() -> None:
    ctx = _cloud_storage_context("backup-1")
    _with_files(ctx, "backup_1", {"disks/": 0})

    assert ctx.copy(["backup-1"]) == (["backup-1"], [])

    copied = ctx.copied_paths()
    assert "ch_backup/backup_1/disks/" not in copied
    assert "ch_backup_copy/backup_1/disks/s3/db1/t1.tar.gz" in copied.values()


def test_cloud_storage_source_override() -> None:
    ctx = _cloud_storage_context()
    source_conf = ctx.target_config["copy"]["cloud_storage"]["source"]
    source_conf["bucket"] = "other-bucket"
    source_conf["path"] = "other/path"

    ctx.copy(["b1"])

    assert {
        (
            e.src_storage["credentials"]["endpoint_url"],
            e.src_storage["credentials"]["bucket"],
            e.src_path,
        )
        for e in ctx.target_layout.copied
        if e.dst_storage["credentials"]["bucket"] == "disk-copy"
    } == {
        ("http://minio:9000", "other-bucket", "other/path/xyz/aaa"),
        ("http://minio:9000", "other-bucket", "other/path/xyz/bbb"),
    }


@pytest.mark.parametrize("version", [3, 5])
@pytest.mark.parametrize("virtual_host", [False, True])
@pytest.mark.parametrize("stored_source", [False, True])
@pytest.mark.parametrize(
    "overrides",
    [
        {},
        {"path": "other/path"},
        {"bucket": "other-bucket"},
        {"bucket": "other-bucket", "path": "other/path"},
        {"path": ""},
        {"bucket": "other-bucket", "path": ""},
        {"endpoint_url": "http://override:9000"},
    ],
)
def test_cloud_source_overrides_are_independent(
    version: int, virtual_host: bool, stored_source: bool, overrides: dict
) -> None:
    ctx = _cloud_storage_context()
    base = {
        "endpoint_url": "http://minio:9000",
        "bucket": "cloud-storage",
        "path": "data",
    }
    if virtual_host:
        ctx.ch_config.config["storage_configuration"]["disks"]["s3"][
            "endpoint"
        ] = "https://cloud-storage.s3.mds.yandex.net/data/"
        base["endpoint_url"] = "https://s3.mds.yandex.net"
    if stored_source:
        base = {
            "endpoint_url": base["endpoint_url"],
            "bucket": "previous",
            "path": "previous/b1",
        }
        ctx.layout.backups["b1"].cloud_storage.source = base
    if version == 5:
        ctx.layout.cloud_storage_files["ch_backup/b1/disks/s3/db1/t1.tar.gz"] = [
            (
                "part/data.bin",
                _object_storage_metadata(
                    (base["path"] + "/xyz/aaa", 1000),
                    (base["path"] + "/xyz/bbb", 10),
                    version=5,
                ),
            )
        ]
    ctx.target_config["copy"]["cloud_storage"]["source"].update(overrides)
    expected = {**base, **overrides}

    ctx.copy(["b1"])

    entries = [
        e
        for e in ctx.target_layout.copied
        if e.dst_storage["credentials"]["bucket"] == "disk-copy"
    ]
    assert {e.src_path for e in entries} == {
        os.path.join(expected["path"], key) for key in ("xyz/aaa", "xyz/bbb")
    }
    assert {e.dst_path for e in entries} == {"copies/b1/xyz/aaa", "copies/b1/xyz/bbb"}
    for entry in entries:
        credentials = entry.src_storage["credentials"]
        assert credentials["endpoint_url"] == expected["endpoint_url"]
        assert credentials["bucket"] == expected["bucket"]
    if version == 5:
        assert ctx.target_layout.cloud_storage_files[
            "ch_backup_copy/b1/disks/s3/db1/t1.tar.gz"
        ] == [
            (
                "part/data.bin",
                _object_storage_metadata(
                    ("copies/b1/xyz/aaa", 1000), ("copies/b1/xyz/bbb", 10), version=5
                ),
            )
        ]


@pytest.mark.parametrize("stored_source", [False, True])
@pytest.mark.parametrize("override", ["other/path", ""])
def test_full_key_source_path_override_from_bucket_root(
    stored_source: bool, override: str
) -> None:
    ctx = _cloud_storage_context()
    if stored_source:
        ctx.layout.backups["b1"].cloud_storage.source = {
            "endpoint_url": ENDPOINT,
            "bucket": "source",
            "path": "",
        }
    else:
        ctx.ch_config.config["storage_configuration"]["disks"]["s3"][
            "endpoint"
        ] = f"{ENDPOINT}/source/"
    ctx.target_config["copy"]["cloud_storage"]["source"]["path"] = override
    ctx.layout.cloud_storage_files["ch_backup/b1/disks/s3/db1/t1.tar.gz"] = [
        ("part/data.bin", _object_storage_metadata(("xyz/aaa", 1000), version=5))
    ]

    ctx.copy(["b1"])

    assert [
        (e.src_path, e.dst_path)
        for e in ctx.target_layout.copied
        if e.dst_storage["credentials"]["bucket"] == "disk-copy"
    ] == [(os.path.join(override, "xyz/aaa"), "copies/b1/xyz/aaa")]


@pytest.mark.parametrize("override", ["other/path", ""])
@pytest.mark.parametrize(
    "key,known_prefix,error",
    [
        ("data/xyz/aaa", False, "original source prefix is unknown"),
        ("other/xyz/aaa", True, "outside original source prefix"),
        ("database/xyz/aaa", True, "outside original source prefix"),
        ("data", True, "outside original source prefix"),
    ],
)
def test_unmappable_full_key_override_fails_before_writes(
    override: str, key: str, known_prefix: bool, error: str
) -> None:
    ctx = _cloud_storage_context()
    ctx.target_config["copy"]["server_side_copy"] = True
    ctx.target_config["copy"]["cloud_storage"]["source"].update(
        bucket="source", path=override
    )
    if not known_prefix:
        ctx.ch_config.config = {}
    ctx.layout.cloud_storage_files["ch_backup/b1/disks/s3/db1/t1.tar.gz"] = [
        ("part/data.bin", _object_storage_metadata((key, 1000), version=5))
    ]

    with pytest.raises(ConfigurationError, match=error) as raised:
        ctx.copy(["b1"])

    assert key in str(raised.value)
    assert not ctx.store.probes
    assert not ctx.target_layout.uploaded
    assert not ctx.target_layout.copied
    assert not ctx.target_layout.cloud_storage_files


@pytest.mark.parametrize("version", [3, 5])
def test_explicit_cloud_source_without_disk_configuration(version: int) -> None:
    ctx = _cloud_storage_context()
    ctx.ch_config.config = {}
    ctx.target_config["copy"]["cloud_storage"]["source"]["bucket"] = "source"
    ctx.layout.cloud_storage_files["ch_backup/b1/disks/s3/db1/t1.tar.gz"] = [
        (
            "part/data.bin",
            _object_storage_metadata(
                ("xyz/aaa", 1000), ("xyz/bbb", 10), version=version
            ),
        )
    ]
    ctx.copy(["b1"])
    entries = [
        e
        for e in ctx.target_layout.copied
        if e.dst_storage["credentials"]["bucket"] == "disk-copy"
    ]
    assert {e.src_path for e in entries} == {"xyz/aaa", "xyz/bbb"}
    assert all(
        e.src_storage["credentials"]["endpoint_url"] == ENDPOINT for e in entries
    )


def test_copy_rejects_unconfigured_stored_endpoint_before_requests() -> None:
    ctx = _cloud_storage_context()
    ctx.target_config["copy"]["server_side_copy"] = True
    # Bucket/path overrides must not authorize the endpoint from metadata.
    ctx.target_config["copy"]["cloud_storage"]["source"].update(
        bucket="source", path="data"
    )
    ctx.layout.backups["b1"].cloud_storage.source = {
        "endpoint_url": "http://127.0.0.1:9000",
        "bucket": "source",
        "path": "data",
    }

    with pytest.raises(
        ConfigurationError, match=r"copy\.cloud_storage\.source\.endpoint_url"
    ):
        ctx.copy(["b1"])

    assert not ctx.store.probes
    assert not ctx.target_layout.uploaded
    assert not ctx.target_layout.copied


@pytest.mark.parametrize("endpoint_override", [None, "http://other-s3:9000"])
def test_cloud_storage_of_copied_backup(endpoint_override: str | None) -> None:
    ctx = _cloud_storage_context()
    ctx.ch_config.config = {}
    ctx.layout.backups["b1"].cloud_storage.source = {
        "endpoint_url": (endpoint_override or ENDPOINT) + "/",
        "bucket": "copied-disk-data",
        "path": "prev/b1",
    }
    ctx.target_config["copy"]["cloud_storage"]["source"][
        "endpoint_url"
    ] = endpoint_override

    ctx.copy(["b1"])

    assert {
        (
            e.src_storage["credentials"]["endpoint_url"],
            e.src_storage["credentials"]["bucket"],
            e.src_path,
        )
        for e in ctx.target_layout.copied
        if e.dst_storage["credentials"]["bucket"] == "disk-copy"
    } == {
        (endpoint_override or ENDPOINT, "copied-disk-data", "prev/b1/xyz/aaa"),
        (endpoint_override or ENDPOINT, "copied-disk-data", "prev/b1/xyz/bbb"),
    }


def test_cloud_storage_recopies_existing_objects() -> None:
    ctx = _cloud_storage_context()
    ctx.store.bucket("disk-copy")["copies/b1/xyz/aaa"] = 1000

    ctx.copy(["b1"])

    assert "copies/b1/xyz/aaa" in {e.dst_path for e in ctx.target_layout.copied}
    assert "copies/b1/xyz/bbb" in {e.dst_path for e in ctx.target_layout.copied}


@pytest.mark.parametrize("version", [3, 5])
@pytest.mark.parametrize(
    "key",
    ["/unrelated", "../unrelated", "data/../../unrelated", "./data", "data/./object"],
)
def test_unsafe_cloud_keys_fail_before_any_writes(version: int, key: str) -> None:
    ctx = _cloud_storage_context()
    ctx.target_config["copy"]["server_side_copy"] = True
    ctx.layout.cloud_storage_files["ch_backup/b1/disks/s3/db1/t1.tar.gz"] = [
        ("part/data.bin", _object_storage_metadata((key, 1000), version=version)),
    ]
    with pytest.raises(ClickhouseBackupError, match="Unsafe copy object key"):
        ctx.copy(["b1"])
    assert not ctx.store.probes
    assert not ctx.target_layout.uploaded
    assert not ctx.target_layout.copied
    assert not ctx.target_layout.cloud_storage_files


@pytest.mark.parametrize(
    "key", ["", "/other", "../other", "metadata/../../other", "metadata/./file"]
)
def test_unsafe_backup_file_keys_fail_before_any_writes(key: str) -> None:
    ctx = CopyTestContext([_backup("b1")])
    _with_files(ctx, "b1", {key: 10})
    with pytest.raises(ClickhouseBackupError, match="Unsafe copy object key"):
        ctx.copy(["b1"])
    assert not ctx.store.probes
    assert not ctx.target_layout.uploaded


def test_rewritten_metadata_must_stay_in_target_backup() -> None:
    ctx = _cloud_storage_context()
    ctx.layout.cloud_storage_files = {
        "ch_backup/b1/disks/s3/../../../other-backup/t1.tar.gz": [
            (
                "part/data.bin",
                _object_storage_metadata(("data/xyz/aaa", 1000), version=5),
            ),
        ]
    }
    with pytest.raises(ClickhouseBackupError, match="Unsafe copy object key"):
        ctx.copy(["b1"])
    assert not ctx.target_layout.uploaded
    assert not ctx.target_layout.cloud_storage_files


def test_cloud_storage_without_target_bucket() -> None:
    ctx = _cloud_storage_context()
    ctx.target_config["copy"]["cloud_storage"]["target"]["bucket"] = None

    with pytest.raises(ConfigurationError, match="--target-cloud-storage-bucket"):
        ctx.copy()
    assert not ctx.target_layout.uploaded


def test_cloud_storage_without_disk_config() -> None:
    ctx = _cloud_storage_context()
    ctx.ch_config.config = {}

    with pytest.raises(ConfigurationError, match="--cloud-storage-source-bucket"):
        ctx.copy(["b1"])
    assert not ctx.target_layout.uploaded


@pytest.mark.parametrize("version", [3, 5])
@pytest.mark.parametrize(
    "endpoint,service_endpoint,bucket,prefix",
    [
        pytest.param(
            "https://cloud-storage-cluster-id.s3.mds.yandex.net/cloud_storage/cluster-id/shard1/",
            "https://s3.mds.yandex.net",
            "cloud-storage-cluster-id",
            "cloud_storage/cluster-id/shard1",
            id="reviewer-example",
        ),
        pytest.param(
            "https://cloud-storage.s3.us-east-1.amazonaws.com/data/",
            "https://s3.us-east-1.amazonaws.com",
            "cloud-storage",
            "data",
            id="aws-regional",
        ),
        pytest.param(
            "https://cloud-storage.s3-us-west-2.amazonaws.com/data/",
            "https://s3-us-west-2.amazonaws.com",
            "cloud-storage",
            "data",
            id="aws-legacy-regional",
        ),
        pytest.param(
            "https://cloud.storage.s3.mds.yandex.net:9443/data/",
            "https://s3.mds.yandex.net:9443",
            "cloud.storage",
            "data",
            id="dotted-bucket-and-port",
        ),
        pytest.param(
            "https://cloud-storage.s3.amazonaws.com",
            "https://s3.amazonaws.com",
            "cloud-storage",
            "",
            id="virtual-host-no-path",
        ),
        pytest.param(
            "https://cloud-storage.s3.amazonaws.com/",
            "https://s3.amazonaws.com",
            "cloud-storage",
            "",
            id="virtual-host-root",
        ),
        pytest.param(
            "https://s3.us-east-1.amazonaws.com/cloud-storage/data/",
            "https://s3.us-east-1.amazonaws.com",
            "cloud-storage",
            "data",
            id="aws-path-style",
        ),
        pytest.param(
            "http://minio:9000/cloud-storage/data/",
            "http://minio:9000",
            "cloud-storage",
            "data",
            id="path-style-with-port",
        ),
        pytest.param(
            "https://objects.example.net/cloud-storage/",
            "https://objects.example.net",
            "cloud-storage",
            "",
            id="custom-path-style-root",
        ),
        pytest.param(
            "http://192.168.64.6:9000/cloud-storage/data/",
            "http://192.168.64.6:9000",
            "cloud-storage",
            "data",
            id="ip-path-style",
        ),
        pytest.param(
            "https://bucket.vpce-123.s3.us-east-1.vpce.amazonaws.com/cloud-storage/data/",
            "https://bucket.vpce-123.s3.us-east-1.vpce.amazonaws.com",
            "cloud-storage",
            "data",
            id="aws-private-link",
        ),
        pytest.param(
            "https://bucket.vpce-123.s3.us-east-1.vpce.amazonaws.com:443/cloud-storage/data/",
            "https://bucket.vpce-123.s3.us-east-1.vpce.amazonaws.com:443",
            "cloud-storage",
            "data",
            id="aws-private-link-with-port",
        ),
    ],
)
def test_cloud_storage_disk_endpoints(
    version: int, endpoint: str, service_endpoint: str, bucket: str, prefix: str
) -> None:
    ctx = _cloud_storage_context()
    ctx.ch_config.config["storage_configuration"]["disks"]["s3"]["endpoint"] = endpoint
    source_key = os.path.join(prefix, "xyz/aaa")
    metadata_key = source_key if version == 5 else "xyz/aaa"
    tarball = "ch_backup/b1/disks/s3/db1/t1.tar.gz"
    file_name = "store/abc/uuid/all_1_1_0/data.bin"
    ctx.layout.cloud_storage_files[tarball] = [
        (file_name, _object_storage_metadata((metadata_key, 1000), version=version))
    ]

    assert ctx.copy(["b1"]) == (["b1"], [])

    disk_entries = [
        e
        for e in ctx.target_layout.copied
        if e.dst_storage["credentials"]["bucket"] == "disk-copy"
    ]
    assert {
        (
            e.src_storage["credentials"]["endpoint_url"],
            e.src_storage["credentials"]["bucket"],
            e.src_path,
            e.dst_path,
        )
        for e in disk_entries
    } == {(service_endpoint, bucket, source_key, "copies/b1/xyz/aaa")}
    assert disk_entries[0].src_storage["credentials"]["access_key_id"] == "disk_key"
    assert (
        disk_entries[0].src_storage["boto_config"]
        == ctx.config["storage"]["boto_config"]
    )
    target_tarball = "ch_backup_copy/b1/disks/s3/db1/t1.tar.gz"
    if version == 5:
        assert target_tarball not in {e.dst_path for e in ctx.target_layout.copied}
        assert ctx.target_layout.cloud_storage_files[target_tarball] == [
            (
                file_name,
                _object_storage_metadata(("copies/b1/xyz/aaa", 1000), version=5),
            )
        ]
    else:
        assert target_tarball in ctx.copied_paths().values()
        assert not ctx.target_layout.cloud_storage_files
    assert ctx.target_layout.backups["b1"].state == BackupState.CREATED


def test_cloud_storage_full_object_keys() -> None:
    ctx = _cloud_storage_context()
    tarball = "ch_backup/b1/disks/s3/db1/t1.tar.gz"
    ctx.layout.cloud_storage_files[tarball] = [
        (
            "store/abc/uuid/all_1_1_0/data.bin",
            _object_storage_metadata(("data/xyz/aaa", 1000), version=5),
        ),
        (
            "store/abc/uuid/all_1_1_0/count.txt",
            _object_storage_metadata(("other/xyz/ccc", 1), version=5),
        ),
        (
            "store/abc/uuid/all_1_1_0/checksums.txt",
            _object_storage_metadata(("xyz/bbb", 10), version=4),
        ),
    ]

    ctx.copy(["b1"])

    assert {
        (e.src_path, e.dst_path)
        for e in ctx.target_layout.copied
        if e.dst_storage["credentials"]["bucket"] == "disk-copy"
    } == {
        ("data/xyz/aaa", "copies/b1/xyz/aaa"),
        ("other/xyz/ccc", "copies/b1/other/xyz/ccc"),
        ("data/xyz/bbb", "copies/b1/xyz/bbb"),
    }
    # Tarball is uploaded with rewritten full object keys instead of copying as is.
    target_tarball = "ch_backup_copy/b1/disks/s3/db1/t1.tar.gz"
    assert target_tarball not in {e.dst_path for e in ctx.target_layout.copied}
    assert ctx.target_layout.cloud_storage_files[target_tarball] == [
        (
            "store/abc/uuid/all_1_1_0/data.bin",
            _object_storage_metadata(("copies/b1/xyz/aaa", 1000), version=5),
        ),
        (
            "store/abc/uuid/all_1_1_0/count.txt",
            _object_storage_metadata(("copies/b1/other/xyz/ccc", 1), version=5),
        ),
        (
            "store/abc/uuid/all_1_1_0/checksums.txt",
            _object_storage_metadata(("xyz/bbb", 10), version=4),
        ),
    ]


def test_cloud_storage_unsupported_metadata() -> None:
    ctx = _cloud_storage_context()
    tarball = "ch_backup/b1/disks/s3/db1/t1.tar.gz"
    ctx.layout.cloud_storage_files[tarball] = [
        ("store/abc/uuid/all_1_1_0/data.bin", b"1\n1\t10\n10\tdata/xyz\n0\n")
    ]

    with pytest.raises(ClickhouseBackupError, match="version 1 is not supported"):
        ctx.copy(["b1"])
    assert not ctx.target_layout.uploaded


@pytest.mark.parametrize("version", [3, 5])
@pytest.mark.parametrize("linked", [False, True])
@pytest.mark.parametrize("source_path_override", [None, "other/path"])
def test_full_cloud_storage_copy(
    version: int, linked: bool, source_path_override: str | None
) -> None:
    part = {
        **_part(
            link="base" if linked else None,
            link_part_name="all_1_1_0" if linked else None,
        ),
        "disk_name": "s3",
    }
    backup = _backup(
        "backup-1", parts={"all_1_1_0_2": part}, cloud_storage_disks=["s3"]
    )
    backup.cloud_storage.data_copied = True
    owner = _backup(
        "base",
        parts={"all_1_1_0": {**part, "link": None, "link_part_name": None}},
        cloud_storage_disks=["s3"],
    )
    owner.cloud_storage.data_copied = True
    # A retained, partially deleted backup is still a valid source for linked data.
    owner.state = BackupState.PARTIALLY_DELETED
    ctx = CopyTestContext([backup, owner])
    ctx.target_config["copy"]["cloud_storage"]["source"]["path"] = source_path_override
    ctx.target_config["copy"]["server_side_copy"] = False
    source_name = "base" if linked else "backup_1"
    prefix = f"ch_backup/{source_name}/cloud_storage/s3"
    key = f"{prefix}/xyz/data" if version == 5 else "xyz/data"
    tarball = f"ch_backup/{source_name}/disks/s3/db1/t1.tar.gz"
    source_part_name = "all_1_1_0" if linked else "all_1_1_0_2"
    file_name = f"store/abc/uuid/{source_part_name}/data.bin"
    ctx.layout.cloud_storage_files[tarball] = [
        (file_name, _object_storage_metadata((key, 100), version=version))
    ]
    _with_files(
        ctx,
        source_name,
        {"disks/s3/db1/t1.tar.gz": 50, "cloud_storage/s3/xyz/data": 100},
    )

    assert ctx.copy([backup.name]) == ([backup.name], [])

    dst_prefix = "ch_backup_copy/backup_1/cloud_storage/s3"
    assert ctx.copied_paths() == {f"{prefix}/xyz/data": f"{dst_prefix}/xyz/data"}
    target = ctx.target_layout.backups[backup.name]
    assert target.cloud_storage.data_copied
    assert target.cloud_storage.source is None
    assert target.get_parts()[0].link is None
    assert target.get_parts()[0].link_part_name is None
    assert ctx.target_layout.cloud_storage_files[
        "ch_backup_copy/backup_1/disks/s3/db1/t1.tar.gz"
    ] == [
        (
            "store/abc/uuid/all_1_1_0_2/data.bin",
            _object_storage_metadata(
                (f"{dst_prefix}/xyz/data" if version == 5 else "xyz/data", 100),
                version=version,
            ),
        )
    ]


def test_full_cloud_storage_copy_keeps_valid_links() -> None:
    part = {**_part(), "disk_name": "s3"}
    base = _backup("base", parts={"all_1_1_0": part}, cloud_storage_disks=["s3"])
    base.cloud_storage.data_copied = True
    backup = _backup(
        "next",
        parts={"all_1_1_0": {**part, "link": "base"}},
        cloud_storage_disks=["s3"],
    )
    backup.cloud_storage.data_copied = True
    ctx = CopyTestContext([base, backup], [base])

    ctx.copy(["next"])

    assert ctx.target_layout.backups["next"].get_parts()[0].link == "base"
    assert not ctx.target_layout.copied
    assert not ctx.target_layout.cloud_storage_files


def test_full_cloud_storage_copy_rejects_missing_linked_metadata() -> None:
    part = {**_part(link="missing"), "disk_name": "s3"}
    backup = _backup("next", parts={"all_1_1_0": part}, cloud_storage_disks=["s3"])
    backup.cloud_storage.data_copied = True
    ctx = CopyTestContext([backup])

    with pytest.raises(
        ClickhouseBackupError, match="holding cloud storage data is missing"
    ):
        ctx.copy(["next"])

    assert not ctx.target_layout.uploaded


def test_full_cloud_storage_copy_separates_disk_prefixes() -> None:
    backup = _backup(
        "b1",
        parts={
            "all_1_1_0": {**_part(), "disk_name": "s3"},
            "all_2_2_0": {**_part(), "disk_name": "s3_other"},
        },
        cloud_storage_disks=["s3", "s3_other"],
    )
    backup.cloud_storage.data_copied = True
    ctx = CopyTestContext([backup])
    ctx.target_config["copy"]["server_side_copy"] = False
    for part in backup.get_parts():
        ctx.layout.cloud_storage_files[
            f"ch_backup/b1/disks/{part.disk_name}/db1/t1.tar.gz"
        ] = [
            (
                f"store/abc/uuid/{part.name}/data.bin",
                _object_storage_metadata(("same-key", 100)),
            )
        ]

    ctx.copy(["b1"])

    assert ctx.copied_paths() == {
        f"ch_backup/b1/cloud_storage/{disk}/same-key": f"ch_backup_copy/b1/cloud_storage/{disk}/same-key"
        for disk in backup.cloud_storage.disks
    }


@pytest.mark.parametrize("already_in_target", [False, True])
def test_copy_rejects_conflicting_sanitized_names(already_in_target: bool) -> None:
    backups = [_backup("backup-1"), _backup("backup_1")]
    ctx = CopyTestContext(
        backups[:1] if already_in_target else backups,
        backups[1:] if already_in_target else [],
    )

    with pytest.raises(ClickhouseBackupError, match="their sanitized names are equal"):
        ctx.copy()

    assert not ctx.target_layout.uploaded
    assert not ctx.target_layout.copied


@pytest.mark.parametrize("keep_going", [False, True])
def test_copy_reports_partial_results_and_continues(keep_going: bool) -> None:
    ctx = CopyTestContext(
        [
            _backup(f"b{i}", start_time=f"2026-01-0{i} 00:00:00 +0000")
            for i in range(1, 4)
        ]
    )
    ctx.target_config["copy"]["server_side_copy"] = False
    for name in ("b1", "b2", "b3"):
        _with_files(ctx, name, {"metadata/a.tar": 1, "metadata/b.tar": 2})
    ctx.target_layout.object_errors["ch_backup/b2/metadata/a.tar"] = RuntimeError(
        "object unavailable"
    )
    report = CopyReport()

    with pytest.raises(ClickhouseBackupError if keep_going else RuntimeError):
        ctx.copy(keep_going=keep_going, report=report)

    assert [r.status for r in report.backups] == [
        "copied",
        "failed",
        "copied" if keep_going else "not_processed",
    ]
    first, failed, last = report.backups
    assert (first.objects_total, first.objects_copied, first.known_bytes_copied) == (
        2,
        2,
        3,
    )
    assert (failed.objects_total, failed.objects_copied, failed.objects_failed) == (
        2,
        int(keep_going),
        1,
    )
    assert failed.known_bytes_copied == (2 if keep_going else 0)
    assert failed.errors == [
        {
            "stage": "copy",
            "message": "RuntimeError: object unavailable",
            "source": "ch_backup/b2/metadata/a.tar",
            "target": "ch_backup_copy/b2/metadata/a.tar",
        }
    ]
    assert ctx.target_layout.backups["b2"].state == BackupState.FAILED
    assert last.objects_total == (2 if keep_going else None)
    assert report.error

    ctx.target_layout.object_errors.clear()
    assert ctx.copy(keep_going=True, report=report) == (
        ["b2"] if keep_going else ["b2", "b3"],
        ["b1", "b3"] if keep_going else ["b1"],
    )
    assert report.error is None
    assert ctx.target_layout.backups["b2"].state == BackupState.CREATED


def test_copy_keep_going_skips_corrupt_source_metadata() -> None:
    ctx = CopyTestContext([_backup("b1"), _backup("broken"), _backup("b3")])
    report = CopyReport()
    original = ctx.layout.get_backup

    def read(name, use_light_meta=False):
        if name == "broken" and use_light_meta:
            raise StorageError("invalid metadata")
        return original(name, use_light_meta)

    with patch.object(ctx.layout, "get_backup", side_effect=read):
        with pytest.raises(ClickhouseBackupError, match="broken"):
            ctx.copy(keep_going=True, report=report)
    assert [(r.name, r.status) for r in report.backups] == [
        ("b1", "copied"),
        ("b3", "copied"),
        ("broken", "failed"),
    ]
    assert report.backups[-1].errors[0]["stage"] == "selection"
    assert report.backups[-1].objects_total is None
    assert "broken" not in ctx.target_layout.backups


@pytest.mark.parametrize("explicit_names", [False, True])
@pytest.mark.parametrize("keep_going", [False, True])
def test_selection_failure_preserves_unprocessed_candidates(
    explicit_names: bool, keep_going: bool, capsys: pytest.CaptureFixture[str]
) -> None:
    names = ["a_good", "b_broken", "c_good"]
    ctx = CopyTestContext([_backup(name) for name in names])
    report = CopyReport()
    failure = StorageError("invalid metadata")
    original = ctx.layout.get_backup

    def read(name, use_light_meta=False):
        if name == "b_broken" and use_light_meta:
            raise failure
        return original(name, use_light_meta)

    requested = names + ["a_good"] if explicit_names else None
    with patch.object(ctx.layout, "get_backup", side_effect=read):
        with pytest.raises(
            ClickhouseBackupError if keep_going else StorageError
        ) as raised:
            ctx.copy(requested, keep_going=keep_going, report=report)

    assert len(report.backups) == 3
    assert {r.name: r.status for r in report.backups} == {
        "a_good": "copied" if keep_going else "not_processed",
        "b_broken": "failed",
        "c_good": "copied" if keep_going else "not_processed",
    }
    broken = next(r for r in report.backups if r.name == "b_broken")
    assert broken.errors == [
        {"stage": "selection", "message": "StorageError: invalid metadata"}
    ]
    if not keep_going:
        assert raised.value is failure
        assert not ctx.target_layout.uploaded
        assert not ctx.target_layout.copied
        assert all(r.objects_total is None for r in report.backups)
    print_copy_report(report, None)
    assert f"not_processed: {0 if keep_going else 2}" in capsys.readouterr().out


def test_selection_excludes_known_ineligible_candidates_from_report() -> None:
    ctx = CopyTestContext(
        [
            _backup("failed", state=BackupState.FAILED),
            _backup("healthy"),
            _backup("missing"),
        ]
    )
    report = CopyReport()
    original = ctx.layout.get_backup

    def read(name, use_light_meta=False):
        return None if name == "missing" else original(name, use_light_meta)

    with patch.object(ctx.layout, "get_backup", side_effect=read):
        assert ctx.copy(report=report) == (["healthy"], [])
    assert [(r.name, r.status) for r in report.backups] == [("healthy", "copied")]


@pytest.mark.parametrize("explicit_names", [False, True])
def test_selection_rejects_mismatched_metadata_names(explicit_names: bool) -> None:
    ctx = CopyTestContext([_backup("b1"), _backup("b2")])
    report = CopyReport()
    original = ctx.layout.get_backup

    def read(name, use_light_meta=False):
        return _backup("different") if name == "b1" else original(name, use_light_meta)

    with patch.object(ctx.layout, "get_backup", side_effect=read):
        with pytest.raises(ClickhouseBackupError, match='name does not match "b1"'):
            ctx.copy(["b1", "b2"] if explicit_names else None, report=report)
    assert [(r.name, r.status) for r in report.backups] == [
        ("b1", "failed"),
        ("b2", "not_processed"),
    ]
    assert not ctx.target_layout.uploaded


@pytest.mark.parametrize("target_inventory", [False, True])
def test_copy_inventory_errors_stop_keep_going(target_inventory: bool) -> None:
    ctx = CopyTestContext([_backup("b1")])
    report = CopyReport()
    layout = ctx.target_layout if target_inventory else ctx.layout
    method = "get_backup_names"
    with patch.object(layout, method, side_effect=StorageError("listing failed")):
        with pytest.raises(StorageError, match="listing failed"):
            ctx.copy(keep_going=True, report=report)
    assert not ctx.target_layout.uploaded
    assert report.error == "StorageError: listing failed"


@pytest.mark.parametrize("already_in_target", [False, True])
def test_copy_keep_going_isolates_name_conflicts(already_in_target: bool) -> None:
    backup = _backup("backup-1")
    conflict = _backup("backup_1")
    ctx = CopyTestContext(
        (
            [backup, _backup("b3")]
            if already_in_target
            else [backup, conflict, _backup("b3")]
        ),
        [conflict] if already_in_target else [],
    )
    report = CopyReport()
    with pytest.raises(ClickhouseBackupError):
        ctx.copy(keep_going=True, report=report)
    expected = (
        {"backup-1": "failed", "b3": "copied"}
        if already_in_target
        else {"backup-1": "copied", "backup_1": "failed", "b3": "copied"}
    )
    assert {r.name: r.status for r in report.backups} == expected
    assert (
        "their sanitized names are equal"
        in next(r for r in report.backups if r.status == "failed").errors[0]["message"]
    )


@pytest.mark.parametrize("keep_going", [False, True])
def test_missing_cloud_target_does_not_block_local_backups(keep_going: bool) -> None:
    ctx = CopyTestContext(
        [_backup("b1"), _backup("cloud", cloud_storage_disks=["s3"]), _backup("b3")]
    )
    report = CopyReport()
    with pytest.raises(ClickhouseBackupError):
        ctx.copy(keep_going=keep_going, report=report)
    assert {r.name: r.status for r in report.backups} == {
        "b1": "copied" if keep_going else "not_processed",
        "cloud": "failed",
        "b3": "copied" if keep_going else "not_processed",
    }
    assert "cloud" not in ctx.target_layout.backups
    if not keep_going:
        assert not ctx.target_layout.uploaded


def test_copy_planning_errors_preserve_independent_work_and_allow_retry() -> None:
    ctx = CopyTestContext(
        [
            _backup(
                "broken",
                parts={
                    "all_1_1_0": _part(link="missing1"),
                    "all_2_2_0": _part(link="missing2"),
                    "all_3_3_0": _part(link="available"),
                },
            ),
            _backup("healthy"),
        ]
    )
    ctx.target_config["copy"]["server_side_copy"] = False
    _with_files(ctx, "broken", {"metadata/a.tar": 1, "metadata/b.tar": 2})
    _with_files(ctx, "available", {"data/db1/t1/all_3_3_0/all_3_3_0.tar": 100})
    report = CopyReport()
    with pytest.raises(ClickhouseBackupError):
        ctx.copy(keep_going=True, report=report)
    assert ctx.target_layout.backups["broken"].state == BackupState.FAILED
    failed = report.backups[0]
    assert len(failed.errors) == 2
    assert all(e["stage"] == "planning" for e in failed.errors)
    assert [e["source"].split("/", 1)[0] for e in failed.errors] == [
        "missing1",
        "missing2",
    ]
    assert (
        failed.objects_copied,
        failed.objects_failed,
        failed.known_bytes_copied,
    ) == (
        3,
        0,
        103,
    )
    assert [(r.status, r.objects_total) for r in report.backups] == [
        ("failed", 3),
        ("copied", 0),
    ]
    for name, part_name in (("missing1", "all_1_1_0"), ("missing2", "all_2_2_0")):
        _with_files(ctx, name, {f"data/db1/t1/{part_name}/{part_name}.tar": 100})
    assert ctx.copy(keep_going=True, report=report) == (["broken"], ["healthy"])
    assert ctx.target_layout.backups["broken"].state == BackupState.CREATED
    assert all(p.link is None for p in ctx.target_layout.backups["broken"].get_parts())
    assert report.backups[0].objects_total == report.backups[0].objects_copied == 5
    assert not report.backups[0].errors


@pytest.mark.parametrize("keep_going", [False, True])
def test_copy_isolates_corrupt_target_metadata(keep_going: bool) -> None:
    ctx = CopyTestContext(
        [_backup(name) for name in ("a", "b", "c")],
        [_backup("b", state=BackupState.FAILED)],
    )
    original = ctx.target_layout.get_backup

    def read(name, use_light_meta=False):
        if name == "b" and use_light_meta:
            raise StorageError("invalid target metadata")
        return original(name, use_light_meta)

    report = CopyReport()
    with patch.object(ctx.target_layout, "get_backup", side_effect=read):
        with pytest.raises(ClickhouseBackupError):
            ctx.copy(keep_going=keep_going, report=report)
    assert {r.name: r.status for r in report.backups} == {
        "a": "copied" if keep_going else "not_processed",
        "b": "failed",
        "c": "copied" if keep_going else "not_processed",
    }
    assert report.backups[1].errors == [
        {
            "stage": "target_selection",
            "message": "StorageError: invalid target metadata",
        }
    ]
    assert ctx.target_layout.backups["b"].state == BackupState.FAILED
    assert not any(name == "b" for name, _ in ctx.target_layout.uploaded)


def test_unreadable_target_reserves_sanitized_name() -> None:
    ctx = CopyTestContext(
        [_backup("backup-1"), _backup("healthy")], [_backup("backup_1")]
    )
    original = ctx.target_layout.get_backup

    def read(name, use_light_meta=False):
        if name == "backup_1":
            raise StorageError("invalid target metadata")
        return original(name, use_light_meta)

    report = CopyReport()
    with patch.object(ctx.target_layout, "get_backup", side_effect=read):
        with pytest.raises(ClickhouseBackupError):
            ctx.copy(keep_going=True, report=report)
    assert {r.name: r.status for r in report.backups} == {
        "backup-1": "failed",
        "healthy": "copied",
        "backup_1": "failed",
    }
    assert "their sanitized names are equal" in report.backups[0].errors[0]["message"]
    assert ctx.target_layout.uploaded == [
        ("healthy", BackupState.CREATING),
        ("healthy", BackupState.CREATED),
    ]


@pytest.mark.parametrize("full_cloud", [False, True])
@pytest.mark.parametrize("keep_going", [False, True])
def test_unreadable_target_dependency_uses_source_data(
    full_cloud: bool, keep_going: bool
) -> None:
    disk = "s3" if full_cloud else "default"
    owner = _backup(
        "owner",
        parts={"all_1_1_0": {**_part(), "disk_name": disk}},
        cloud_storage_disks=[disk] if full_cloud else [],
    )
    child = _backup(
        "child",
        parts={"all_1_1_0": {**_part(link="owner"), "disk_name": disk}},
        cloud_storage_disks=[disk] if full_cloud else [],
    )
    owner.cloud_storage.data_copied = child.cloud_storage.data_copied = full_cloud
    ctx = CopyTestContext([owner, child], [owner])
    ctx.target_config["copy"]["server_side_copy"] = False
    if full_cloud:
        _with_files(ctx, "owner", {"cloud_storage/s3/xyz/data": 101})
        ctx.layout.cloud_storage_files["ch_backup/owner/disks/s3/db1/t1.tar.gz"] = [
            ("all_1_1_0/data.bin", _object_storage_metadata(("xyz/data", 101))),
        ]
    else:
        _with_files(ctx, "owner", {"data/db1/t1/all_1_1_0/all_1_1_0.tar": 101})
    original = ctx.target_layout.get_backup

    def read(name, use_light_meta=False):
        if name == "owner" and not use_light_meta:
            raise StorageError("invalid target full metadata")
        return original(name, use_light_meta)

    report = CopyReport()
    with patch.object(ctx.target_layout, "get_backup", side_effect=read):
        if not keep_going:
            with pytest.raises(StorageError, match="invalid target full metadata"):
                ctx.copy(["child"], report=report)
            assert not ctx.target_layout.uploaded
            return
        assert ctx.copy(["child"], keep_going=True, report=report) == (["child"], [])
    assert ctx.target_layout.backups["child"].state == BackupState.CREATED
    assert ctx.target_layout.backups["child"].get_parts()[0].link is None
    assert report.backups[0].known_bytes_copied == 101
    assert not report.backups[0].errors


@pytest.mark.parametrize("full_cloud", [False, True])
def test_cloud_planning_continues_after_corrupt_metadata_files(
    full_cloud: bool,
) -> None:
    ctx = _cloud_storage_context()
    backup = _backup(
        "b1",
        parts={"all_1_1_0": {**_part(), "disk_name": "s3"}},
        cloud_storage_disks=["s3"],
    )
    ctx.layout.backups["b1"] = backup
    backup.cloud_storage.data_copied = full_cloud
    tarball = "ch_backup/b1/disks/s3/db1/t1.tar.gz"
    ctx.layout.cloud_storage_files[tarball] = [
        ("all_1_1_0/data.bin", b"invalid metadata"),
        ("all_1_1_0/checksums.txt", _object_storage_metadata(("xyz/good", 100))),
        ("all_1_1_0/count.txt", b"also invalid"),
    ]
    report = CopyReport()
    with pytest.raises(ClickhouseBackupError):
        ctx.copy(keep_going=True, report=report)
    result = report.backups[0]
    assert len(result.errors) == 2
    assert all(e["stage"] == "planning" for e in result.errors)
    assert result.known_bytes_copied >= 100
    assert any(e.dst_path.endswith("xyz/good") for e in ctx.target_layout.copied)
    assert ctx.target_layout.backups["b1"].state == BackupState.FAILED


@pytest.mark.parametrize("full_cloud", [False, True])
@pytest.mark.parametrize("version", [3, 5])
@pytest.mark.parametrize("keep_going", [False, True])
def test_cloud_planning_continues_within_metadata_file_and_retries(
    full_cloud: bool, version: int, keep_going: bool
) -> None:
    ctx = _cloud_storage_context()
    backup = _backup(
        "b1",
        parts={"all_1_1_0": {**_part(), "disk_name": "s3"}},
        cloud_storage_disks=["s3"],
    )
    backup.cloud_storage.data_copied = full_cloud
    ctx.layout.backups["b1"] = backup
    src_prefix = "ch_backup/b1/cloud_storage/s3" if full_cloud else "data"
    key = f"{src_prefix}/xyz/good" if version == 5 else "xyz/good"
    tarball = "ch_backup/b1/disks/s3/db1/t1.tar.gz"
    file_name = "all_1_1_0/data.bin"
    data = _object_storage_metadata(
        ("../invalid1", 1), (key, 100), ("../invalid2", 2), version=version
    )
    ctx.layout.cloud_storage_files[tarball] = [(file_name, data)]
    report = CopyReport()

    with pytest.raises(ClickhouseBackupError):
        ctx.copy(keep_going=keep_going, report=report)
    if keep_going:
        result = report.backups[0]
        assert len(result.errors) == 2
        assert [e["source"] for e in result.errors] == [
            f"{tarball}:{file_name}:../invalid1",
            f"{tarball}:{file_name}:../invalid2",
        ]
        assert result.known_bytes_copied >= 100
        assert f"{src_prefix}/xyz/good" in ctx.copied_paths()
        assert ctx.target_layout.backups["b1"].state == BackupState.FAILED
        assert not any(s == BackupState.CREATED for _, s in ctx.target_layout.uploaded)
        if full_cloud:
            # Incomplete V5 metadata must not be rewritten with a shortened list.
            assert ctx.target_layout.cloud_storage_files[
                "ch_backup_copy/b1/disks/s3/db1/t1.tar.gz"
            ] == [(file_name, data)]
    else:
        assert not ctx.target_layout.uploaded
        assert not ctx.target_layout.copied
        assert len(report.backups[0].errors) == 1

    ctx.layout.cloud_storage_files[tarball] = [
        (file_name, _object_storage_metadata((key, 100), version=version))
    ]
    assert ctx.copy(keep_going=keep_going, report=report) == (["b1"], [])
    assert ctx.target_layout.backups["b1"].state == BackupState.CREATED
    assert not report.backups[0].errors
    assert report.backups[0].objects_total == report.backups[0].objects_copied


def test_copy_materializes_links_after_owner_copy_fails() -> None:
    owner = _backup("b1", parts={"all_1_1_0": _part()})
    linked = _backup(
        "b2",
        start_time="2026-01-02 00:00:00 +0000",
        parts={"all_1_1_0": _part(link="b1")},
    )
    ctx = CopyTestContext([owner, linked])
    ctx.target_config["copy"]["server_side_copy"] = False
    _with_files(
        ctx, "b1", {"metadata/a.tar": 1, "data/db1/t1/all_1_1_0/all_1_1_0.tar": 100}
    )
    ctx.target_layout.object_errors["ch_backup/b1/metadata/a.tar"] = RuntimeError(
        "cannot copy metadata"
    )
    with pytest.raises(ClickhouseBackupError):
        ctx.copy(keep_going=True)
    target = ctx.target_layout.backups["b2"]
    assert target.state == BackupState.CREATED
    assert target.get_parts()[0].link is None
    assert any(
        e.dst_path == "ch_backup_copy/b2/data/db1/t1/all_1_1_0/all_1_1_0.tar"
        for e in ctx.target_layout.copied
    )


@pytest.mark.parametrize("cloud", [False, True])
@pytest.mark.parametrize("renamed", [False, True])
@pytest.mark.parametrize("materialized_backup_fails", [False, True])
def test_copy_reuses_only_successfully_materialized_parts(
    cloud: bool, renamed: bool, materialized_backup_fails: bool
) -> None:
    names = (
        ["all_1_1_0", "all_1_1_0_2", "all_1_1_0_3"] if renamed else ["all_1_1_0"] * 3
    )
    backups = [
        _backup(
            name,
            start_time=f"2026-01-0{index + 1} 00:00:00 +0000",
            parts={
                names[index]: {
                    **_part(
                        link="a" if index else None,
                        link_part_name=names[0] if index else None,
                    ),
                    "disk_name": "s3" if cloud else "default",
                }
            },
            cloud_storage_disks=["s3"] if cloud else [],
        )
        for index, name in enumerate(("a", "b", "c"))
    ]
    for backup in backups:
        backup.cloud_storage.data_copied = cloud
    ctx = CopyTestContext(backups)
    ctx.target_config["copy"]["server_side_copy"] = False
    part_path = (
        "cloud_storage/s3/data" if cloud else f"data/db1/t1/{names[0]}/{names[0]}.tar"
    )
    _with_files(ctx, "a", {"metadata/a.tar": 1, part_path: 100})
    if cloud:
        ctx.layout.cloud_storage_files["ch_backup/a/disks/s3/db1/t1.tar.gz"] = [
            (f"store/uuid/{names[0]}/data.bin", _object_storage_metadata(("data", 100)))
        ]
    ctx.target_layout.object_errors["ch_backup/a/metadata/a.tar"] = RuntimeError(
        "owner failed"
    )
    upload = ctx.target_layout.upload_backup_metadata

    def fail_publication(backup: BackupMetadata) -> None:
        if (
            materialized_backup_fails
            and backup.name == "b"
            and backup.state == BackupState.CREATED
        ):
            raise RuntimeError("publication failed")
        upload(backup)

    with patch.object(
        ctx.target_layout, "upload_backup_metadata", side_effect=fail_publication
    ):
        with pytest.raises(ClickhouseBackupError):
            ctx.copy(keep_going=True)
    target = ctx.target_layout.backups["c"]
    part = target.get_parts()[0]
    assert target.state == BackupState.CREATED
    assert part.link == (None if materialized_backup_fails else "b")
    assert part.link_part_name == (
        names[1] if renamed and not materialized_backup_fails else None
    )
    assert target.real_size == (100 if materialized_backup_fails else 0)
    copied_to_c = [
        e
        for e in ctx.target_layout.copied
        if e.dst_path.startswith("ch_backup_copy/c/")
    ]
    assert bool(copied_to_c) == materialized_backup_fails


@pytest.mark.parametrize("different_checksum", [False, True])
def test_copy_reuses_materialized_parts_from_skipped_backup(
    different_checksum: bool,
) -> None:
    ctx = CopyTestContext(
        [
            _backup(
                "b", parts={"all_1_1_0_2": _part(link="a", link_part_name="all_1_1_0")}
            ),
            _backup(
                "c",
                start_time="2026-01-02 00:00:00 +0000",
                parts={
                    "all_1_1_0_3": _part(
                        link="a",
                        link_part_name="all_1_1_0",
                        checksum="other" if different_checksum else "checksum1",
                    )
                },
            ),
        ]
    )
    ctx.target_config["copy"]["server_side_copy"] = False
    _with_files(ctx, "a", {"data/db1/t1/all_1_1_0/all_1_1_0.tar": 100})
    assert ctx.copy(["b"]) == (["b"], [])
    ctx.target_layout.copied.clear()
    assert ctx.copy() == (["c"], ["b"])
    part = ctx.target_layout.backups["c"].get_parts()[0]
    assert part.link == (None if different_checksum else "b")
    assert bool(ctx.target_layout.copied) == different_checksum


@pytest.mark.parametrize("keep_going", [False, True])
def test_copy_probe_failure_can_fall_back_to_streaming(keep_going: bool) -> None:
    ctx = CopyTestContext([_backup("b1")])
    _with_files(ctx, "b1", {"metadata/a.tar": 1, "metadata/b.tar": 2})
    report = CopyReport()
    with patch.object(
        FakeEngine, "try_copy_object", side_effect=RuntimeError("probe failed")
    ):
        if keep_going:
            assert ctx.copy(keep_going=True, report=report) == (["b1"], [])
            assert not report.backups[0].errors
            assert report.backups[0].objects_copied == 2
            assert all(not e.server_side for e in ctx.target_layout.copied)
        else:
            with pytest.raises(RuntimeError, match="probe failed"):
                ctx.copy(report=report)


def test_copy_counts_server_side_probe_once() -> None:
    ctx = CopyTestContext([_backup("b1")])
    _with_files(ctx, "b1", {"metadata/a.tar": 1, "metadata/b.tar": 2})
    report = CopyReport()
    ctx.copy(report=report)
    result = report.backups[0]
    assert (result.objects_total, result.objects_copied, result.known_bytes_copied) == (
        2,
        2,
        3,
    )
    assert len(ctx.store.probes) == 1
    assert len(ctx.target_layout.copied) == 1


@pytest.mark.parametrize("metadata_failure", [False, True])
def test_copy_counts_rewritten_metadata(metadata_failure: bool) -> None:
    ctx = _cloud_storage_context()
    src_path = "ch_backup/b1/disks/s3/db1/t1.tar.gz"
    dst_path = "ch_backup_copy/b1/disks/s3/db1/t1.tar.gz"
    ctx.layout.cloud_storage_files[src_path] = [
        ("part/data.bin", _object_storage_metadata(("data/xyz/aaa", 1000), version=5))
    ]
    if metadata_failure:
        ctx.target_layout.metadata_errors[dst_path] = RuntimeError(
            "metadata upload failed"
        )
    report = CopyReport()
    if metadata_failure:
        with pytest.raises(ClickhouseBackupError):
            ctx.copy(keep_going=True, report=report)
        assert ctx.target_layout.backups["b1"].state == BackupState.FAILED
    else:
        ctx.copy(keep_going=True, report=report)
    result = report.backups[0]
    assert (
        result.objects_total,
        result.objects_copied,
        result.objects_failed,
        result.known_bytes_copied,
    ) == (4, 3 if metadata_failure else 4, int(metadata_failure), 1150)
    if metadata_failure:
        assert result.errors[0]["target"] == dst_path


@pytest.mark.parametrize("keep_going", [False, True])
def test_copy_records_synchronous_cloud_metadata_validation_failure(
    keep_going: bool,
) -> None:
    ctx = _cloud_storage_context()
    ctx.layout.backups["b1"].cloud_storage.encrypted = False
    src_path = "ch_backup/b1/disks/s3/db1/t1.tar.gz"
    dst_path = "ch_backup_copy/b1/disks/s3/db1/t1.tar.gz"
    ctx.layout.cloud_storage_files[src_path] = [
        ("part/data.bin", _object_storage_metadata(("data/xyz/aaa", 1000), version=5)),
        ("part/count.txt", b"5\n0\t1\n0\n0\n1\n"),
    ]
    ctx.layout.backups["b2"] = _backup("b2", start_time="2026-01-02 00:00:00 +0000")
    _with_files(ctx, "b2", {"metadata/db.sql": 10})
    with patch("ch_backup.backup.layout.StorageLoader") as loader:
        validating_layout = BackupLayout(ctx.target_config)  # type: ignore[arg-type]
    report = CopyReport()
    with patch.object(
        ctx.target_layout,
        "upload_cloud_storage_metadata_files",
        side_effect=validating_layout.upload_cloud_storage_metadata_files,
    ):
        with pytest.raises(ClickhouseBackupError if keep_going else StorageError):
            ctx.copy(keep_going=keep_going, report=report)

    loader.return_value.upload_data_tarball.assert_not_called()
    assert dst_path not in {entry.dst_path for entry in ctx.target_layout.copied}
    assert dst_path not in ctx.target_layout.cloud_storage_files
    assert ctx.target_layout.backups["b1"].state == BackupState.FAILED
    assert ctx.target_layout.uploaded[:2] == [
        ("b1", BackupState.CREATING),
        ("b1", BackupState.FAILED),
    ]
    failed, independent = report.backups
    assert failed.status == "failed"
    assert (failed.objects_total, failed.objects_copied, failed.objects_failed) == (
        4,
        3,
        1,
    )
    assert len(failed.errors) == 1
    assert failed.errors[0]["stage"] == "copy_metadata"
    assert failed.errors[0]["target"] == dst_path
    assert "inline file data" in failed.errors[0]["message"]
    assert independent.status == ("copied" if keep_going else "not_processed")

    # A corrected archive must be retried, rather than skipped as an existing copy.
    ctx.layout.cloud_storage_files[src_path].pop()
    assert ctx.copy(["b1"], keep_going=keep_going) == (["b1"], [])
    assert ctx.target_layout.backups["b1"].state == BackupState.CREATED
    assert dst_path in ctx.target_layout.cloud_storage_files


def test_failed_metadata_save_preserves_copy_error_and_clears_cache() -> None:
    ctx = CopyTestContext([_backup("b1")])
    ctx.target_config["copy"]["server_side_copy"] = False
    _with_files(ctx, "b1", {"metadata/a.tar": 1})
    ctx.target_layout.copy_error = RuntimeError("primary copy error")
    report = CopyReport()
    copier = ctx.make_copier()
    original = ctx.target_layout.upload_backup_metadata

    def upload(backup):
        if backup.state == BackupState.FAILED:
            try:
                raise OSError("metadata write failed")
            except OSError as error:
                raise StorageError("failed to save FAILED") from error
        original(backup)

    messages: list[str] = []
    sink = ch_logging.logger.add(
        messages.append, format="{message}", diagnose=False, backtrace=False
    )
    try:
        with (
            patch(
                "ch_backup.backup.copier.get_storage_engine",
                side_effect=ctx.store.engine,
            ),
            patch.object(
                ctx.target_layout, "upload_backup_metadata", side_effect=upload
            ),
        ):
            with pytest.raises(RuntimeError, match="primary copy error"):
                copier.copy(report=report)
    finally:
        ch_logging.logger.remove(sink)
    log = "".join(messages)
    assert "OSError: metadata write failed" in log
    assert "StorageError: failed to save FAILED" in log
    assert [e["message"] for e in report.backups[0].errors] == [
        "RuntimeError: primary copy error",
        "StorageError: failed to save FAILED",
    ]
    assert "b1" not in copier._target_backups  # pylint: disable=protected-access


def test_copy_signal_marks_current_backup_and_stops_keep_going() -> None:
    ctx = CopyTestContext([_backup("b1"), _backup("b2")])
    ctx.target_config["copy"]["server_side_copy"] = False
    report = CopyReport()
    with patch.object(
        ctx.target_layout, "copy_objects", side_effect=TerminatingSignal("interrupted")
    ):
        with pytest.raises(TerminatingSignal):
            ctx.copy(keep_going=True, report=report)
    assert [r.status for r in report.backups] == ["failed", "not_processed"]
    assert ctx.target_layout.backups["b1"].state == BackupState.FAILED
    assert report.error == "TerminatingSignal: interrupted"


def test_copy_broken_executor_stops_keep_going() -> None:
    ctx = CopyTestContext([_backup("b1"), _backup("b2")])
    report = CopyReport()
    failure = StorageError("wrapped executor failure")
    failure.__cause__ = BrokenExecutor("workers unavailable")
    with patch.object(ctx.target_layout, "wait", side_effect=failure):
        with pytest.raises(StorageError, match="wrapped executor failure"):
            ctx.copy(keep_going=True, report=report)
    assert [r.status for r in report.backups] == ["failed", "not_processed"]
    assert ctx.target_layout.backups["b1"].state == BackupState.FAILED


def test_copy_broken_executor_during_drain_preserves_primary_error() -> None:
    ctx = CopyTestContext([_backup("b1"), _backup("b2")])
    report = CopyReport()
    failure = RuntimeError("submission failed")
    drain_error = StorageError("drain failed")
    drain_error.__cause__ = BrokenExecutor("workers unavailable")
    with patch.object(ctx.target_layout, "copy_objects", side_effect=failure):
        with patch.object(ctx.target_layout, "wait", side_effect=drain_error):
            with pytest.raises(RuntimeError, match="submission failed") as raised:
                ctx.copy(keep_going=True, report=report)
    assert raised.value is failure
    assert [r.status for r in report.backups] == ["failed", "not_processed"]
    assert [e["message"] for e in report.backups[0].errors] == [
        "RuntimeError: submission failed",
        "StorageError: drain failed",
    ]


@pytest.mark.parametrize("failed_state", [BackupState.CREATING, BackupState.CREATED])
def test_copy_metadata_publication_failure_is_reported(
    failed_state: BackupState,
) -> None:
    ctx = CopyTestContext([_backup("b1")])
    _with_files(ctx, "b1", {"metadata/a.tar": 1})
    report = CopyReport()
    original = ctx.target_layout.upload_backup_metadata

    def upload(backup):
        if backup.state == failed_state:
            raise StorageError("publication failed")
        original(backup)

    with patch.object(ctx.target_layout, "upload_backup_metadata", side_effect=upload):
        with pytest.raises(StorageError, match="publication failed"):
            ctx.copy(report=report)
    result = report.backups[0]
    assert result.status == "failed"
    assert result.objects_total == 1
    assert result.objects_copied == int(failed_state == BackupState.CREATED)
    assert result.errors[0]["stage"] == "metadata"
    assert ctx.target_layout.backups["b1"].state == BackupState.FAILED
    if failed_state == BackupState.CREATING:
        assert not ctx.target_layout.copied
        assert not ctx.store.probes


def test_copy_continues_other_rewritten_metadata_uploads() -> None:
    ctx = _cloud_storage_context()
    for table, key in (("t1", "aaa"), ("t2", "bbb")):
        src_path = f"ch_backup/b1/disks/s3/db1/{table}.tar.gz"
        _with_files(ctx, "b1", {f"disks/s3/db1/{table}.tar.gz": 50})
        ctx.layout.cloud_storage_files[src_path] = [
            (
                "part/data.bin",
                _object_storage_metadata((f"data/xyz/{key}", 1000), version=5),
            )
        ]
    ctx.target_layout.metadata_errors["ch_backup_copy/b1/disks/s3/db1/t1.tar.gz"] = (
        RuntimeError("one archive failed")
    )
    report = CopyReport()
    with pytest.raises(ClickhouseBackupError):
        ctx.copy(keep_going=True, report=report)
    assert (
        "ch_backup_copy/b1/disks/s3/db1/t2.tar.gz"
        in ctx.target_layout.cloud_storage_files
    )
    result = report.backups[0]
    assert (result.objects_total, result.objects_copied, result.objects_failed) == (
        7,
        6,
        1,
    )
    assert result.known_bytes_copied == 2200


def _interrupted_copy(
    error: BaseException = StorageError("network error"), keep_going: bool = False
) -> CopyTestContext:
    """Copy one object of two before the other one fails."""
    ctx = CopyTestContext([_backup("b1")])
    ctx.target_config["copy"]["server_side_copy"] = False
    ctx.target_layout.store = ctx.store
    _with_files(ctx, "b1", {"metadata/a.tar": 1, "metadata/b.tar": 2})
    ctx.target_layout.object_errors["ch_backup/b1/metadata/b.tar"] = error
    with pytest.raises((type(error), ClickhouseBackupError)):
        ctx.copy(["b1"], keep_going=keep_going)
    ctx.target_layout.object_errors.clear()
    ctx.target_layout.copied.clear()
    return ctx


@pytest.mark.parametrize(
    "error,keep_going",
    [
        (StorageError("network error"), False),
        (StorageError("network error"), True),
        (TerminatingSignal("interrupted"), False),
    ],
)
def test_resume_skips_confirmed_objects(error: BaseException, keep_going: bool) -> None:
    ctx = _interrupted_copy(error, keep_going)
    assert ctx.target_layout.backups["b1"].state == BackupState.FAILED

    report = CopyReport()
    assert ctx.copy(["b1"], report=report) == (["b1"], [])

    assert set(ctx.copied_paths()) == {"ch_backup/b1/metadata/b.tar"}
    result = report.backups[0]
    assert (result.objects_total, result.objects_copied, result.objects_skipped) == (
        2,
        1,
        1,
    )
    assert ctx.target_layout.backups["b1"].state == BackupState.CREATED


def test_resume_after_kill_uses_periodic_saves() -> None:
    flush = CopyContext.flush
    with (
        patch(
            "ch_backup.backup.copier.CopyContext", partial(CopyContext, flush_size=1)
        ),
        patch.object(
            CopyContext, "flush", lambda self, force=False: force or flush(self)
        ),
    ):
        ctx = _interrupted_copy()

    ctx.copy(["b1"])

    assert set(ctx.copied_paths()) == {"ch_backup/b1/metadata/b.tar"}


@pytest.mark.parametrize("size", [None, 2])
@pytest.mark.parametrize("state", [BackupState.FAILED, BackupState.PARTIALLY_DELETED])
def test_resume_recopies_objects_missing_in_target(
    size: int | None, state: BackupState
) -> None:
    ctx = _interrupted_copy()
    ctx.target_layout.backups["b1"].state = state
    if size is None:
        del ctx.dst_objects["ch_backup_copy/b1/metadata/a.tar"]
    else:
        ctx.dst_objects["ch_backup_copy/b1/metadata/a.tar"] = size

    ctx.copy(["b1"])

    assert set(ctx.copied_paths()) == {
        "ch_backup/b1/metadata/a.tar",
        "ch_backup/b1/metadata/b.tar",
    }


def test_resume_without_target_metadata_ignores_copy_context() -> None:
    ctx = _interrupted_copy()
    del ctx.target_layout.backups["b1"]

    ctx.copy(["b1"])

    assert len(ctx.copied_paths()) == 2


def test_resume_recopies_objects_if_target_listing_fails() -> None:
    ctx = _interrupted_copy()
    ctx.store.list_errors.add("ch_backup_copy/b1/")

    ctx.copy(["b1"])

    assert len(ctx.copied_paths()) == 2


def test_copy_without_resume_recopies_confirmed_objects() -> None:
    ctx = _interrupted_copy()
    ctx.target_config["copy"]["resume"] = False
    report = CopyReport()

    ctx.copy(["b1"], report=report)

    assert len(ctx.copied_paths()) == 2
    assert report.backups[0].objects_skipped == 0
    assert ctx.target_layout.backups["b1"].state == BackupState.CREATED


def test_resume_skips_object_copied_by_server_side_probe() -> None:
    ctx = CopyTestContext([_backup("b1")])
    ctx.target_layout.store = ctx.store
    _with_files(ctx, "b1", {"metadata/a.tar": 1, "metadata/b.tar": 2})
    ctx.target_layout.object_errors["ch_backup/b1/metadata/b.tar"] = StorageError(
        "network error"
    )
    with pytest.raises(StorageError):
        ctx.copy(["b1"])
    assert ctx.store.probes == [
        (SRC_BUCKET, "ch_backup/b1/metadata/a.tar", "ch_backup_copy/b1/metadata/a.tar")
    ]
    ctx.dst_objects["ch_backup_copy/b1/metadata/a.tar"] = 1
    ctx.target_layout.object_errors.clear()
    ctx.target_layout.copied.clear()
    ctx.store.probes.clear()

    ctx.copy(["b1"])

    copied = {dst for _, _, dst in ctx.store.probes}
    copied.update(e.dst_path for e in ctx.target_layout.copied)
    assert copied == {"ch_backup_copy/b1/metadata/b.tar"}


def test_resume_skips_confirmed_cloud_storage_objects() -> None:
    ctx = _cloud_storage_context()
    ctx.target_layout.store = ctx.store
    ctx.target_layout.object_errors["data/xyz/bbb"] = StorageError("SlowDown")
    with pytest.raises(StorageError):
        ctx.copy(["b1"])
    ctx.target_layout.object_errors.clear()
    ctx.target_layout.copied.clear()

    ctx.copy(["b1"])

    assert {e.dst_path for e in ctx.target_layout.copied} == {"copies/b1/xyz/bbb"}


def test_copy_does_not_copy_copy_context_of_source() -> None:
    ctx = CopyTestContext([_backup("b1")])
    ctx.target_config["copy"]["server_side_copy"] = False
    _with_files(
        ctx,
        "b1",
        {"metadata/a.tar": 1, "copy_context/0123456789abcdef/x.jsonl.gz": 10},
    )

    ctx.copy(["b1"])

    assert set(ctx.copied_paths()) == {"ch_backup/b1/metadata/a.tar"}


def _scoped_context(version: int = 3) -> CopyTestContext:
    """Backup with data of object storage disks copied into target storage tracked by the store."""
    ctx = _cloud_storage_context()
    ctx.target_layout.store = ctx.store
    _with_files(ctx, "b1", {"metadata/a.tar": 1})
    if version >= 5:
        ctx.layout.cloud_storage_files["ch_backup/b1/disks/s3/db1/t1.tar.gz"] = [
            (
                "part/data.bin",
                _object_storage_metadata(("data/xyz/aaa", 1000), version=version),
            )
        ]
    return ctx


def _scopes(backup: BackupMetadata) -> dict[str, str]:
    """Return statuses of scopes saved in metadata of the copy."""
    assert backup.copy
    return backup.copy["scopes"]


def _disk_objects(ctx: CopyTestContext) -> set[str]:
    """Return target keys of copied objects of object storage disks."""
    return {
        e.dst_path
        for e in ctx.target_layout.copied
        if e.dst_storage["credentials"]["bucket"] == "disk-copy"
    }


def test_copy_scopes_separately() -> None:
    ctx = _scoped_context()
    report = CopyReport()

    assert ctx.copy(["b1"], scopes=["files"], report=report) == (["b1"], [])

    target = ctx.target_layout.backups["b1"]
    assert target.state == BackupState.CREATING
    assert _scopes(target) == {"files": "completed", "cloud_storage": "pending"}
    assert report.backups[0].scopes == _scopes(target)
    assert not _disk_objects(ctx)
    ctx.target_layout.copied.clear()

    assert ctx.copy(["b1"], scopes=["cloud_storage"]) == (["b1"], [])

    target = ctx.target_layout.backups["b1"]
    assert target.state == BackupState.CREATED
    assert _scopes(target) == {"files": "completed", "cloud_storage": "completed"}
    assert set(ctx.copied_paths().values()) == _disk_objects(ctx) | {
        "ch_backup_copy/b1/disks/s3/db1/t1.tar.gz"
    }
    assert _disk_objects(ctx) == {"copies/b1/xyz/aaa", "copies/b1/xyz/bbb"}


def test_copy_scopes_of_backup_without_cloud_storage() -> None:
    ctx = CopyTestContext([_backup("b1")])
    _with_files(ctx, "b1", {"metadata/a.tar": 1})

    ctx.copy(["b1"], scopes=["files"])

    target = ctx.target_layout.backups["b1"]
    assert target.state == BackupState.CREATED
    assert _scopes(target) == {"files": "completed", "cloud_storage": "completed"}


def test_copy_rejects_unknown_scope() -> None:
    ctx = CopyTestContext([_backup("b1")])

    with pytest.raises(ConfigurationError, match="data"):
        ctx.copy(["b1"], scopes=["data"])


def test_cloud_storage_scope_lists_failed_objects() -> None:
    ctx = _scoped_context()
    ctx.target_layout.object_errors["data/xyz/bbb"] = StorageError("SlowDown")
    report = CopyReport()

    with pytest.raises(ClickhouseBackupError):
        ctx.copy(["b1"], keep_going=True, report=report)

    target = ctx.target_layout.backups["b1"]
    assert target.state == BackupState.FAILED
    assert _scopes(target) == {"files": "completed", "cloud_storage": "failed"}
    data = _failed_objects(ctx)
    assert data is not None
    failed = [json.loads(line) for line in gzip.decompress(data).decode().splitlines()]
    assert failed == [
        {
            "src": "data/xyz/bbb",
            "dst": "copies/b1/xyz/bbb",
            "size": 10,
            "error": "StorageError: SlowDown",
        }
    ]
    ctx.target_layout.object_errors.clear()
    ctx.target_layout.copied.clear()
    report = CopyReport()

    ctx.copy(["b1"], scopes=["cloud_storage"], report=report)

    assert set(ctx.copied_paths().values()) == {"copies/b1/xyz/bbb"}
    assert report.backups[0].objects_skipped == 2
    assert ctx.target_layout.backups["b1"].state == BackupState.CREATED
    assert _failed_objects(ctx) is None


def _failed_objects(ctx: CopyTestContext, scope: str = "cloud_storage") -> bytes | None:
    """Return the saved list of objects of the scope that failed to copy."""
    return next(
        (
            data
            for (_, path), data in ctx.store.data.items()
            if path.endswith(f"/failed/{scope}.jsonl.gz")
        ),
        None,
    )


def test_files_scope_lists_failed_objects() -> None:
    ctx = _scoped_context()
    ctx.target_layout.object_errors["ch_backup/b1/metadata/a.tar"] = StorageError(
        "SlowDown"
    )

    with pytest.raises(ClickhouseBackupError):
        ctx.copy(["b1"], keep_going=True)

    assert _scopes(ctx.target_layout.backups["b1"]) == {
        "files": "failed",
        "cloud_storage": "completed",
    }
    data = _failed_objects(ctx, "files")
    assert data is not None
    assert [json.loads(line)["src"] for line in gzip.decompress(data).splitlines()] == [
        "ch_backup/b1/metadata/a.tar"
    ]
    assert _failed_objects(ctx) is None


def test_completed_scope_with_deleted_objects_is_copied_again() -> None:
    ctx = _scoped_context()
    path = "ch_backup_copy/b1/metadata/a.tar"
    ctx.copy(["b1"], scopes=["files"])
    size = ctx.dst_objects.pop(path)

    ctx.copy(["b1"], scopes=["cloud_storage"])

    target = ctx.target_layout.backups["b1"]
    assert target.state == BackupState.CREATING
    assert _scopes(target) == {"files": "pending", "cloud_storage": "completed"}

    ctx.copy(["b1"], scopes=["files"])

    assert ctx.dst_objects[path] == size
    assert ctx.target_layout.backups["b1"].state == BackupState.CREATED


@pytest.mark.parametrize("replaced", [False, True])
def test_completed_scope_with_changed_rewritten_metadata_is_copied_again(
    replaced: bool,
) -> None:
    ctx = _scoped_context(version=5)
    tarball = "ch_backup_copy/b1/disks/s3/db1/t1.tar.gz"
    ctx.copy(["b1"], scopes=["files"])
    ctx.dst_objects.pop("ch_backup_copy/b1/metadata/a.tar")
    ctx.copy(["b1"], scopes=["cloud_storage"])
    assert _scopes(ctx.target_layout.backups["b1"]) == {
        "files": "pending",
        "cloud_storage": "completed",
    }
    uploaded = ctx.target_layout.cloud_storage_files.pop(tarball)
    size = ctx.dst_objects.pop(tarball)
    if replaced:
        ctx.dst_objects[tarball] = size - 1

    ctx.copy(["b1"], scopes=["files"])

    target = ctx.target_layout.backups["b1"]
    assert target.state == BackupState.CREATING
    assert _scopes(target) == {"files": "completed", "cloud_storage": "pending"}
    ctx.target_layout.copied.clear()

    ctx.copy(["b1"], scopes=["cloud_storage"])

    assert not ctx.target_layout.copied
    assert ctx.target_layout.cloud_storage_files[tarball] == uploaded
    assert ctx.dst_objects[tarball] == size
    assert ctx.target_layout.backups["b1"].state == BackupState.CREATED


def test_resume_skips_confirmed_rewritten_metadata() -> None:
    ctx = _scoped_context(version=5)
    tarball = "ch_backup_copy/b1/disks/s3/db1/t1.tar.gz"
    ctx.target_layout.object_errors["data/xyz/aaa"] = StorageError("SlowDown")
    with pytest.raises(ClickhouseBackupError):
        ctx.copy(["b1"], keep_going=True)
    assert tarball in ctx.target_layout.cloud_storage_files
    ctx.target_layout.object_errors.clear()
    ctx.target_layout.copied.clear()
    del ctx.target_layout.cloud_storage_files[tarball]
    report = CopyReport()

    ctx.copy(["b1"], scopes=["cloud_storage"], report=report)

    assert tarball not in ctx.target_layout.cloud_storage_files
    assert set(ctx.copied_paths().values()) == {"copies/b1/xyz/aaa"}
    assert report.backups[0].objects_skipped == 1
    assert ctx.target_layout.backups["b1"].state == BackupState.CREATED


def test_failed_cloud_storage_planning_keeps_rewritten_metadata() -> None:
    ctx = _scoped_context(version=5)
    tarball = "ch_backup_copy/b1/disks/s3/db1/t1.tar.gz"
    disks = ctx.ch_config.config
    ctx.copy(["b1"], scopes=["files"])
    ctx.dst_objects.pop("ch_backup_copy/b1/metadata/a.tar")
    ctx.copy(["b1"], scopes=["cloud_storage"])
    ctx.ch_config.config = {"storage_configuration": {"disks": {}}}
    ctx.target_layout.copied.clear()

    ctx.copy(["b1"], scopes=["files"])

    assert tarball not in ctx.copied_paths().values()
    assert _scopes(ctx.target_layout.backups["b1"]) == {
        "files": "completed",
        "cloud_storage": "pending",
    }
    ctx.ch_config.config = disks

    ctx.copy(["b1"], scopes=["cloud_storage"])

    assert ctx.target_layout.backups["b1"].state == BackupState.CREATED


def test_files_scope_does_not_depend_on_cloud_storage_planning() -> None:
    ctx = _scoped_context()
    disks = ctx.ch_config.config
    ctx.ch_config.config = {"storage_configuration": {"disks": {}}}
    ctx.copy(["b1"], scopes=["files"])
    ctx.ch_config.config = disks

    ctx.copy(["b1"], scopes=["cloud_storage"])

    assert ctx.target_layout.backups["b1"].state == BackupState.CREATED


def test_files_scope_needs_no_settings_of_object_storage() -> None:
    ctx = _scoped_context()
    target = ctx.target_config["copy"]["cloud_storage"]["target"]
    report = CopyReport()

    with patch.object(
        ctx.layout,
        "get_cloud_storage_metadata_files",
        wraps=ctx.layout.get_cloud_storage_metadata_files,
    ) as download:
        ctx.copy(["b1"], scopes=["files"])
        bucket, target["bucket"] = target["bucket"], None
        ctx.copy(["b1"], scopes=["files"], report=report)

    download.assert_not_called()
    assert report.backups[0].objects_skipped == 3
    assert _scopes(ctx.target_layout.backups["b1"]) == {
        "files": "completed",
        "cloud_storage": "pending",
    }
    target["bucket"] = bucket

    ctx.copy(["b1"], scopes=["cloud_storage"])

    assert ctx.target_layout.backups["b1"].state == BackupState.CREATED


class UnavailableLayout:
    """
    Layout of a source storage that must not be accessed.
    """

    backups: dict = {}

    def __getattr__(self, name: str) -> Any:
        raise AssertionError(f"Source storage is accessed: {name}")


@pytest.mark.parametrize("version", [3, 5])
def test_cloud_storage_scope_without_source_storage(version: int) -> None:
    ctx = _scoped_context(version=version)
    tarball = "ch_backup_copy/b1/disks/s3/db1/t1.tar.gz"
    ctx.copy(["b1"], scopes=["files"])
    ctx.layout = UnavailableLayout()  # type: ignore[assignment]
    ctx.target_layout.source = None
    engine = ctx.store.engine

    def target_only(storage: dict) -> FakeEngine:
        assert storage["credentials"]["bucket"] != SRC_BUCKET
        return engine(storage)

    ctx.store.engine = target_only  # type: ignore[method-assign]

    assert ctx.copy(["b1"], scopes=["cloud_storage"]) == (["b1"], [])

    target = ctx.target_layout.backups["b1"]
    assert target.state == BackupState.CREATED
    assert _scopes(target) == {"files": "completed", "cloud_storage": "completed"}
    if version == 5:
        assert _disk_objects(ctx) == {"copies/b1/xyz/aaa"}
        assert tarball in ctx.target_layout.cloud_storage_files
    else:
        assert _disk_objects(ctx) == {"copies/b1/xyz/aaa", "copies/b1/xyz/bbb"}
        assert tarball in ctx.copied_paths().values()


def test_cloud_storage_scope_requires_files_scope() -> None:
    ctx = _scoped_context()

    with pytest.raises(
        ClickhouseBackupError, match='scope "files" must be copied first'
    ):
        ctx.copy(["b1"], scopes=["cloud_storage"])

    assert not ctx.target_layout.uploaded


def test_cloud_storage_scope_selects_copies_in_target() -> None:
    ctx = _scoped_context()
    ctx.layout.backups["b2"] = _backup(
        "b2", start_time="2026-01-02 00:00:00 +0000", cloud_storage_disks=["s3"]
    )
    _with_files(ctx, "b2", {"metadata/a.tar": 1, "disks/s3/db1/t1.tar.gz": 50})
    ctx.layout.cloud_storage_files["ch_backup/b2/disks/s3/db1/t1.tar.gz"] = list(
        ctx.layout.cloud_storage_files["ch_backup/b1/disks/s3/db1/t1.tar.gz"]
    )
    ctx.copy(scopes=["files"])

    assert ctx.copy(["LAST"], scopes=["cloud_storage"]) == (["b2"], [])
    assert ctx.copy(scopes=["cloud_storage"]) == (["b1"], [])

    assert ctx.target_layout.backups["b1"].state == BackupState.CREATED
    assert ctx.target_layout.backups["b2"].state == BackupState.CREATED
    assert ctx.copy(scopes=["cloud_storage"]) == ([], [])


def test_files_scope_copies_backup_with_copied_cloud_storage_data() -> None:
    backup = _backup(
        "b1",
        parts={"all_1_1_0": {**_part(), "disk_name": "s3"}},
        cloud_storage_disks=["s3"],
    )
    backup.cloud_storage.data_copied = True
    ctx = CopyTestContext([backup])
    ctx.target_config["copy"]["server_side_copy"] = False
    ctx.layout.cloud_storage_files["ch_backup/b1/disks/s3/db1/t1.tar.gz"] = [
        (
            "store/abc/uuid/all_1_1_0/data.bin",
            _object_storage_metadata(("xyz/data", 100), version=3),
        )
    ]
    _with_files(
        ctx, "b1", {"disks/s3/db1/t1.tar.gz": 50, "cloud_storage/s3/xyz/data": 100}
    )

    assert ctx.copy(["b1"], scopes=["files"]) == (["b1"], [])

    target = ctx.target_layout.backups["b1"]
    assert target.state == BackupState.CREATED
    assert _scopes(target) == {"files": "completed", "cloud_storage": "completed"}
    assert ctx.copied_paths() == {
        "ch_backup/b1/cloud_storage/s3/xyz/data": "ch_backup_copy/b1/cloud_storage/s3/xyz/data"
    }


def test_disk_metadata_with_planning_errors_is_not_copied_as_is() -> None:
    ctx = _scoped_context()
    tarball = "ch_backup/b1/disks/s3/db1/t1.tar.gz"
    ctx.layout.cloud_storage_files[tarball].append(
        ("store/abc/uuid/all_1_1_0/broken.bin", b"garbage")
    )

    with pytest.raises(ClickhouseBackupError):
        ctx.copy(["b1"], keep_going=True)

    assert "ch_backup_copy/b1/disks/s3/db1/t1.tar.gz" not in ctx.copied_paths().values()
    assert _disk_objects(ctx) == {"copies/b1/xyz/aaa", "copies/b1/xyz/bbb"}


def test_interrupted_cloud_storage_scope_keeps_failed_objects() -> None:
    ctx = _scoped_context()
    ctx.target_layout.object_errors["data/xyz/bbb"] = StorageError("SlowDown")
    with pytest.raises(ClickhouseBackupError):
        ctx.copy(["b1"], keep_going=True)
    failed = _failed_objects(ctx)
    assert failed is not None

    with patch.object(
        ctx.target_layout, "copy_objects", side_effect=TerminatingSignal()
    ):
        with pytest.raises(TerminatingSignal):
            ctx.copy(["b1"], scopes=["cloud_storage"])

    assert _failed_objects(ctx) == failed
    assert _scopes(ctx.target_layout.backups["b1"]) == {
        "files": "completed",
        "cloud_storage": "failed",
    }


def test_incomplete_cloud_storage_planning_keeps_failed_objects() -> None:
    ctx = _scoped_context()
    ctx.target_layout.object_errors["data/xyz/bbb"] = StorageError("SlowDown")
    with pytest.raises(ClickhouseBackupError):
        ctx.copy(["b1"], keep_going=True)
    failed = _failed_objects(ctx)
    ctx.target_layout.object_errors.clear()
    ctx.target_layout.copied.clear()
    # pylint: disable-next=protected-access
    plan_objects = BackupCopier._plan_cloud_storage_objects

    def incomplete(self, backup, target_meta, plan, result):
        plan_objects(self, backup, target_meta, plan, result)
        result.add_error(StorageError("SlowDown"), "planning")

    with patch.object(BackupCopier, "_plan_cloud_storage_objects", incomplete):
        with pytest.raises(ClickhouseBackupError):
            ctx.copy(["b1"], scopes=["cloud_storage"], keep_going=True)

    assert set(ctx.copied_paths().values()) == {"copies/b1/xyz/bbb"}
    assert _failed_objects(ctx) == failed
    assert _scopes(ctx.target_layout.backups["b1"]) == {
        "files": "completed",
        "cloud_storage": "failed",
    }


def test_scope_without_resume_recopies_its_objects_and_checks_others() -> None:
    ctx = _scoped_context()
    ctx.target_layout.object_errors["data/xyz/bbb"] = StorageError("SlowDown")
    with pytest.raises(ClickhouseBackupError):
        ctx.copy(["b1"], keep_going=True)
    ctx.target_layout.object_errors.clear()
    ctx.target_layout.copied.clear()
    ctx.target_config["copy"]["resume"] = False
    report = CopyReport()

    ctx.copy(["b1"], scopes=["cloud_storage"], report=report)

    assert _disk_objects(ctx) == {"copies/b1/xyz/aaa", "copies/b1/xyz/bbb"}
    assert report.backups[0].objects_skipped == 0
    assert ctx.target_layout.backups["b1"].state == BackupState.CREATED


def test_repeated_scope_is_copied_and_counted_once() -> None:
    ctx = _scoped_context()
    report = CopyReport()

    ctx.copy(["b1"], scopes=["files", "files"], report=report)

    assert report.backups[0].objects_total == report.backups[0].objects_copied == 3


@pytest.mark.parametrize("keep_going", [False, True])
def test_cloud_storage_planning_failure_does_not_fail_backup_scope(
    keep_going: bool,
) -> None:
    ctx = _scoped_context()
    ctx.ch_config.config = {"storage_configuration": {"disks": {}}}

    assert ctx.copy(["b1"], scopes=["files"], keep_going=keep_going) == (["b1"], [])

    target = ctx.target_layout.backups["b1"]
    assert target.state == BackupState.CREATING
    assert _scopes(target) == {"files": "completed", "cloud_storage": "pending"}

    with pytest.raises((ConfigurationError, ClickhouseBackupError)):
        ctx.copy(["b1"], scopes=["cloud_storage"], keep_going=keep_going)

    target = ctx.target_layout.backups["b1"]
    assert target.state == (BackupState.FAILED if keep_going else BackupState.CREATING)
    assert _scopes(target) == {
        "files": "completed",
        "cloud_storage": "failed" if keep_going else "pending",
    }
