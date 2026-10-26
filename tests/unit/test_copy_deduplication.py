"""Independent target deduplication in backup copy."""

import gc
import weakref
from concurrent.futures import BrokenExecutor
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest

from ch_backup.backup.copy_deduplication import CopyDeduplication
from ch_backup.backup.copy_report import CopyReport
from ch_backup.backup.layout import (
    get_data_part_file_names,
    get_data_part_relative_dirs,
)
from ch_backup.backup.metadata import BackupMetadata, BackupState
from ch_backup.ch_backup import ClickhouseBackup
from ch_backup.exceptions import ClickhouseBackupError, StorageError
from tests.unit.test_backup_copy import CopyTestContext, _backup, _part, _with_files

NOW = datetime(2026, 10, 22, tzinfo=timezone.utc)
PART = "all_1_1_0"


@pytest.fixture(autouse=True)
def fixed_time(monkeypatch):
    monkeypatch.setattr("ch_backup.backup.copier.utcnow", lambda: NOW)


def _files(backup, layout):
    # pylint: disable=protected-access
    files = {}
    for part in backup.get_parts():
        size = layout._target_part_size(part, part.encrypted) if part.tarball else 1
        for name in get_data_part_file_names(part):
            files[f"{get_data_part_relative_dirs(part)[0]}/{name}"] = size
    return files


@pytest.mark.parametrize("linked", [False, True])
def test_copy_finds_target_owner_independently_of_source(linked):
    owner = _backup(
        "target-owner", start_time="2026-10-20 00:00:00 +0000", parts={PART: _part()}
    )
    source = _backup(
        "new",
        start_time="2026-10-21 00:00:00 +0000",
        parts={PART: _part(link="source-owner" if linked else None)},
    )
    ctx = CopyTestContext([source], [owner])
    ctx.target_config["copy"]["deduplication_age_limit"] = {"days": 90}
    ctx.target_config["copy"]["server_side_copy"] = False
    for path, size in _files(owner, ctx.target_layout).items():
        ctx.dst_objects[f"ch_backup_copy/{owner.name}/{path}"] = size
    _with_files(
        ctx, "source-owner" if linked else source.name, _files(source, ctx.layout)
    )
    report = CopyReport()

    assert ctx.copy(["new"], report=report) == (["new"], [])

    target = ctx.target_layout.backups["new"]
    assert target.get_parts()[0].link == "target-owner"
    assert target.size == 100
    assert target.real_size == 0
    assert not ctx.target_layout.copied
    assert report.backups[0].objects_copied == 0


def _context(source_part=None, owners=None):
    source = _backup(
        "new",
        start_time="2026-10-21 00:00:00 +0000",
        parts={PART: source_part if source_part is not None else _part()},
    )
    if owners is None:
        owners = [
            _backup(
                "owner", start_time="2026-10-20 00:00:00 +0000", parts={PART: _part()}
            )
        ]
    ctx = CopyTestContext([source], owners)
    ctx.target_config["copy"]["deduplication_age_limit"] = {"days": 7}
    ctx.target_config["copy"]["server_side_copy"] = False
    for owner in owners:
        for path, size in _files(owner, ctx.target_layout).items():
            ctx.dst_objects[f"ch_backup_copy/{owner.name}/{path}"] = size
    part = source.get_parts()[0]
    _with_files(ctx, part.link or source.name, _files(source, ctx.layout))
    return ctx


@pytest.mark.parametrize(
    "age, linked, expected",
    [(0, False, None), (0, True, "owner"), (7, False, "owner"), (7, True, "owner")],
)
def test_window_is_opt_in_and_independent(age, linked, expected):
    ctx = _context(_part(link="owner" if linked else None))
    ctx.config["backup"]["deduplicate_parts"] = False
    ctx.config["backup"]["deduplication_start_time"] = NOW
    ctx.config["backup"]["deduplication_age_limit"] = {"days": 0}
    ctx.target_config["copy"]["deduplication_age_limit"] = {"days": age}
    ctx.copy()
    assert ctx.target_layout.backups["new"].get_parts()[0].link == expected


@pytest.mark.parametrize(
    "date, expected",
    [
        ("2026-10-15 00:00:00 +0000", "owner"),
        ("2026-10-14 23:59:59 +0000", None),
        ("2026-10-22 00:00:00 +0000", "owner"),
        ("2026-10-22 00:00:01 +0000", None),
    ],
)
@pytest.mark.parametrize("linked", [False, True])
def test_physical_owner_must_be_in_window(date, expected, linked):
    owner = _backup("owner", start_time=date, parts={PART: _part()})
    recent = _backup(
        "recent",
        start_time="2026-10-21 00:00:00 +0000",
        parts={PART: _part(link="owner")},
    )
    ctx = _context(_part(link="owner" if linked else None), [owner, recent])
    ctx.copy()
    target = ctx.target_layout.backups["new"]
    assert target.get_parts()[0].link == expected
    assert target.real_size == (0 if expected else 100)
    assert len(ctx.target_layout.copied) == (0 if expected else 1)


@pytest.mark.parametrize(
    "field,value",
    [
        ("checksum", "different"),
        ("disk_name", "other"),
        ("bytes", 101),
        ("tarball", False),
        ("encrypted", False),
        ("files", ["other.bin"]),
    ],
)
def test_incompatible_part_is_copied(field, value):
    ctx = _context({**_part(), field: value})
    ctx.copy()
    assert ctx.target_layout.backups["new"].get_parts()[0].link is None
    assert ctx.target_layout.copied


@pytest.mark.parametrize(
    "files,expected_owner", [(["c", "ab"], "owner"), (["a", "bc"], None)]
)
def test_file_list_order_and_name_boundaries(files, expected_owner):
    owner = _backup(
        "owner",
        start_time="2026-10-20 00:00:00 +0000",
        parts={PART: {**_part(), "files": ["ab", "c"]}},
    )
    ctx = _context({**_part(), "files": files}, [owner])
    ctx.copy()
    assert ctx.target_layout.backups["new"].get_parts()[0].link == expected_owner
    assert bool(ctx.target_layout.copied) == (expected_owner is None)


def test_index_releases_file_names_and_duplicate_owner_keys():
    class TrackedString(str):
        pass

    ctx = _context(owners=[])
    index = CopyDeduplication(ctx.target_layout, NOW - timedelta(days=7), NOW)
    file_refs = []
    checksum_refs = []
    for name in ("first", "second"):
        checksum = TrackedString("shared checksum")
        filename = TrackedString("data.bin")
        owner = _backup(
            name,
            start_time="2026-10-20 00:00:00 +0000",
            parts={
                PART: {
                    **_part(checksum=checksum),
                    "files": [filename, "checksums.txt"],
                }
            },
        )
        file_refs.append(weakref.ref(filename))
        checksum_refs.append(weakref.ref(checksum))
        index.add_backup(owner, published=True)
        del owner, checksum, filename
    gc.collect()

    assert all(reference() is None for reference in file_refs)
    # Keep the canonical key, but release the duplicate key of a CREATED owner.
    assert checksum_refs[0]() is not None
    assert checksum_refs[1]() is None
    part = _backup(
        "source", parts={PART: _part(checksum="shared checksum")}
    ).get_parts()[0]
    match = index.find_part(part, "new")
    assert match is not None
    assert match.backup_name == "second"


@pytest.mark.parametrize("state", list(BackupState))
def test_only_completed_physical_owners_are_reused(state):
    ctx = _context()
    ctx.target_layout.backups["owner"].state = state
    ctx.copy()
    expected = (
        "owner"
        if state in (BackupState.CREATED, BackupState.PARTIALLY_DELETED)
        else None
    )
    assert ctx.target_layout.backups["new"].get_parts()[0].link == expected


@pytest.mark.parametrize("renamed", [False, True])
def test_newest_owner_legacy_paths(renamed):
    stored_name = "all_1_1_0_2" if renamed else PART
    owners = [
        _backup(
            name,
            start_time=date,
            database="db-1",
            parts={stored_name: _part()},
        )
        for name, date in [
            ("older", "2026-10-19 00:00:00 +0000"),
            ("a", "2026-10-20 00:00:00 +0000"),
            ("z", "2026-10-20 00:00:00 +0000"),
        ]
    ]
    source = _backup(
        "new",
        start_time="2026-10-21 00:00:00 +0000",
        database="db-1",
        parts={PART: _part()},
    )
    ctx = _context(owners=owners)
    ctx.layout.backups = {"new": source}
    ctx.src_objects.clear()
    # Old backups used unescaped database names. Use the real layout reader to find them.
    ctx.dst_objects.clear()
    for owner in owners:
        for path, size in _files(owner, ctx.target_layout).items():
            ctx.dst_objects[
                f"ch_backup_copy/{owner.name}/{path.replace('db%2D1', 'db-1')}"
            ] = size
    _with_files(ctx, "new", _files(source, ctx.layout))
    ctx.copy()
    target = ctx.target_layout.backups["new"]
    part = target.get_parts()[0]
    assert part.link == "z"
    assert part.link_part_name == (stored_name if renamed else None)
    assert target.real_size == 0
    assert not any("/data/" in e.dst_path for e in ctx.target_layout.copied)
    serialized = target.dump()["databases"]["db-1"]["tables"]["t1"]["parts"][PART]
    assert serialized["link"] == "ch_backup_copy/z"


@pytest.mark.parametrize("encrypted", [False, True])
@pytest.mark.parametrize("linked", [False, True])
def test_per_file_owner_is_materialized_without_trusting_stored_file_sizes(
    encrypted, linked
):
    part = {**_part(), "tarball": False, "encrypted": encrypted}
    owner = _backup("owner", start_time="2026-10-20 00:00:00 +0000", parts={PART: part})
    ctx = _context({**part, "link": "source-owner" if linked else None}, [owner])
    # Both names exist, but the target files are truncated. Legacy metadata has
    # no expected per-file sizes, so those owners cannot be verified by listing.
    for path in ctx.src_objects:
        ctx.src_objects[path] = 80 if path.endswith("data.bin") else 20
    ctx.copy()
    target = ctx.target_layout.backups["new"]
    assert target.get_parts()[0].link is None
    assert target.real_size == 100
    assert sorted(entry.size for entry in ctx.target_layout.copied) == [20, 80]


@pytest.mark.parametrize("keep_going", [False, True])
def test_owner_without_start_time_is_skipped(keep_going):
    ctx = _context()
    raw = ctx.target_layout.backups["owner"].dump()
    del raw["meta"]["start_time"]
    ctx.target_layout.backups["owner"] = BackupMetadata.load(raw)
    report = CopyReport()
    assert ctx.copy(keep_going=keep_going, report=report) == (["new"], [])
    assert ctx.target_layout.backups["new"].get_parts()[0].link is None
    assert [(result.name, result.status) for result in report.backups] == [
        ("new", "copied")
    ]


def test_copy_window_is_fixed_before_waiting_for_storage_locks(monkeypatch):
    owner = _backup(
        "owner", start_time="2026-10-15 00:00:10 +0000", parts={PART: _part()}
    )
    ctx = _context(owners=[owner])
    clock = [NOW]
    monkeypatch.setattr("ch_backup.ch_backup.utcnow", lambda: clock[0])
    monkeypatch.setattr("ch_backup.backup.copier.utcnow", lambda: clock[0])

    @contextmanager
    def delayed_lock(*_args, **_kwargs):
        clock[0] += timedelta(seconds=30)
        yield

    backup = ClickhouseBackup(ctx.config)
    backup.__dict__["_context"] = MagicMock(
        backup_layout=ctx.layout, ch_config=ctx.ch_config
    )
    with (
        patch("ch_backup.ch_backup.storage_locks", side_effect=delayed_lock),
        patch("ch_backup.ch_backup.BackupLayout", return_value=ctx.target_layout),
        patch(
            "ch_backup.backup.copier.get_storage_engine", side_effect=ctx.store.engine
        ),
    ):
        backup.copy(ctx.target_config, ["new"])
    assert ctx.target_layout.backups["new"].get_parts()[0].link == "owner"
    assert not ctx.target_layout.copied


@pytest.mark.parametrize("broken", ["missing", "size", "metadata"])
def test_bad_candidate_falls_back_to_older_owner(broken):
    owners = [
        _backup(name, start_time=f"2026-10-{day} 00:00:00 +0000", parts={PART: _part()})
        for name, day in [("older", 19), ("newer", 20)]
    ]
    ctx = _context(owners=owners)
    for key in list(ctx.dst_objects):
        if "/newer/" in key:
            if broken == "missing":
                del ctx.dst_objects[key]
            elif broken == "size":
                ctx.dst_objects[key] += 1
    original = ctx.target_layout.get_backup

    def get_backup(name, use_light_meta=False):
        if broken == "metadata" and name == "newer" and not use_light_meta:
            raise StorageError("unreadable")
        return original(name, use_light_meta)

    with patch.object(ctx.target_layout, "get_backup", side_effect=get_backup):
        ctx.copy()
    assert ctx.target_layout.backups["new"].get_parts()[0].link == "older"


@pytest.mark.parametrize("publication_fails", [False, True])
@pytest.mark.parametrize("tarball", [False, True])
def test_copy_all_indexes_only_published_owners(publication_fails, tarball):
    backups = [
        _backup(
            name,
            start_time=f"2026-10-{day} 00:00:00 +0000",
            parts={PART: {**_part(), "tarball": tarball}},
        )
        for name, day in [("a", 19), ("b", 20), ("c", 21)]
    ]
    ctx = _context(owners=[])
    ctx.layout.backups = {b.name: b for b in backups}
    for backup in backups:
        _with_files(ctx, backup.name, _files(backup, ctx.layout))
    original = ctx.target_layout.upload_backup_metadata

    def upload(backup):
        if (
            publication_fails
            and backup.name == "a"
            and backup.state == BackupState.CREATED
        ):
            raise RuntimeError("publication failed")
        original(backup)

    with patch.object(ctx.target_layout, "upload_backup_metadata", side_effect=upload):
        if publication_fails:
            with pytest.raises(ClickhouseBackupError):
                ctx.copy(keep_going=True)
        else:
            ctx.copy()
    expected_owner = ("b" if publication_fails else "a") if tarball else None
    assert ctx.target_layout.backups["c"].get_parts()[0].link == expected_owner
    assert ctx.target_layout.backups["c"].real_size == (0 if tarball else 100)
    expected_copies = (2 if publication_fails else 1) if tarball else 6
    assert len(ctx.target_layout.copied) == expected_copies


def test_retry_does_not_link_to_itself():
    owner = _backup(
        "new",
        start_time="2026-10-21 00:00:00 +0000",
        state=BackupState.PARTIALLY_DELETED,
        parts={PART: _part()},
    )
    ctx = _context(owners=[owner])
    ctx.copy()
    assert ctx.target_layout.backups["new"].get_parts()[0].link is None
    assert ctx.target_layout.copied


def test_reads_target_metadata_and_table_once_for_copy_all():
    parts = {PART: _part(), "all_2_2_0": _part()}
    owner = _backup("owner", start_time="2026-10-20 00:00:00 +0000", parts=parts)
    old = _backup("old", start_time="2025-10-20 00:00:00 +0000", parts=parts)
    ctx = _context(owners=[owner, old])
    ctx.layout.backups = {
        name: _backup(name, start_time="2026-10-21 00:00:00 +0000", parts=parts)
        for name in ("b", "c")
    }
    with (
        patch.object(
            ctx.target_layout, "get_backup", wraps=ctx.target_layout.get_backup
        ) as read,
        patch.object(
            ctx.target_layout,
            "get_table_data_files",
            wraps=ctx.target_layout.get_table_data_files,
        ) as listing,
    ):
        ctx.copy()
    full_reads = [
        call.args[0]
        for call in read.call_args_list
        if not call.kwargs.get("use_light_meta", False)
    ]
    assert full_reads == ["owner"]
    listing.assert_called_once_with("owner", "db1", "t1")
    assert not ctx.target_layout.copied
    assert all(
        part.link == "owner" for part in ctx.target_layout.backups["c"].get_parts()
    )


@pytest.mark.parametrize("keep_going", [False, True])
def test_broken_executor_in_candidate_read_is_fatal(keep_going):
    ctx = _context()
    original = ctx.target_layout.get_backup

    def read(name, use_light_meta=False):
        if not use_light_meta:
            raise BrokenExecutor("broken")
        return original(name, use_light_meta)

    with patch.object(ctx.target_layout, "get_backup", side_effect=read):
        with pytest.raises(BrokenExecutor):
            ctx.copy(keep_going=keep_going)
    assert not ctx.target_layout.uploaded


def test_missing_target_data_is_materialized_and_retry_reuses_success():
    ctx = _context(_part(link="source-owner"))
    ctx.dst_objects.clear()
    ctx.copy(["new"])
    target = ctx.target_layout.backups["new"]
    assert target.get_parts()[0].link is None
    assert target.real_size == 100
    for entry in ctx.target_layout.copied:
        ctx.dst_objects[entry.dst_path] = entry.size
    ctx.target_layout.copied.clear()
    ctx.layout.backups["next"] = _backup(
        "next", start_time="2026-10-22 00:00:00 +0000", parts={PART: _part()}
    )
    ctx.copy(["next"])
    assert ctx.target_layout.backups["next"].get_parts()[0].link == "new"
    assert not ctx.target_layout.copied


@pytest.mark.parametrize("failure", ["copy", "metadata"])
def test_failed_copy_is_not_an_owner_on_retry(failure):
    ctx = _context(owners=[])
    original = ctx.target_layout.upload_backup_metadata

    def upload(backup):
        if failure == "metadata" and backup.state == BackupState.CREATED:
            raise RuntimeError("failed publication")
        original(backup)

    if failure == "copy":
        ctx.target_layout.copy_error = RuntimeError("failed copy")
    with patch.object(ctx.target_layout, "upload_backup_metadata", side_effect=upload):
        with pytest.raises(RuntimeError):
            ctx.copy(["new"])
    assert ctx.target_layout.backups["new"].state == BackupState.FAILED
    ctx.target_layout.copy_error = None
    ctx.target_layout.copied.clear()
    ctx.copy(["new"])
    assert ctx.target_layout.backups["new"].state == BackupState.CREATED
    assert ctx.target_layout.backups["new"].get_parts()[0].link is None
    assert ctx.target_layout.copied


def test_failed_restart_invalidates_partially_deleted_owner():
    owner = _backup(
        "a",
        start_time="2026-10-20 00:00:00 +0000",
        state=BackupState.PARTIALLY_DELETED,
        parts={PART: _part()},
    )
    ctx = _context(owners=[owner])
    source = _backup("a", start_time="2026-10-20 00:00:00 +0000", parts={PART: _part()})
    ctx.layout.backups["a"] = source
    _with_files(ctx, "a", _files(source, ctx.layout))
    ctx.target_layout.object_errors[next(e for e in ctx.src_objects if "/a/" in e)] = (
        RuntimeError("owner failed")
    )
    with pytest.raises(ClickhouseBackupError):
        ctx.copy(keep_going=True)
    assert ctx.target_layout.backups["a"].state == BackupState.FAILED
    target = ctx.target_layout.backups["new"]
    assert target.state == BackupState.CREATED
    assert target.get_parts()[0].link is None
    assert target.real_size == 100


def test_malformed_candidate_parts_are_skipped_atomically():
    ctx = _context()
    raw = (
        ctx.target_layout.backups["owner"].get_table("db1", "t1").raw_metadata["parts"]
    )
    # The first part is valid, but a later broken part makes the candidate unreadable.
    raw["all_2_2_0"] = {"files": []}
    with patch.object(
        ctx.target_layout,
        "get_table_data_files",
        wraps=ctx.target_layout.get_table_data_files,
    ) as listing:
        ctx.copy()
    listing.assert_not_called()
    assert ctx.target_layout.backups["new"].get_parts()[0].link is None
    assert ctx.target_layout.copied


@pytest.mark.parametrize("keep_going", [False, True])
def test_candidate_listing_failure_stops_copy(keep_going):
    ctx = _context()
    with patch.object(
        ctx.target_layout,
        "get_table_data_files",
        side_effect=RuntimeError("listing failed"),
    ):
        with pytest.raises(RuntimeError, match="listing failed"):
            ctx.copy(keep_going=keep_going)
    assert not ctx.target_layout.uploaded
