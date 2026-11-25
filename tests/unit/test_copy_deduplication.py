"""Independent target deduplication in backup copy."""

import weakref
from concurrent.futures import BrokenExecutor, ThreadPoolExecutor
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from threading import Barrier, Event, get_ident
from unittest.mock import MagicMock, patch

import pytest

from ch_backup.backup import copy_deduplication
from ch_backup.backup.copy_deduplication import CopyDeduplication, _part_key
from ch_backup.backup.layout import (
    get_data_part_file_names,
    get_data_part_relative_dirs,
)
from ch_backup.backup.metadata import (
    BackupState,
    CopyScope,
    CopyScopeStatus,
    PartMetadata,
)
from ch_backup.ch_backup import ClickhouseBackup
from ch_backup.command_context import command_context, get_current_command
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


def _context(source_part=None, owners=None):
    source = _backup("new", parts={PART: source_part or _part()})
    source.start_time = source.end_time = NOW - timedelta(days=1)
    if owners is None:
        owners = [_backup("owner", parts={PART: _part()})]
        owners[0].start_time = owners[0].end_time = source.start_time
    ctx = CopyTestContext([source], owners)
    ctx.target_config["copy"]["deduplication_age_limit"] = {"days": 7}
    ctx.target_config["copy"]["server_side_copy_mode"] = "disabled"
    for owner in owners:
        for path, size in _files(owner, ctx.target_layout).items():
            ctx.dst_objects[f"ch_backup_copy/{owner.name}/{path}"] = size
    part = source.get_parts()[0]
    _with_files(ctx, part.link or source.name, _files(source, ctx.layout))
    return ctx


@pytest.mark.parametrize("workers", [1, 2, 3])
def test_full_metadata_prefetch_is_bounded_and_indexed_in_order(workers, monkeypatch):
    owners = [_backup(f"owner{i}", parts={PART: _part()}) for i in range(workers * 2)]
    for owner in owners:
        owner.start_time = NOW - timedelta(days=1)
    ctx = _context(owners=owners)
    index = CopyDeduplication(ctx.target_layout, NOW - timedelta(days=7), NOW)
    barrier = Barrier(workers)
    refill_started = Event()
    main_thread = get_ident()
    references, indexed = [], []
    get_backup = ctx.target_layout.get_backup
    index_parts = index._index_parts  # pylint: disable=protected-access

    def read(name, use_light_meta=False):
        assert not use_light_meta
        assert get_current_command() == "copy"
        backup = get_backup(name)
        references.append(weakref.ref(backup))
        if name == f"owner{workers}":
            refill_started.set()
        barrier.wait(timeout=10)
        return backup

    def add(backup, *args, **kwargs):
        assert get_ident() == main_thread
        indexed.append(backup.name)
        if workers > 1 and len(indexed) == 2:
            assert refill_started.wait(10)
        assert sum(ref() is not None for ref in references) <= workers
        index_parts(backup, *args, **kwargs)

    # A Mock would retain all metadata objects in call_args_list.
    monkeypatch.setattr(index, "_index_parts", add)
    with (
        command_context("copy"),
        patch.object(ctx.target_layout, "get_backup", side_effect=read),
    ):
        index.load(owners, workers=workers)

    assert indexed == [owner.name for owner in owners]
    assert all(ref() is None for ref in references)
    stored = index.find_part(owners[0].get_parts()[0], "new")
    assert stored is not None and stored.backup_name == owners[-1].name


@pytest.mark.parametrize("stage", ["read", "parse", "executor", "index"])
def test_full_metadata_prefetch_preserves_error_policy_and_drains_reads(stage):
    owners = [_backup(name, parts={PART: _part()}) for name in ("broken", "good")]
    for owner in owners:
        owner.start_time = NOW - timedelta(days=1)
    ctx = _context(owners=owners)
    index = CopyDeduplication(ctx.target_layout, NOW - timedelta(days=7), NOW)
    failure = StorageError("candidate failure")
    if stage == "executor":
        failure.__cause__ = BrokenExecutor("unavailable")
    barrier = Barrier(2)
    events = {
        name: Event() for name in ("good_started", "release_good", "error_seen", "done")
    }
    finished, indexed = [], []
    get_backup = ctx.target_layout.get_backup
    local_parts = copy_deduplication._local_parts  # pylint: disable=protected-access

    def read(name, **_kwargs):
        try:
            barrier.wait(timeout=10)
            if name == "good":
                events["good_started"].set()
                assert events["release_good"].wait(10)
            if name == "broken" and stage in ("read", "executor"):
                events["error_seen"].set()
                raise failure
            return get_backup(name)
        finally:
            finished.append(name)

    def parse(backup):
        if backup.name == "broken" and stage == "parse":
            events["error_seen"].set()
            raise failure
        return local_parts(backup)

    def add(backup, *_args, **_kwargs):
        if backup.name == "broken" and stage == "index":
            events["error_seen"].set()
            raise failure
        indexed.append(backup.name)

    with (
        patch.object(ctx.target_layout, "get_backup", side_effect=read),
        patch.object(copy_deduplication, "_local_parts", side_effect=parse),
        patch.object(index, "_index_parts", side_effect=add),
    ):
        with ThreadPoolExecutor(max_workers=1) as consumer:
            loading = consumer.submit(index.load, owners, workers=2)
            loading.add_done_callback(lambda _: events["done"].set())
            try:
                assert events["good_started"].wait(10)
                assert events["error_seen"].wait(10)
                assert not events["done"].wait(0.05)
            finally:
                events["release_good"].set()
            if stage in ("executor", "index"):
                with pytest.raises(StorageError) as raised:
                    loading.result(timeout=10)
                assert raised.value is failure
            else:
                loading.result(timeout=10)

    assert sorted(finished) == ["broken", "good"]
    assert indexed == (["good"] if stage in ("read", "parse") else [])


@pytest.mark.parametrize(
    "date, expected",
    [
        ("2026-10-15 00:00:00 +0000", "owner"),
        ("2026-10-14 23:59:59 +0000", None),
        ("2026-10-22 00:00:00 +0000", "owner"),
        ("2026-10-22 00:00:01 +0000", None),
        (None, None),
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


@pytest.mark.parametrize("linked", [False, True])
def test_per_file_owner_is_materialized(linked):
    part = {**_part(), "tarball": False}
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


def test_copy_all_indexes_only_published_owners():
    backups = [
        _backup(
            name,
            start_time=f"2026-10-{day} 00:00:00 +0000",
            parts={PART: _part()},
        )
        for name, day in [("a", 19), ("b", 20), ("c", 21)]
    ]
    ctx = _context(owners=[])
    ctx.layout.backups = {b.name: b for b in backups}
    for backup in backups:
        _with_files(ctx, backup.name, _files(backup, ctx.layout))
    original = ctx.target_layout.upload_backup_metadata

    def upload(backup):
        if backup.name == "a" and backup.state == BackupState.CREATED:
            raise RuntimeError("publication failed")
        original(backup)

    with patch.object(ctx.target_layout, "upload_backup_metadata", side_effect=upload):
        with pytest.raises(ClickhouseBackupError):
            ctx.copy(keep_going=True)
    assert ctx.target_layout.backups["c"].get_parts()[0].link == "b"
    assert ctx.target_layout.backups["c"].real_size == 0
    assert len(ctx.target_layout.copied) == 2


@pytest.mark.parametrize(
    "state", [BackupState.PARTIALLY_DELETED, BackupState.CREATING, BackupState.FAILED]
)
def test_failed_restart_invalidates_owner(state: BackupState) -> None:
    """Remove an owner's indexed parts before a retry that fails."""
    owner = _backup(
        "a",
        start_time="2026-10-20 00:00:00 +0000",
        state=state,
        parts={PART: _part()},
    )
    owner.copy_scopes = {CopyScope.FILES: CopyScopeStatus.COMPLETED}
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


def test_truncated_target_part_is_materialized():
    ctx = _context()
    for path in ctx.dst_objects:
        ctx.dst_objects[path] = 1
    ctx.copy()
    assert ctx.target_layout.backups["new"].get_parts()[0].link is None
    assert len(ctx.target_layout.copied) == 1


@pytest.mark.parametrize("separate", [False, True])
@pytest.mark.parametrize("linked", [False, True])
def test_files_scope_reuses_incomplete_owner(separate: bool, linked: bool) -> None:
    """Reuse completed local files within one command and across separate runs."""
    ctx = _context(owners=[])
    ctx.target_layout.store = ctx.store
    backups = [
        _backup(
            "a",
            start_time="2026-10-19 00:00:00 +0000",
            parts={PART: _part()},
            cloud_storage_disks=["s3"],
        ),
        _backup(
            "b",
            start_time="2026-10-20 00:00:00 +0000",
            parts={PART: _part(link="a" if linked else None)},
            cloud_storage_disks=["s3"],
        ),
    ]
    ctx.layout.backups = {backup.name: backup for backup in backups}
    for backup in backups:
        if backup.get_parts()[0].link is None:
            _with_files(ctx, backup.name, _files(backup, ctx.layout))

    if separate:
        ctx.copy(["a"], scopes=["files"])
        ctx.copy(["b"], scopes=["files"])
    else:
        ctx.copy(scopes=["files"])

    owner = ctx.target_layout.backups["a"]
    target = ctx.target_layout.backups["b"]
    assert owner.state == target.state == BackupState.CREATING
    assert target.get_parts()[0].link == "a"
    assert target.real_size == 0
    assert (
        sum(entry.dst_path.endswith(".tar") for entry in ctx.target_layout.copied) == 1
    )


@pytest.mark.parametrize(
    "state", [BackupState.CREATING, BackupState.FAILED, BackupState.DELETING]
)
@pytest.mark.parametrize("status", ["pending", "failed", "completed"])
def test_incomplete_owner_requires_completed_files(
    state: BackupState, status: str
) -> None:
    """Accept only completed files of creating or failed copies with verified data."""
    ctx = _context()
    owner = ctx.target_layout.backups["owner"]
    owner.state = state
    owner.copy_scopes = {CopyScope.FILES: CopyScopeStatus(status)}

    ctx.copy()

    expected = (
        "owner" if state != BackupState.DELETING and status == "completed" else None
    )
    assert ctx.target_layout.backups["new"].get_parts()[0].link == expected


@pytest.mark.parametrize(
    "change,equal",
    [
        ({"name": "all_1_1_0_7"}, True),
        ({"files": ["b", "a"]}, True),
        ({"link": "other", "link_part_name": "all_1_1_0_3"}, True),
        ({"database": "other"}, False),
        ({"table": "other"}, False),
        ({"name": "all_1_2_0"}, False),
        ({"name": "all_1_1_1"}, False),
        ({"size": 101}, False),
        ({"files": ["a", "b", "b"]}, False),
    ],
)
def test_local_reuse_ignores_only_mutation_order_and_source_owner(change, equal):
    """
    The same local payload can have a different mutation suffix, file-list
    order or source owner. The key permits those changes while separating
    different addresses, ranges, sizes and file multiplicities.
    """
    fields: dict = dict(
        database="db",
        table="table",
        name="all_1_1_0",
        checksum="c",
        size=100,
        files=["a", "b"],
        tarball=True,
    )
    assert (
        _part_key(PartMetadata(**fields))
        == _part_key(PartMetadata(**{**fields, **change}))
    ) is equal


@pytest.mark.parametrize(
    "left,right,equal",
    [(["a", "bc"], ["ab", "c"], False), (["a", "bc"], ["bc", "a"], True)],
)
def test_file_list_boundaries_do_not_alias(left, right, equal):
    """
    Concatenating filenames would confuse [a, bc] with [ab, c]. Preserve file
    boundaries while allowing order changes; unparsable part names can match
    only when their names are identical.
    """
    first = PartMetadata("db", "t", "broken", "c", 1, left, True)
    second = PartMetadata("db", "t", "broken", "c", 1, right, True)
    assert (_part_key(first) == _part_key(second)) is equal
    second.name = "other"
    assert _part_key(first) != _part_key(second)
