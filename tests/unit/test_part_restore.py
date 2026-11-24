"""Metadata compatibility and publication failure boundaries."""

from hashlib import md5
from os import rename
from pathlib import Path
from unittest.mock import Mock

import pytest

from ch_backup.backup.metadata import PartMetadata
from ch_backup.exceptions import ClickhouseBackupError
from ch_backup.logic.part_restore import (
    LOCAL_RESTORE_DIR,
    LocalPartRestorer,
    check_local_part,
)


def _part(name):
    return PartMetadata(
        "db",
        "t",
        name,
        md5(b"checksums").hexdigest(),
        512,
        ["checksums.txt"],
        True,
    )


@pytest.fixture(name="part")
def fixture_part():
    return _part("all_1_1_0")


@pytest.fixture(name="layout")
def fixture_layout():
    def download(_backup, part, path, callback, **_kwargs):
        staging = Path(path)
        staging.mkdir(exist_ok=True)
        (staging / "checksums.txt").write_bytes(b"checksums")
        callback(part)

    return Mock(download_data_part=Mock(side_effect=download))


def _restorer(layout):
    return LocalPartRestorer(
        layout, Mock(), keep_going=False, on_success=Mock(), on_error=Mock()
    )


@pytest.mark.parametrize(
    "change", ["aligned", "legacy", "missing", "extra", "size", "checksum"]
)
def test_download_metadata_validation(tmp_path, part, change):
    (tmp_path / "checksums.txt").write_bytes(b"checksums")
    if change == "legacy":
        part.raw_metadata.size = len(b"checksums")
    elif change == "missing":
        (tmp_path / "checksums.txt").unlink()
    elif change == "extra":
        (tmp_path / "extra").touch()
    elif change == "size":
        part.raw_metadata.size = 1
    elif change == "checksum":
        (tmp_path / "checksums.txt").write_bytes(b"different")
    if change in {"aligned", "legacy"}:
        check_local_part(part, str(tmp_path))
    else:
        with pytest.raises(ClickhouseBackupError, match="does not match backup"):
            check_local_part(part, str(tmp_path))


@pytest.mark.parametrize("failure", ["rename", "state", "wait", "checksum"])
@pytest.mark.parametrize("keep_going", [False, True])
def test_publication_failure_boundary(
    tmp_path, part, layout, failure, keep_going, *, monkeypatch
):
    success, error = Mock(), Mock()
    if failure == "wait":
        layout.wait.side_effect = RuntimeError("wait")
    if failure == "state":
        success.side_effect = RuntimeError("state")
    restorer = LocalPartRestorer(
        layout, Mock(), keep_going=keep_going, on_success=success, on_error=error
    )
    destination = tmp_path / part.name
    restorer.download(part, str(destination))
    staging = Path(layout.download_data_part.call_args.args[2])
    assert not destination.exists()
    if failure in {"rename", "checksum"}:
        destination.mkdir()
        (destination / "original").write_bytes(b"keep me")
    if failure == "checksum":
        (staging / "checksums.txt").write_bytes(b"corrupt")
    if failure == "rename":

        def fail_publication(src, dst):
            if Path(src) == staging and Path(dst) == destination:
                raise OSError("rename")
            rename(src, dst)

        monkeypatch.setattr("ch_backup.logic.part_restore.os.rename", fail_publication)
    if failure in {"rename", "checksum"} and keep_going:
        restorer.wait()
    else:
        with pytest.raises(
            (OSError, RuntimeError, ClickhouseBackupError), match=failure
        ):
            restorer.wait()
    assert destination.exists() == (failure != "wait")
    if failure in {"rename", "checksum"}:
        assert (destination / "original").read_bytes() == b"keep me"
    assert staging.exists() == (failure == "wait")
    assert success.called == (failure == "state")
    assert error.called == (failure in {"rename", "checksum"})


def test_interrupted_downloads_reuse_workspace(tmp_path, layout):
    parts = [_part("all_1_1_0"), _part("all_2_2_0")]
    root = tmp_path / LOCAL_RESTORE_DIR
    incoming = root / "incoming"
    expected = {incoming / part.name / "checksums.txt" for part in parts}
    for attempt in range(3):
        restorer = _restorer(layout)
        for part in parts:
            restorer.download(part, str(tmp_path / part.name))
        assert list(tmp_path.iterdir()) == [root]
        assert {path for path in root.rglob("*") if path.is_file()} == expected
        if attempt < 2:
            # Leave a partial file and abandon this invocation before publication.
            (incoming / parts[0].name / "partial").touch()

    restorer.wait()
    assert set(tmp_path.iterdir()) == {tmp_path / part.name for part in parts}
    for part in parts:
        assert (tmp_path / part.name / "checksums.txt").read_bytes() == b"checksums"


@pytest.mark.parametrize("published", [False, True])
@pytest.mark.parametrize("old_symlink", [False, True])
def test_recovery_of_interrupted_publication(tmp_path, part, published, old_symlink):
    root = tmp_path / LOCAL_RESTORE_DIR
    old = root / "previous" / part.name
    staging = root / "incoming" / part.name
    final = tmp_path / part.name
    old.parent.mkdir(parents=True)
    original = tmp_path / "original"
    original.mkdir()
    (original / "data").write_bytes(b"old")
    if old_symlink:
        old.symlink_to(original)
    else:
        original.rename(old)
    if published:
        final.mkdir()
        (final / "data").write_bytes(b"new")
    else:
        staging.mkdir(parents=True)

    _restorer(Mock()).prepare(tmp_path)
    assert not root.exists()
    assert final.is_symlink() == (old_symlink and not published)
    assert (final / "data").read_bytes() == (b"new" if published else b"old")
    if old_symlink:
        assert (original / "data").read_bytes() == b"old"


@pytest.mark.parametrize(
    "entry", [".", "incoming", "previous", "incoming/all_1_1_0", "unknown", "data.bin"]
)
def test_unexpected_workspace_entry_is_preserved(tmp_path, part, layout, entry):
    root = tmp_path / LOCAL_RESTORE_DIR
    final = tmp_path / part.name
    final.mkdir()
    marker = final / "original"
    marker.write_bytes(b"original")
    unexpected = root / entry
    unexpected.parent.mkdir(parents=True, exist_ok=True)
    if entry == "data.bin":
        unexpected.write_bytes(b"user data")
    elif entry == "unknown":
        unexpected.mkdir()
    else:
        unexpected.symlink_to(final)

    with pytest.raises(ClickhouseBackupError, match="Unexpected") as error:
        _restorer(layout).download(part, str(final))
    assert str(root) in str(error.value)
    assert unexpected.exists()
    if entry == "data.bin":
        assert unexpected.read_bytes() == b"user data"
    assert marker.read_bytes() == b"original"
    layout.download_data_part.assert_not_called()


@pytest.mark.parametrize("old_symlink", [False, True])
def test_failed_publication_and_rollback_preserve_conflict_on_retry(
    tmp_path, part, layout, monkeypatch, old_symlink
):
    root = tmp_path / LOCAL_RESTORE_DIR
    destination = tmp_path / part.name
    original = tmp_path / "original" if old_symlink else destination
    original.mkdir()
    (original / "data").write_bytes(b"original")
    if old_symlink:
        destination.symlink_to(original)
    restorer = _restorer(layout)
    restorer.download(part, str(destination))

    def conflicting_rename(source, target):
        if Path(source).parent.name == "incoming":
            destination.mkdir()
            (destination / "data").write_bytes(b"unexpected")
        rename(source, target)

    monkeypatch.setattr("ch_backup.logic.part_restore.os.rename", conflicting_rename)
    with pytest.raises(OSError):
        restorer.wait()

    with pytest.raises(ClickhouseBackupError, match="Conflicting restore paths"):
        _restorer(layout).prepare(tmp_path)
    assert (destination / "data").read_bytes() == b"unexpected"
    previous = root / "previous" / part.name
    assert previous.is_symlink() == old_symlink
    assert (previous / "data").read_bytes() == b"original"
    assert (
        root / "incoming" / part.name / "checksums.txt"
    ).read_bytes() == b"checksums"
