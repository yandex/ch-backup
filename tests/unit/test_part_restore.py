"""Metadata compatibility and publication failure boundaries."""

from hashlib import md5
from os import rename
from pathlib import Path
from unittest.mock import Mock

import pytest

from ch_backup.backup.metadata import PartMetadata
from ch_backup.backup.restore_workspace import LOCAL_RESTORE_DIR
from ch_backup.exceptions import ClickhouseBackupError
from ch_backup.logic.part_restore import LocalPartRestorer, check_local_part


@pytest.fixture(name="part")
def fixture_part():
    return PartMetadata(
        "db",
        "t",
        "all_1_1_0",
        md5(b"checksums").hexdigest(),
        512,
        ["checksums.txt"],
        True,
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
def test_publication_failure_boundary(tmp_path, part, failure, keep_going, monkeypatch):
    layout, success, error = Mock(), Mock(), Mock()
    paths = []

    def download(_backup, downloaded_part, path, callback, **_kwargs):
        Path(path).mkdir()
        (Path(path) / "checksums.txt").write_bytes(b"checksums")
        paths.append(Path(path))
        callback(downloaded_part)

    layout.download_data_part.side_effect = download
    if failure == "wait":
        layout.wait.side_effect = RuntimeError("wait")
    if failure == "state":
        success.side_effect = RuntimeError("state")
    restorer = LocalPartRestorer(
        layout, Mock(), keep_going=keep_going, on_success=success, on_error=error
    )
    destination = tmp_path / part.name
    restorer.download(part, str(destination))
    assert not destination.exists()
    if failure in {"rename", "checksum"}:
        destination.mkdir()
        (destination / "original").write_bytes(b"keep me")
    if failure == "checksum":
        (paths[0] / "checksums.txt").write_bytes(b"corrupt")
    if failure == "rename":

        def fail_publication(src, dst):
            if Path(src) == paths[0] and Path(dst) == destination:
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
    assert paths[0].exists() == (failure == "wait")
    assert success.called == (failure == "state")
    assert error.called == (failure in {"rename", "checksum"})


@pytest.mark.parametrize(
    "final_exists,incoming_exists", [(False, True), (True, False), (True, True)]
)
@pytest.mark.parametrize("old_symlink", [False, True])
def test_recovery_of_interrupted_publication(
    tmp_path, part, final_exists, incoming_exists, old_symlink
):
    root = tmp_path / ".ch-backup-restore"
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
    if incoming_exists:
        staging.mkdir(parents=True)
        (staging / "data").write_bytes(b"incoming")
    if final_exists:
        final.mkdir()
        (final / "data").write_bytes(b"new")
    restorer = LocalPartRestorer(
        Mock(), Mock(), keep_going=False, on_success=Mock(), on_error=Mock()
    )
    if final_exists and incoming_exists:
        with pytest.raises(ClickhouseBackupError, match="Conflicting restore paths"):
            restorer.prepare(tmp_path)
        assert (old / "data").read_bytes() == b"old"
        assert (staging / "data").read_bytes() == b"incoming"
    else:
        restorer.prepare(tmp_path)
        assert not root.exists()
        assert final.is_symlink() == (old_symlink and not final_exists)
    assert (final / "data").read_bytes() == (b"new" if final_exists else b"old")
    if old_symlink:
        assert (original / "data").read_bytes() == b"old"


@pytest.mark.parametrize(
    "entry", [".", "incoming", "previous", "incoming/all_1_1_0", "unknown"]
)
def test_unexpected_workspace_entry_is_preserved(tmp_path, entry):
    root = tmp_path / ".ch-backup-restore"
    external = tmp_path / "external"
    external.mkdir()
    (external / "marker").touch()
    unexpected = root / entry
    unexpected.parent.mkdir(parents=True, exist_ok=True)
    if entry == "unknown":
        unexpected.mkdir()
    else:
        unexpected.symlink_to(external)
    restorer = LocalPartRestorer(
        Mock(), Mock(), keep_going=False, on_success=Mock(), on_error=Mock()
    )
    with pytest.raises(ClickhouseBackupError, match="Unexpected"):
        restorer.prepare(tmp_path)
    assert unexpected.exists()
    assert (external / "marker").exists()


def test_restore_preserves_user_directory_named_like_workspace(tmp_path, part):
    root = tmp_path / LOCAL_RESTORE_DIR
    root.mkdir()
    (root / "checksums.txt").write_bytes(b"user checksums")
    (root / "data.bin").write_bytes(b"user data")
    final = tmp_path / part.name
    final.mkdir()
    (final / "data.bin").write_bytes(b"original data")
    layout = Mock()
    restorer = LocalPartRestorer(
        layout, Mock(), keep_going=False, on_success=Mock(), on_error=Mock()
    )

    with pytest.raises(ClickhouseBackupError) as error:
        restorer.download(part, str(final))

    assert str(root) in str(error.value)
    assert set(root.iterdir()) == {root / "checksums.txt", root / "data.bin"}
    assert (root / "checksums.txt").read_bytes() == b"user checksums"
    assert (root / "data.bin").read_bytes() == b"user data"
    assert (final / "data.bin").read_bytes() == b"original data"
    layout.download_data_part.assert_not_called()


def test_failed_publication_and_rollback_preserve_conflict_on_retry(
    tmp_path, part, monkeypatch
):
    root = tmp_path / ".ch-backup-restore"
    destination = tmp_path / part.name
    destination.mkdir()
    (destination / "data").write_bytes(b"original")
    layout = Mock()

    def download(_backup, downloaded_part, path, callback, **_kwargs):
        Path(path).mkdir()
        (Path(path) / "checksums.txt").write_bytes(b"checksums")
        callback(downloaded_part)

    layout.download_data_part.side_effect = download
    restorer = LocalPartRestorer(
        layout, Mock(), keep_going=False, on_success=Mock(), on_error=Mock()
    )
    restorer.download(part, str(destination))

    def conflicting_rename(source, target):
        if Path(source).parent.name == "incoming":
            destination.mkdir()
            (destination / "data").write_bytes(b"unexpected")
        rename(source, target)

    monkeypatch.setattr("ch_backup.logic.part_restore.os.rename", conflicting_rename)
    with pytest.raises(OSError):
        restorer.wait()

    retry = LocalPartRestorer(
        layout, Mock(), keep_going=False, on_success=Mock(), on_error=Mock()
    )
    with pytest.raises(ClickhouseBackupError, match="Conflicting restore paths"):
        retry.prepare(tmp_path)
    assert (destination / "data").read_bytes() == b"unexpected"
    assert (root / "previous" / part.name / "data").read_bytes() == b"original"
    assert (
        root / "incoming" / part.name / "checksums.txt"
    ).read_bytes() == b"checksums"
