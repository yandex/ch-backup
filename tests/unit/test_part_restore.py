"""Metadata compatibility and publication failure boundaries."""

from hashlib import md5
from pathlib import Path
from unittest.mock import Mock

import pytest

from ch_backup.backup.metadata import PartMetadata
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
    if failure == "state":
        destination.symlink_to(paths[0])
    if failure == "checksum":
        (paths[0] / "checksums.txt").write_bytes(b"corrupt")
    if failure == "rename":
        monkeypatch.setattr("os.rename", Mock(side_effect=OSError("rename")))
    if failure in {"rename", "checksum"} and keep_going:
        restorer.wait()
    else:
        with pytest.raises(
            (OSError, RuntimeError, ClickhouseBackupError), match=failure
        ):
            restorer.wait()
    assert destination.exists() == (failure in {"checksum", "state"})
    if failure == "checksum":
        assert (destination / "original").read_bytes() == b"keep me"
    if failure == "state":
        assert (destination / "checksums.txt").read_bytes() == b"checksums"
    assert paths[0].exists() == (failure == "wait")
    assert success.called == (failure == "state")
    assert error.called == (failure in {"rename", "checksum"})


@pytest.mark.parametrize(
    "entry", ["", ".ch-backup-restore", ".ch-backup-restore/all_1_1_0"]
)
def test_prepare_preserves_symlinks(tmp_path, entry):
    detached = tmp_path / "detached"
    part = detached / ".ch-backup-restore" / "all_1_1_0"
    part.mkdir(parents=True)
    link = detached / entry
    target = link.rename(tmp_path / "target")
    link.symlink_to(target)
    with pytest.raises(ClickhouseBackupError, match="Unexpected"):
        LocalPartRestorer(
            Mock(), Mock(), keep_going=False, on_success=Mock(), on_error=Mock()
        ).prepare(detached)
    assert link.is_symlink() and part.is_dir()
