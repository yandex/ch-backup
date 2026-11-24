"""Metadata compatibility and publication failure boundaries."""

from hashlib import md5
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


@pytest.fixture(name="layout")
def fixture_layout():
    def download(_backup, part, path, callback, **_kwargs):
        staging = Path(path)
        staging.mkdir()
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
    destination.mkdir()
    original = destination / "original"
    original.write_bytes(b"keep me")
    restorer.download(part, str(destination))
    staging = Path(layout.download_data_part.call_args.args[2])
    assert original.read_bytes() == b"keep me"
    if failure == "checksum":
        (staging / "checksums.txt").write_bytes(b"corrupt")
    if failure == "rename":
        monkeypatch.setattr(
            "ch_backup.logic.part_restore.os.rename",
            Mock(side_effect=OSError("rename")),
        )
    if failure in {"rename", "checksum"} and keep_going:
        restorer.wait()
    else:
        with pytest.raises(
            (OSError, RuntimeError, ClickhouseBackupError), match=failure
        ):
            restorer.wait()
    if failure in {"wait", "checksum"}:
        assert original.read_bytes() == b"keep me"
    elif failure == "rename":
        assert not destination.exists()
    else:
        assert (destination / "checksums.txt").read_bytes() == b"checksums"
    assert staging.exists() == (failure == "wait")
    assert success.called == (failure == "state")
    assert error.called == (failure in {"rename", "checksum"})


def test_retry_replaces_detached_symlink_and_clears_staging(tmp_path, part, layout):
    target = tmp_path / "target"
    target.mkdir()
    marker = target / "original"
    marker.write_bytes(b"keep me")
    destination = tmp_path / part.name
    destination.symlink_to(target)
    workspace = tmp_path / LOCAL_RESTORE_DIR
    (workspace / "all_2_2_0").mkdir(parents=True)

    restorer = _restorer(layout)
    restorer.download(part, str(destination))
    assert set(workspace.iterdir()) == {workspace / part.name}
    restorer.wait()

    assert set(tmp_path.iterdir()) == {target, destination}
    assert not destination.is_symlink()
    assert (destination / "checksums.txt").read_bytes() == b"checksums"
    assert marker.read_bytes() == b"keep me"


@pytest.mark.parametrize(
    "entry",
    [None, ".", "all_1_1_0"],
    ids=["detached", "workspace", "part"],
)
def test_prepare_preserves_workspace_symlinks(tmp_path, entry):
    root = tmp_path / "detached" / LOCAL_RESTORE_DIR
    target = tmp_path / "target"
    target.mkdir()
    link = root.parent if entry is None else root / entry
    link.parent.mkdir(parents=True, exist_ok=True)
    link.symlink_to(target)
    marker = root / "all_1_1_0" / "data.bin"
    marker.parent.mkdir(parents=True, exist_ok=True)
    marker.write_bytes(b"user data")

    with pytest.raises(ClickhouseBackupError, match="Unexpected"):
        _restorer(Mock()).prepare(root.parent)
    assert link.is_symlink()
    assert marker.read_bytes() == b"user data"


@pytest.mark.parametrize(
    "file",
    ["invalid/data", "all_1_1_0"],
)
def test_prepare_preserves_unrecognized_files(tmp_path, file):
    path = tmp_path / LOCAL_RESTORE_DIR / file
    path.parent.mkdir(parents=True)
    path.write_bytes(b"user data")

    with pytest.raises(ClickhouseBackupError, match="Unexpected"):
        _restorer(Mock()).prepare(tmp_path)
    assert path.read_bytes() == b"user data"
