"""Verify explicit archive ownership, bounded reads and asynchronous file lifetime."""

# pylint: disable=protected-access,no-member

import gc
import tarfile
import weakref
from io import BytesIO
from pathlib import Path
from unittest.mock import patch

import pytest

from ch_backup.exceptions import StorageError
from ch_backup.storage.async_pipeline.stages.filesystem.write_files_stage import (
    UnpackTarballStage,
)
from ch_backup.storage.metadata_archive import (
    METADATA_ARCHIVE_BATCH_BYTES,
    MetadataArchiveWriter,
    TemporaryMetadataArchive,
    spool_metadata,
)
from tests.unit.test_keyless_metadata import get_loader, make_backup, make_layout


def read_files(archive: TemporaryMetadataArchive) -> list[tuple[str, bytes]]:
    """Collect explicit batches for assertions."""
    files = []
    continuation = None
    while True:
        batch, continuation = archive.read_batch(continuation)
        files.extend(batch)
        if continuation is None:
            return files


def test_finish_transfers_ownership_and_closes_writer() -> None:
    """Retain the finished file independently and reject further writes."""
    writer = MetadataArchiveWriter()
    writer.append(("part/data", b"payload"))
    archive = writer.finish()
    path = Path(archive.path())
    assert writer._file.closed
    assert path.stat().st_mode & 0o077 == 0
    with pytest.raises(ValueError, match="closed"):
        writer.append(("extra", b"data"))
    with pytest.raises(ValueError, match="closed"):
        writer.finish()
    del writer
    gc.collect()
    assert path.exists()
    assert read_files(archive) == [("part/data", b"payload")]
    del archive
    gc.collect()
    assert not path.exists()


def test_unfinished_writer_cleanup() -> None:
    """Remove an abandoned writer's file."""
    writer = MetadataArchiveWriter()
    path = Path(writer._file.name)
    del writer
    gc.collect()
    assert not path.exists()


@pytest.mark.parametrize("operation", ["append", "finish", "source"])
def test_failed_writing_removes_file_immediately(operation: str) -> None:
    """Clean up even while a failure traceback retains the writer."""
    writer = MetadataArchiveWriter()
    path = Path(writer._file.name)
    with pytest.raises(OSError) as error:
        if operation == "append":
            with patch.object(
                writer._file, "write", side_effect=OSError("write failed")
            ):
                writer.append(("part/data", b"payload"))
        elif operation == "finish":
            with patch(
                "ch_backup.storage.metadata_archive.TemporaryMetadataArchive",
                side_effect=OSError("finish failed"),
            ):
                writer.finish()
        else:

            def failing_source():
                """Fail after yielding one valid metadata file."""
                yield "part/data", b"payload"
                raise OSError("source failed")

            with patch(
                "ch_backup.storage.metadata_archive.MetadataArchiveWriter",
                return_value=writer,
            ):
                spool_metadata(failing_source())
    assert error.value.__traceback__ is not None
    assert writer._file.closed
    assert not path.exists()


def test_path_performs_no_io_and_reading_is_explicit() -> None:
    """Return only the filename and expose no iterable or length protocol."""
    archive = spool_metadata([])
    expected = archive.path()
    with (
        patch.object(Path, "open", side_effect=AssertionError),
        patch.object(Path, "stat", side_effect=AssertionError),
    ):
        assert archive.path() == expected
    with pytest.raises(TypeError):
        iter(archive)  # type: ignore[call-overload]
    with pytest.raises(TypeError):
        len(archive)  # type: ignore[arg-type]
    assert archive.read_batch() == ([], None)
    assert archive.count_files() == 0


def test_downloaded_archive_count_digest_and_repeated_reads(tmp_path: Path) -> None:
    """Preserve long names, empty files and the writing digest after downloading."""
    files = [("part/empty", b""), ("part/" + "long" * 50, b"data" * 300000)]
    original = spool_metadata(files)
    path = tmp_path / "download.tar"
    path.write_bytes(Path(original.path()).read_bytes())
    downloaded = TemporaryMetadataArchive(str(path))
    with patch.object(downloaded, "read_batch", wraps=downloaded.read_batch) as read:
        assert downloaded.count_files() == 2
        assert read.call_count > 0
        read.reset_mock()
        assert downloaded.count_files() == 2
        read.assert_not_called()
    assert downloaded.content_digest() == original.content_digest()
    assert read_files(downloaded) == read_files(downloaded) == files


@pytest.mark.parametrize("continuation", [-1, 1, 512 * 100])
def test_invalid_continuation_is_rejected(continuation: int) -> None:
    """Reject offsets outside the archive or between tar blocks."""
    archive = spool_metadata([("part/data", b"payload")])
    with pytest.raises(ValueError, match="continuation"):
        archive.read_batch(continuation)


def test_read_batches_bound_input_and_preserve_all_files() -> None:
    """Stop at a complete member after the input budget is exhausted."""
    files = [(str(i), b"x" * 1024) for i in range(2000)]
    archive = spool_metadata(files)
    first, continuation = archive.read_batch()
    assert continuation is not None
    assert (
        METADATA_ARCHIVE_BATCH_BYTES
        <= continuation
        < METADATA_ARCHIVE_BATCH_BYTES + 1536
    )
    assert 1 < len(first) < len(files)
    second, _ = archive.read_batch(continuation)
    assert second[0] == files[len(first)]
    assert read_files(archive) == files


def test_empty_directory_batch_does_not_end_reading(tmp_path: Path) -> None:
    """Return a continuation even when a bounded batch contains no files."""
    directory = tarfile.TarInfo("part")
    directory.type = tarfile.DIRTYPE
    original = spool_metadata([("part/data", b"payload")])
    path = tmp_path / "download.tar"
    path.write_bytes(directory.tobuf() + Path(original.path()).read_bytes())
    archive = TemporaryMetadataArchive(str(path))
    with patch("ch_backup.storage.metadata_archive.METADATA_ARCHIVE_BATCH_BYTES", 512):
        assert archive.read_batch() == ([], 512)
        assert archive.read_batch(512) == ([("part/data", b"payload")], None)
        assert archive.count_files() == 1


def test_pax_names_remain_readable(tmp_path: Path) -> None:
    """Read long Unicode names carried by extended tar headers."""
    output = BytesIO()
    name = "part/" + "данные" * 40
    with tarfile.open(fileobj=output, mode="w", format=tarfile.PAX_FORMAT) as tar:
        member = tarfile.TarInfo(name)
        member.size = len(b"payload")
        tar.addfile(member, BytesIO(b"payload"))
        end = tar.offset
    path = tmp_path / "download.tar"
    path.write_bytes(output.getvalue()[:end])
    archive = TemporaryMetadataArchive(str(path))
    assert read_files(archive) == [(name, b"payload")]


@pytest.mark.parametrize("kind", ["directory", "nonzero padding", "partial header"])
def test_reader_preserves_previous_parser_contract(tmp_path: Path, kind: str) -> None:
    """Skip empty directories and reject corrupt padding and trailing headers."""
    member = tarfile.TarInfo("part/data")
    member.size = 3
    raw = member.tobuf() + b"abc" + b"\0" * 509
    if kind == "directory":
        directory = tarfile.TarInfo("part")
        directory.type = tarfile.DIRTYPE
        raw = directory.tobuf() + raw
    elif kind == "nonzero padding":
        raw = raw[:-1] + b"x"
    else:
        raw += b"truncated"
    path = tmp_path / "download.tar"
    path.write_bytes(raw)
    archive = TemporaryMetadataArchive(str(path))
    previous = UnpackTarballStage({}, len(raw) + 512, include_empty=True)
    if kind == "directory":
        previous(raw, 0)
        expected = previous.take_files() + list(previous.on_done())
        assert read_files(archive) == expected == [("part/data", b"abc")]
    else:
        with pytest.raises((AssertionError, ValueError)):
            previous(raw, 0)
            list(previous.on_done())
        with pytest.raises(ValueError, match="Truncated TAR"):
            read_files(archive)


def test_failed_download_removes_partial_destination() -> None:
    """Keep download ownership until success and unlink partial output on failure."""
    layout = make_layout()
    paths = []

    def download(_remote, local, **_kwargs):
        """Leave partial output before reporting the failed transfer."""
        path = Path(local)
        paths.append(path)
        path.write_bytes(b"partial")
        raise OSError("download failed")

    get_loader(layout).download_file.side_effect = download
    with pytest.raises(StorageError) as error:
        layout.download_cloud_storage_metadata_archive(make_backup(), "table.tar")
    assert error.value.__cause__ is not None
    assert len(paths) == 1
    assert not paths[0].exists()


def test_successful_download_transfers_destination_ownership() -> None:
    """Return a completed archive whose lifetime controls the downloaded file."""
    layout = make_layout()
    original = spool_metadata([("part/data", b"payload")])

    def download(_remote, local, **_kwargs):
        """Complete the transfer before exposing its destination."""
        Path(local).write_bytes(Path(original.path()).read_bytes())

    get_loader(layout).download_file.side_effect = download
    archive = layout.download_cloud_storage_metadata_archive(make_backup(), "table.tar")
    path = Path(archive.path())
    assert archive.read_batch() == ([("part/data", b"payload")], None)
    del archive
    gc.collect()
    assert not path.exists()


@pytest.mark.parametrize("success", [True, False])
@pytest.mark.parametrize("callback_raises", [True, False])
def test_async_upload_retains_archive_until_callback_finishes(
    success: bool, callback_raises: bool
) -> None:
    """Keep the file through success and failure, then release both callbacks' ownership."""
    layout = make_layout()
    archive = spool_metadata([("part/data", b"payload")])
    path = Path(archive.path())
    owner = weakref.ref(archive)
    failure = OSError("upload failed")
    calls = []

    def callback(*args):
        """Check that the file survives until user completion handling finishes."""
        assert path.exists()
        assert owner() is not None
        calls.append(args)
        if callback_raises:
            raise RuntimeError("callback failed")

    layout.upload_cloud_storage_metadata_files(
        make_backup(), "table.tar", archive, callback=callback, on_error=callback
    )
    pending = get_loader(layout).upload_file.call_args.kwargs
    assert pending["is_async"]
    del archive
    gc.collect()
    assert path.exists()
    action = pending["callback"] if success else lambda: pending["on_error"](failure)
    if callback_raises:
        with pytest.raises(RuntimeError, match="callback failed"):
            action()
    else:
        action()
    assert calls == [() if success else (failure,)]
    gc.collect()
    assert owner() is None
    assert not path.exists()


@pytest.mark.parametrize("success", [True, False])
def test_async_upload_without_user_callbacks(success: bool) -> None:
    """Release the upload's archive when no user callback was supplied."""
    layout = make_layout()
    layout.upload_cloud_storage_metadata_files(
        make_backup(), "table.tar", [("part/data", b"payload")]
    )
    pending = get_loader(layout).upload_file.call_args.kwargs
    path = Path(pending["local_path"])
    assert path.exists()
    if success:
        pending["callback"]()
    else:
        pending["on_error"](OSError("upload failed"))
    gc.collect()
    assert not path.exists()
