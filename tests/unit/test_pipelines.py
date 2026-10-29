import copy
import io
import tarfile
import threading
from collections.abc import Callable, Iterator
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from pypeln.thread import Worker

from ch_backup.compression import get_compression
from ch_backup.config import DEFAULT_CONFIG
from ch_backup.encryption import get_encryption
from ch_backup.storage import StorageLoader
from ch_backup.storage.async_pipeline.pipeline_builder import PipelineBuilder
from ch_backup.storage.async_pipeline.pipelines import (
    download_data_pipeline,
    run,
    run_and_collect_all,
    run_and_return_first,
)


@pytest.mark.parametrize("size", [0, 1, 63, 64, 65, 128, 129])
@pytest.mark.parametrize("download_chunk_size", [17, 104, 257])
def test_decrypt_downloaded_data_matches_download_pipeline(
    size: int, download_chunk_size: int
) -> None:
    config = copy.deepcopy(DEFAULT_CONFIG)
    config["encryption"].update(key="a" * 32, chunk_size=64)
    crypto = get_encryption("nacl", config["encryption"])
    plaintext = b"x" * size
    ciphertext = b"".join(
        crypto.encrypt(plaintext[pos : pos + 64]) for pos in range(0, size, 64)
    )
    with (
        patch("ch_backup.storage.loader.get_storage_engine"),
        patch("ch_backup.storage.loader.PipelineExecutor"),
    ):
        loader = StorageLoader(config)
    storage = MagicMock()
    storage.download_part.side_effect = [
        ciphertext[pos : pos + download_chunk_size]
        for pos in range(0, len(ciphertext), download_chunk_size)
    ] + [b""]
    with patch(
        "ch_backup.storage.async_pipeline.pipeline_builder.get_storage_engine",
        return_value=storage,
    ) as get_engine:
        assert loader.decrypt_data(ciphertext) == plaintext
        get_engine.assert_not_called()
        assert download_data_pipeline(config, "metadata", decrypt=True) == plaintext


def _pipeline_with_masked_error() -> Iterator[str]:
    yield from ()
    try:
        raise RuntimeError("pipeline failed")
    except RuntimeError:
        raise ValueError("Invalid thread ID 123")


def _pipeline_with_cleanup_error() -> Iterator[str]:
    yield "result"
    raise ValueError("Invalid thread ID 123")


@pytest.mark.parametrize("runner", [run, run_and_return_first, run_and_collect_all])
def test_run_restores_pipeline_error(runner: Callable[[Any], Any]) -> None:
    with pytest.raises(RuntimeError, match="pipeline failed"):
        runner(_pipeline_with_masked_error())


@pytest.mark.parametrize(
    "runner, expected",
    [(run, None), (run_and_return_first, "result"), (run_and_collect_all, ["result"])],
)
def test_run_ignores_cleanup_error_in_exception_handler(
    runner: Callable[[Any], Any], expected: Any
) -> None:
    # Saving FAILED metadata runs while the copy error is still being handled.
    try:
        raise RuntimeError("previous copy failed")
    except RuntimeError:
        assert runner(_pipeline_with_cleanup_error()) == expected


@pytest.mark.parametrize("runner", [run, run_and_return_first, run_and_collect_all])
@pytest.mark.parametrize("reuse_outer_error", [False, True])
def test_run_preserves_pipeline_error_in_exception_handler(
    runner: Callable[[Any], Any], reuse_outer_error: bool
) -> None:
    outer_error = RuntimeError("previous copy failed")
    pipeline_error = (
        outer_error if reuse_outer_error else RuntimeError("pipeline failed")
    )

    def pipeline() -> Iterator[str]:
        yield "result"
        try:
            raise pipeline_error
        except RuntimeError:
            raise ValueError("Invalid thread ID 123")

    try:
        raise outer_error
    except RuntimeError:
        with pytest.raises(RuntimeError) as raised:
            runner(pipeline())
        assert raised.value is pipeline_error


def test_run_ignores_cleanup_error() -> None:
    run(_pipeline_with_cleanup_error())


def test_run_and_return_first_ignores_cleanup_error() -> None:
    assert run_and_return_first(_pipeline_with_cleanup_error()) == "result"


def test_run_and_collect_all_ignores_cleanup_error() -> None:
    assert run_and_collect_all(_pipeline_with_cleanup_error()) == ["result"]


def test_run_and_return_first_does_not_ignore_error_before_result() -> None:
    def pipeline() -> Iterator[str]:
        yield from ()
        raise ValueError("Invalid thread ID 123")

    with pytest.raises(ValueError, match="Invalid thread ID"):
        run_and_return_first(pipeline())


@pytest.fixture(name="worker_exit_errors")
def fixture_worker_exit_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> Iterator[list[BaseException]]:
    """Hold a real worker after its exception handler exits to expose late cancellation."""
    ready = threading.Event()
    release = threading.Event()
    errors: list[BaseException] = []
    threads: list[threading.Thread] = []
    original_call = Worker.__call__
    original_stop = Worker.stop
    original_join = threading.Thread.join

    def delayed_exit(worker: Worker) -> None:
        threads.append(threading.current_thread())
        original_call(worker)
        ready.set()
        if not release.wait(5):
            raise TimeoutError("Worker exit was not released")

    def stop(worker: Worker) -> None:
        assert ready.wait(5), "Worker did not finish its task"
        try:
            original_stop(worker)
        finally:
            release.set()

    def join(thread: threading.Thread, timeout: float | None = None) -> None:
        release.set()
        original_join(thread, timeout)

    monkeypatch.setattr(Worker, "__call__", delayed_exit)
    monkeypatch.setattr(Worker, "stop", stop)
    monkeypatch.setattr(threading.Thread, "join", join)
    monkeypatch.setattr(
        threading, "excepthook", lambda args: errors.append(args.exc_value)
    )
    try:
        yield errors
    finally:
        release.set()
        for thread in threads:
            original_join(thread, 5)


@pytest.mark.parametrize(
    ("runner", "expected"),
    [(run, None), (run_and_return_first, "result"), (run_and_collect_all, ["result"])],
)
def test_completed_pipeline_does_not_interrupt_worker_exit(
    worker_exit_errors: list[BaseException], runner: Callable[[Any], Any], expected: Any
) -> None:
    builder = PipelineBuilder(DEFAULT_CONFIG).build_iterable_stage(["result"])

    assert runner(builder.pipeline()) == expected
    assert worker_exit_errors == []


@pytest.mark.parametrize("runner", [run, run_and_return_first, run_and_collect_all])
def test_pipeline_propagates_worker_error(runner: Callable[[Any], Any]) -> None:
    def source() -> Iterator[bytes]:
        yield b"data"
        raise RuntimeError("source failed")

    builder = PipelineBuilder(DEFAULT_CONFIG)
    builder.build_iterable_stage(source()).build_collect_data_stage()

    with pytest.raises(RuntimeError, match="source failed"):
        runner(builder.pipeline())


def test_empty_pipeline_finishes() -> None:
    builder = PipelineBuilder(DEFAULT_CONFIG).build_iterable_stage([])

    assert not run_and_collect_all(builder.pipeline())


def test_unpack_data_tarball_larger_than_buffer() -> None:
    config = copy.deepcopy(DEFAULT_CONFIG)
    config["filesystem"].update(buffer_size=64 * 1024, chunk_size=16 * 1024)
    files = [(f"part_{i}/column.bin", bytes([i % 256]) * 4096) for i in range(100)]
    tar = io.BytesIO()
    with tarfile.open(fileobj=tar, mode="w") as archive:
        for name, data in files:
            info = tarfile.TarInfo(name)
            info.size = len(data)
            archive.addfile(info, io.BytesIO(data))
        tarball = tar.getvalue()[: archive.offset]
    compressor = get_compression(config["compression"]["type"])
    compressed = compressor.compress(tarball) + compressor.flush_compress()
    assert len(compressed) < config["filesystem"]["buffer_size"] < len(tarball)

    builder = PipelineBuilder(config).build_iterable_stage([compressed])
    builder.build_decompress_stage().build_unpack_data_tarball_stage()

    assert run_and_collect_all(builder.pipeline()) == files
