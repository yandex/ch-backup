"""Copy result callbacks must work identically with and without workers."""

# pylint: disable=protected-access

from concurrent.futures import BrokenExecutor
from functools import partial
from unittest.mock import MagicMock, call

import pytest

from ch_backup import logging
from ch_backup.exceptions import TerminatingSignal
from ch_backup.storage.async_pipeline.base_pipeline.exec_pool import (
    ProcessExecPool,
    ThreadExecPool,
)
from ch_backup.storage.async_pipeline.pipeline_executor import PipelineExecutor


@pytest.mark.parametrize("workers", [0, 2])
def test_pipeline_callbacks_collect_errors_and_allow_next_batch(
    workers: int, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Pytest's captured stderr cannot be pickled for spawned workers.
    monkeypatch.setattr(logging.logger._core, "handlers", {})  # type: ignore[attr-defined]
    executor = PipelineExecutor({"multiprocessing": {"workers": workers}})
    completed = []
    errors: list[BaseException] = []
    try:
        if workers:
            executor._exec_pipeline(
                "bad", partial(int, "invalid"), True, on_error=errors.append
            )
        else:
            with pytest.raises(ValueError):
                executor._exec_pipeline(
                    "bad", partial(int, "invalid"), True, on_error=errors.append
                )
        executor._exec_pipeline(
            "good", partial(int, "1"), True, callback=lambda: completed.append("good")
        )
        executor.wait(keep_going=True)
        executor._exec_pipeline(
            "next", partial(int, "2"), True, callback=lambda: completed.append("next")
        )
        executor.wait(keep_going=True)
        assert completed == ["good", "next"]
        assert len(errors) == 1
        assert isinstance(errors[0], ValueError)
    finally:
        if executor._exec_pool:
            executor._exec_pool.shutdown()


def test_submission_error_is_reported_once() -> None:
    executor = PipelineExecutor({"multiprocessing": {"workers": 0}})
    executor._exec_pool = MagicMock()
    failure = BrokenExecutor("cannot submit")
    executor._exec_pool.submit.side_effect = failure
    errors: list[BaseException] = []
    with pytest.raises(BrokenExecutor):
        executor._exec_pipeline("bad", int, True, on_error=errors.append)
    assert errors == [failure]


def test_ungraceful_process_pool_shutdown_is_bounded_for_stuck_worker() -> None:
    executor = ProcessExecPool.__new__(ProcessExecPool)
    executor._pool = MagicMock()
    process = MagicMock()
    process.is_alive.side_effect = [True, True, True, True]
    executor._pool._processes = {1: process}

    executor.shutdown(graceful=False)

    assert process.method_calls == [
        call.is_alive(),
        call.terminate(),
        call.join(5.0),
        call.is_alive(),
        call.kill(),
        call.is_alive(),
        call.join(5.0),
        call.is_alive(),
    ]
    executor._pool.shutdown.assert_called_once_with(wait=False, cancel_futures=True)


def test_ungraceful_process_pool_shutdown_waits_after_worker_exits() -> None:
    executor = ProcessExecPool.__new__(ProcessExecPool)
    executor._pool = MagicMock()
    process = MagicMock()
    process.is_alive.side_effect = [True, True, True, False]
    executor._pool._processes = {1: process}

    executor.shutdown(graceful=False)

    executor._pool.shutdown.assert_called_once_with(wait=True, cancel_futures=True)


def test_ungraceful_process_pool_shutdown_is_idempotent() -> None:
    executor = ProcessExecPool.__new__(ProcessExecPool)
    executor._pool = MagicMock()
    process = MagicMock()
    process.is_alive.return_value = False
    executor._pool._processes = {1: process}

    executor.shutdown(graceful=False)
    executor.shutdown(graceful=False)

    executor._pool.shutdown.assert_called_once_with(wait=True, cancel_futures=True)


def test_draining_pool_does_not_repeat_consumed_callbacks() -> None:
    errors: list[BaseException] = []
    completed = []
    with ThreadExecPool(1) as pool:
        pool.submit("bad", partial(int, "invalid"), on_error=errors.append)
        with pytest.raises(ValueError):
            pool.wait_all()
        pool.submit("good", int, callback=lambda: completed.append("good"))
        pool.wait_all(keep_going=True)
    assert len(errors) == 1
    assert completed == ["good"]


def test_broken_executor_is_fatal_even_when_wrapped() -> None:
    errors: list[BaseException] = []

    def broken():
        try:
            raise BrokenExecutor("workers unavailable")
        except BrokenExecutor as e:
            raise RuntimeError("wrapped failure") from e

    with ThreadExecPool(1) as pool:
        pool.submit("bad", broken, on_error=errors.append)
        with pytest.raises(RuntimeError, match="wrapped failure"):
            pool.wait_all(keep_going=True)
    assert len(errors) == 1


def _raise_terminating_signal() -> None:
    raise TerminatingSignal("worker interrupted")


@pytest.mark.parametrize("pool_type", [ThreadExecPool, ProcessExecPool])
@pytest.mark.parametrize("keep_going", [False, True])
def test_terminating_signal_is_reported_and_always_propagated(
    pool_type: type[ThreadExecPool] | type[ProcessExecPool],
    keep_going: bool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Pytest's captured stderr cannot be pickled for spawned workers.
    monkeypatch.setattr(logging.logger._core, "handlers", {})  # type: ignore[attr-defined]
    errors: list[BaseException] = []
    completed = MagicMock()
    with pool_type(1) as pool:
        pool.submit(
            "interrupted",
            _raise_terminating_signal,
            callback=completed,
            on_error=errors.append,
        )
        with pytest.raises(TerminatingSignal, match="worker interrupted"):
            pool.wait_all(keep_going=keep_going)
    assert len(errors) == 1
    assert isinstance(errors[0], TerminatingSignal)
    completed.assert_not_called()
