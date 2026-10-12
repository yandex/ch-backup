"""Copy result callbacks must work identically with and without workers."""

# pylint: disable=protected-access

from concurrent.futures import BrokenExecutor
from functools import partial
from unittest.mock import MagicMock

import pytest

from ch_backup import logging
from ch_backup.storage.async_pipeline.base_pipeline.exec_pool import ThreadExecPool
from ch_backup.storage.async_pipeline.pipeline_executor import PipelineExecutor


@pytest.mark.parametrize("workers", [0, 2])
def test_pipeline_callbacks_collect_errors_and_allow_next_batch(
    workers: int, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Pytest's captured stderr cannot be pickled for spawned workers.
    monkeypatch.setattr(logging.logger._core, "handlers", {})  # type: ignore[attr-defined]
    executor = PipelineExecutor({"multiprocessing": {"workers": workers}})
    completed = []
    errors: list[Exception] = []
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
    errors: list[Exception] = []
    with pytest.raises(BrokenExecutor):
        executor._exec_pipeline("bad", int, True, on_error=errors.append)
    assert errors == [failure]


def test_draining_pool_does_not_repeat_consumed_callbacks() -> None:
    errors: list[Exception] = []
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
    errors: list[Exception] = []

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
