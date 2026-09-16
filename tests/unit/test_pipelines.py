from collections.abc import Callable, Iterator
from typing import Any

import pytest

from ch_backup.storage.async_pipeline.pipelines import (
    run,
    run_and_collect_all,
    run_and_return_first,
)


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
