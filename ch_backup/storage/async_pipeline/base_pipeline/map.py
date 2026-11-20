"""
Map runner for stage.
"""

from collections import deque
from concurrent.futures import Future, ThreadPoolExecutor
from contextlib import closing
from dataclasses import dataclass
from functools import partial
from sys import exc_info
from typing import Any, Callable, Generator, Iterable

from pypeln import utils as pypeln_utils
from pypeln.thread import Worker
from pypeln.thread.api.to_stage import to_stage
from pypeln.thread.stage import Stage
from pypeln.utils import A, B, Element

from ch_backup.storage.async_pipeline.base_pipeline.handler import Handler


@dataclass
class Map:
    """
    Map wrapper for stage handler.
    """

    handler: Handler

    def __call__(self, worker: Worker, **kwargs: Any) -> None:
        """
        Entrypoint for map stage.
        """
        idx = 0
        with closing(self.process(worker)) as results:
            for value in results:
                if value is not None:
                    # pylint: disable=no-value-for-parameter
                    worker.stage_params.output_queues.put(
                        Element(index=(idx,), value=value)
                    )
                    idx += 1

    def process(self, worker: Worker) -> Generator[B | None, None, None]:
        """
        Process map handler.
        """
        yield self.handler.on_start()

        for elem in worker.stage_params.input_queue:
            yield self.handler(elem.value, elem.index[0])

        yield self.handler.on_done()


@dataclass
class OrderedMap(Map):
    """Process a bounded window concurrently and emit results in input order."""

    workers: int

    def process(self, worker: Worker) -> Generator[B | None, None, None]:
        """Drain the window even if pypeln interrupts executor shutdown."""
        yield self.handler.on_start()
        pending: deque[Future] = deque()
        interrupted = False
        executor = ThreadPoolExecutor(max_workers=self.workers)
        try:
            for elem in worker.stage_params.input_queue:
                pending.append(executor.submit(self.handler, elem.value, elem.index[0]))
                if len(pending) == self.workers:
                    yield pending.popleft().result()
            while pending:
                yield pending.popleft().result()
        finally:
            while True:
                try:
                    executor.shutdown(wait=True, cancel_futures=True)
                    break
                except pypeln_utils.StopThreadException:
                    interrupted = True
            if interrupted and exc_info()[0] is None:
                raise pypeln_utils.StopThreadException
        yield self.handler.on_done()


# pylint: disable=too-many-positional-arguments
def map_(
    f: Handler,
    stage: Stage[A] | Iterable[A] | pypeln_utils.Undefined = pypeln_utils.UNDEFINED,
    workers: int = 1,
    maxsize: int = 0,
    timeout: float = 0,
    on_start: Callable = None,
    on_done: Callable = None,
    use_threads: bool = True,
    ordered: bool = False,
) -> Stage[None] | pypeln_utils.Partial[Stage[None] | None] | None:
    """
    Create map stage.
    """
    if isinstance(stage, pypeln_utils.Undefined):
        return pypeln_utils.Partial(
            lambda stage: map_(
                f,
                stage=stage,
                workers=workers,
                maxsize=maxsize,
                timeout=timeout,
                on_start=on_start,
                on_done=on_done,
                use_threads=use_threads,
                ordered=ordered,
            )
        )

    stage = to_stage(stage, maxsize=maxsize)

    stage = Stage(
        process_fn=OrderedMap(f, workers) if ordered else Map(f),  # type: ignore[call-arg]  # https://github.com/python/mypy/issues/6301
        workers=1 if ordered else workers,
        maxsize=maxsize,
        timeout=timeout,
        total_sources=stage.workers,
        dependencies=[stage],
        on_start=on_start,
        on_done=on_done,
        f_args=pypeln_utils.function_args(f),
    )

    return stage


thread_map = partial(map_, use_threads=True)
# TODO: make process version
