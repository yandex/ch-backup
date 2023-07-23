"""
Flat map runner for stage.
"""
from dataclasses import dataclass
from functools import partial
from typing import Any, Callable, Iterable, Optional, Union

from pypeln import utils as pypeln_utils
from pypeln.thread import Worker
from pypeln.thread.api.to_stage import to_stage
from pypeln.thread.stage import Stage
from pypeln.utils import A, B, Element

from ch_backup.storage.async_pipeline.base_pipeline.handler import IterableHandler


@dataclass
class FlatMap:
    """
    Flat map wrapper for stage handler.
    """

    handler: IterableHandler

    def __call__(self, worker: Worker, **kwargs: Any) -> None:
        """
        Entrypoint for flat map stage.
        """
        for idx, value in enumerate(self.process(worker)):
            # pylint: disable=no-value-for-parameter
            worker.stage_params.output_queues.put(Element(index=(idx,), value=value))

    def process(self, worker: Worker) -> Iterable[B]:
        """
        Process flat map handler.
        """
        for value in self.handler.on_start() or []:
            yield value

        for elem in worker.stage_params.input_queue:
            yield from self.handler(elem.value, elem.index[0]) or []

        for value in self.handler.on_done() or []:
            yield value


def flat_map(
    f: IterableHandler,
    stage: Union[
        Stage[A], Iterable[A], pypeln_utils.Undefined
    ] = pypeln_utils.UNDEFINED,
    workers: int = 1,
    maxsize: int = 0,
    timeout: float = 0,
    on_start: Callable = None,
    on_done: Callable = None,
    use_threads: bool = True,
) -> Union[Optional[Stage[None]], pypeln_utils.Partial[Optional[Stage[None]]]]:
    """
    Create flat map stage.

    Flatten iterables returned from handler methods, i.e. [[a, b], [c, d]] -> [a, b, c, d]
    """
    if isinstance(stage, pypeln_utils.Undefined):
        return pypeln_utils.Partial(
            lambda stage: flat_map(
                f,
                stage=stage,
                workers=workers,
                maxsize=maxsize,
                timeout=timeout,
                on_start=on_start,
                on_done=on_done,
                use_threads=use_threads,
            )
        )

    stage = to_stage(stage, maxsize=maxsize)

    stage = Stage(
        process_fn=FlatMap(f),  # type: ignore[call-arg]  # https://github.com/python/mypy/issues/6301
        workers=workers,
        maxsize=maxsize,
        timeout=timeout,
        total_sources=stage.workers,
        dependencies=[stage],
        on_start=on_start,
        on_done=on_done,
        f_args=pypeln_utils.function_args(f),
    )

    return stage


thread_flat_map = partial(flat_map, use_threads=True)
# TODO: make process version
