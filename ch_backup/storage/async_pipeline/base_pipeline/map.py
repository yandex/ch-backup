"""
Map runner for stage.
"""

from dataclasses import dataclass
from functools import partial
from typing import Any, Callable, Iterable, Optional, Union

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
        for value in self.process(worker):
            if value is not None:
                # pylint: disable=no-value-for-parameter
                worker.stage_params.output_queues.put(
                    Element(index=(idx,), value=value)
                )
                idx += 1

    def process(self, worker: Worker) -> Iterable[Optional[B]]:
        """
        Process map handler.
        """
        yield self.handler.on_start()

        for elem in worker.stage_params.input_queue:
            yield self.handler(elem.value, elem.index[0])

        yield self.handler.on_done()


def map_(
    f: Handler,
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
            )
        )

    stage = to_stage(stage, maxsize=maxsize)

    stage = Stage(
        process_fn=Map(f),  # type: ignore[call-arg]  # https://github.com/python/mypy/issues/6301
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


thread_map = partial(map_, use_threads=True)
# TODO: make process version
