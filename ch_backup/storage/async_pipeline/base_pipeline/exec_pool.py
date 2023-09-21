"""
Class for executing callables on specified pool.
"""
from concurrent.futures import ALL_COMPLETED, Executor, Future, wait
from typing import Any, Callable, Dict

from ch_backup import logging


class ExecPool:
    """
    Submit tasks on provided executor.

    Encapsulate collecting of futures and waiting all submitted tasks.
    """

    def __init__(self, executor: Executor) -> None:
        self._futures: Dict[str, Future] = {}
        self._pool = executor

    def shutdown(self, graceful: bool = True) -> None:
        """
        Wait workers for complete jobs and shutdown workers
        """
        self._pool.shutdown(wait=graceful)

    def submit(self, future_id: str, func: Callable, *args: Any, **kwargs: Any) -> None:
        """
        Schedule job for execution
        """
        if future_id in self._futures:
            raise RuntimeError("Duplicate")
        future = self._pool.submit(func, *args, **kwargs)
        future.add_done_callback(lambda _: logging.debug('Future {} completed', future_id))  # type: ignore[misc]
        self._futures[future_id] = future

    def wait_all(self, keep_going: bool = False) -> None:
        """
        Wait workers for complete jobs.

        Args:
            keep_going - skip exceptions raised by futures instead of propagating it.
        """
        wait(self._futures.values(), return_when=ALL_COMPLETED)

        for future_id, future in self._futures.items():
            try:
                future.result()
            except Exception:
                if keep_going:
                    logging.warning(
                        'Future "{}" generated an exception, skipping due to keep_going flag',
                        # exc_info=True,
                        future_id,
                    )
                    continue
                logging.error(
                    'Future "{}" generated an exception:', future_id
                    #, exc_info=True
                )
                raise
        self._futures = {}

    def __del__(self) -> None:
        """
        Shutdown pool explicitly to prevent the program from hanging in case of ungraceful termination.
        """
        self.shutdown()
