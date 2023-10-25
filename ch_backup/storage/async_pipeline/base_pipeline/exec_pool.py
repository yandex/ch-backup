"""
Class for executing callables on specified pool.
"""
from concurrent.futures import Executor, Future, as_completed
from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional

from ch_backup import logging


@dataclass
class Job:
    """
    Job submitted to ExecPool.

    Callback is executed after job completion.
    """

    id_: str
    callback: Optional[Callable]


class ExecPool:
    """
    Submit tasks on provided executor.

    Encapsulate collecting of futures and waiting all submitted tasks.
    """

    def __init__(self, executor: Executor) -> None:
        self._future_to_job: Dict[Future, Job] = {}
        self._pool = executor

    def shutdown(self, graceful: bool = True) -> None:
        """
        Wait workers for complete jobs and shutdown workers
        """
        self._pool.shutdown(wait=graceful)

    def submit(
        self,
        job_id: str,
        func: Callable,
        callback: Optional[Callable],
        *args: Any,
        **kwargs: Any
    ) -> None:
        """
        Schedule job for execution
        """
        if job_id in [job.id_ for job in self._future_to_job.values()]:
            raise RuntimeError("Duplicate")

        future = self._pool.submit(func, *args, **kwargs)
        self._future_to_job[future] = Job(job_id, callback)

    def wait_all(self, keep_going: bool = False) -> None:
        """
        Wait workers for complete jobs.

        Args:
            keep_going - skip exceptions raised by futures instead of propagating it.
        """
        for future in as_completed(self._future_to_job):
            job = self._future_to_job[future]
            logging.debug("Future {} completed", job.id_)

            try:
                future.result()
            except Exception:
                if keep_going:
                    logging.warning(
                        'Job "{}" generated an exception, skipping due to keep_going flag',
                        job.id_,
                        exc_info=True,
                    )
                    continue
                logging.error(
                    'Job "{}" generated an exception:', job.id_, exc_info=True
                )
                raise

            if job.callback:
                job.callback()

        self._future_to_job = {}

    def __del__(self) -> None:
        """
        Shutdown pool explicitly to prevent the program from hanging in case of ungraceful termination.
        """
        self.shutdown()
