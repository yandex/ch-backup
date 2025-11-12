"""
Class for executing callables on specified pool.
"""

import multiprocessing
import os
import signal
import threading
import time
from concurrent.futures import (
    Executor,
    Future,
    ProcessPoolExecutor,
    ThreadPoolExecutor,
    as_completed,
)
from dataclasses import dataclass
from multiprocessing import get_context
from typing import Any, Callable, Dict, Iterable, Optional

from ch_backup import logging
from ch_backup.util import exhaust_iterator

TIMEOUT_WORKER_TERMINATING_SEC = 5.0


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
        # It is necessary to start all processes while there are no running threads
        # Used to freeze and backup tables at the same time
        self._start_processes()

    def shutdown(self, graceful: bool = True) -> None:
        """
        Wait workers for complete jobs and shutdown workers
        """
        self._pool.shutdown(wait=graceful, cancel_futures=True)

    def submit(
        self,
        job_id: str,
        func: Callable,
        *args: Any,
        callback: Optional[Callable] = None,
        **kwargs: Any,
    ) -> None:
        """
        Schedule job for execution
        """
        if job_id in [job.id_ for job in self._future_to_job.values()]:
            raise RuntimeError("Duplicate")

        future = self._pool.submit(func, *args, **kwargs)
        self._future_to_job[future] = Job(job_id, callback)

    @staticmethod
    def _start():
        return

    def _start_processes(self):
        future = self._pool.submit(ExecPool._start)
        future.result()

    def as_completed(
        self, keep_going: bool = False, timeout: Optional[float] = None
    ) -> Iterable[Any]:
        """
        Return result from futures as they are completed.

        Args:
            keep_going - skip exceptions raised by futures instead of propagating it.
        """
        for future in as_completed(self._future_to_job, timeout):
            job = self._future_to_job[future]
            logging.debug("Future {} completed", job.id_)

            try:
                result = future.result()
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

            yield result

        self._future_to_job = {}

    def wait_all(
        self, keep_going: bool = False, timeout: Optional[float] = None
    ) -> None:
        """
        Wait workers for complete jobs.

        Args:
            keep_going - skip exceptions raised by futures instead of propagating it.
        """
        exhaust_iterator(iter(self.as_completed(keep_going, timeout)))

    def __del__(self) -> None:
        """
        Shutdown pool explicitly to prevent the program from hanging in case of ungraceful termination.
        """
        try:
            self.shutdown()
        except Exception:  # nosec B110
            pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            self.shutdown()
        except Exception:  # nosec B110
            pass
        return False


class ThreadExecPool(ExecPool):
    """
    Submit tasks on ThreadPoolExecutor.

    Encapsulate collecting of futures and waiting all submitted tasks.
    """

    def __init__(self, workers: int) -> None:
        super().__init__(ThreadPoolExecutor(workers))


def _init_logger(logger_: Any) -> None:
    """
    Init logger on spawned process.
    """
    logging.logger = logger_


def _init_terminate_thread() -> None:
    """
    Starts a thread to watch if parent process is dead to prevent orphaned processes.
    """

    def _run() -> None:
        sleep_time = 10.0
        while True:
            parent_process = multiprocessing.parent_process()
            if parent_process is None or not parent_process.is_alive():
                os.kill(os.getpid(), signal.SIGTERM)
                return
            time.sleep(sleep_time)

    terminate_thread = threading.Thread(target=_run, daemon=True)
    terminate_thread.start()


def _init_process(logger: Any) -> None:
    """
    Initialize processes in pool.
    """
    _init_logger(logger)
    _init_terminate_thread()


class ProcessExecPool(ExecPool):
    """
    Submit tasks on ProcessPoolExecutor.

    Encapsulate collecting of futures and waiting all submitted tasks.
    """

    _pool: ProcessPoolExecutor

    def __init__(self, workers: int) -> None:
        super().__init__(
            ProcessPoolExecutor(
                max_workers=workers,
                mp_context=get_context("spawn"),
                initializer=_init_process,
                initargs=(logging.logger,),
            )
        )

    def shutdown(self, graceful: bool = True) -> None:
        """
        Wait workers for complete jobs and shutdown workers
        """
        # Try to shutdown gracefully
        if graceful:
            self._pool.shutdown(wait=True, cancel_futures=True)
            return

        # Send SIGTERM to the pool processes
        # pylint: disable=protected-access
        for p in self._pool._processes.values():
            if p.is_alive():
                try:
                    p.terminate()
                except Exception:
                    logging.exception("Failed terminating of worker process. Ignore it")

        # Wait for termination
        remaining_timeout = TIMEOUT_WORKER_TERMINATING_SEC
        for p in self._pool._processes.values():
            start = time.time()
            p.join(remaining_timeout)
            elapsed = time.time() - start
            remaining_timeout = max(remaining_timeout - elapsed, 0)

        # Forcibly kill it if it's not responding to the SIGTERM
        for p in self._pool._processes.values():
            if p.is_alive():
                try:
                    p.kill()
                except Exception:
                    logging.exception("Failed killing of worker process. Ignore it")
