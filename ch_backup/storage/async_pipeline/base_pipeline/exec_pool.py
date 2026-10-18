"""
Class for executing callables on specified pool.
"""

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
from typing import Any, Callable, Iterable

from ch_backup import logging
from ch_backup.exceptions import TerminatingSignal, is_broken_executor
from ch_backup.util import exhaust_iterator

TIMEOUT_WORKER_TERMINATING_SEC = 5.0


@dataclass
class Job:
    """
    Job submitted to ExecPool.

    Callback is executed after job completion.
    """

    id_: str
    callback: Callable | None
    on_error: Callable[[BaseException], None] | None = None


class ExecPool:
    """
    Submit tasks on provided executor.

    Encapsulate collecting of futures and waiting all submitted tasks.
    """

    def __init__(self, executor: Executor) -> None:
        self._future_to_job: dict[Future, Job] = {}
        self._pool = executor
        self._shutdown = False
        # It is necessary to start all processes while there are no running threads
        # Used to freeze and backup tables at the same time
        self._start_processes()

    def shutdown(self, graceful: bool = True) -> None:
        """
        Wait workers for complete jobs and shutdown workers
        """
        if self._shutdown:
            return
        try:
            self._pool.shutdown(wait=graceful, cancel_futures=True)
        finally:
            self._shutdown = True

    def submit(
        self,
        job_id: str,
        func: Callable,
        *args: Any,
        callback: Callable | None = None,
        on_error: Callable[[BaseException], None] | None = None,
        **kwargs: Any,
    ) -> None:
        """
        Schedule job for execution
        """
        if job_id in [job.id_ for job in self._future_to_job.values()]:
            raise RuntimeError("Duplicate")

        future = self._pool.submit(func, *args, **kwargs)
        self._future_to_job[future] = Job(job_id, callback, on_error)

    @staticmethod
    def _start():
        return

    def _start_processes(self):
        future = self._pool.submit(ExecPool._start)
        future.result()

    def as_completed(
        self, keep_going: bool = False, timeout: float | None = None
    ) -> Iterable[Any]:
        """
        Return result from futures as they are completed.

        Args:
            keep_going - skip exceptions raised by futures instead of propagating it.
        """
        for future in as_completed(self._future_to_job, timeout):
            job = self._future_to_job.pop(future)
            logging.debug("Future {} completed", job.id_)

            try:
                result = future.result()
            except (Exception, TerminatingSignal) as e:
                if job.on_error:
                    job.on_error(e)
                if (
                    keep_going
                    and isinstance(e, Exception)
                    and not is_broken_executor(e)
                ):
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

    def wait_all(self, keep_going: bool = False, timeout: float | None = None) -> None:
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
        initial_ppid = os.getppid()
        sleep_time = 10.0
        while True:
            # Check if os.getppid() == 1 is not good enough,
            # systemd can have child processes with id not equal to 1.
            # This may also not work properly if parent id is reused by other process.
            if initial_ppid == 1 or os.getppid() != initial_ppid:
                os.kill(os.getpid(), signal.SIGTERM)
                time.sleep(sleep_time)
                os.kill(os.getpid(), signal.SIGKILL)
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
        # ProcessPoolExecutor clears _processes during shutdown. Check both our
        # state and its state because shutdown may also be initiated externally.
        # pylint: disable=protected-access
        if getattr(self, "_shutdown", False) or self._pool._processes is None:
            self._shutdown = True
            return

        # Try to shutdown gracefully
        if graceful:
            try:
                self._pool.shutdown(wait=True, cancel_futures=True)
            finally:
                self._shutdown = True
            return

        # Send SIGTERM to the pool processes
        processes = list(self._pool._processes.values())
        for p in processes:
            if p.is_alive():
                try:
                    p.terminate()
                except Exception:
                    logging.exception("Failed terminating of worker process. Ignore it")

        # Wait for termination
        remaining_timeout = TIMEOUT_WORKER_TERMINATING_SEC
        for p in processes:
            start = time.time()
            p.join(remaining_timeout)
            elapsed = time.time() - start
            remaining_timeout = max(remaining_timeout - elapsed, 0)

        # Forcibly kill it if it's not responding to the SIGTERM
        for p in processes:
            if p.is_alive():
                try:
                    p.kill()
                except Exception:
                    logging.exception("Failed killing of worker process. Ignore it")

        # Give killed workers a bounded amount of time to exit. A process stuck in
        # an uninterruptible kernel wait may remain alive even after SIGKILL.
        remaining_timeout = TIMEOUT_WORKER_TERMINATING_SEC
        for p in processes:
            if not p.is_alive():
                continue
            start = time.time()
            p.join(remaining_timeout)
            elapsed = time.time() - start
            remaining_timeout = max(remaining_timeout - elapsed, 0)

        stuck_processes = [p for p in processes if p.is_alive()]
        if stuck_processes:
            logging.error(
                "Failed to stop {} worker process(es); abandoning executor cleanup",
                len(stuck_processes),
            )

        # ProcessPoolExecutor owns a management thread and multiprocessing queues in
        # addition to the workers. Finalize them synchronously only when every
        # worker has exited; otherwise its manager thread can wait indefinitely.
        try:
            self._pool.shutdown(
                wait=not stuck_processes,
                cancel_futures=True,
            )
        finally:
            self._shutdown = True
