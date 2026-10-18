"""Forced pool shutdown must return and allow the interpreter to exit."""

import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest


def _busy_worker(ready_path: str) -> None:
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    Path(ready_path).touch()
    time.sleep(60)


def _run_child(ready_path: str) -> None:
    # Imports follow the child source-tree selection in __main__.
    # pylint: disable=import-outside-toplevel
    from ch_backup import logging
    from ch_backup.storage.async_pipeline.base_pipeline import exec_pool

    logging.logger.remove()
    exec_pool.TIMEOUT_WORKER_TERMINATING_SEC = 0.1
    pool = exec_pool.ProcessExecPool(1)
    pool.submit("busy", _busy_worker, ready_path)
    deadline = time.monotonic() + 5
    while not Path(ready_path).exists():
        if time.monotonic() >= deadline:
            raise RuntimeError("Worker did not start")
        time.sleep(0.01)
    pool.submit("queued", len, b"x" * 2_000_000)
    # Let the feeder start writing while its only consumer remains busy.
    time.sleep(0.2)
    pool.shutdown(graceful=False)
    pool.shutdown(graceful=False)
    pool.shutdown()
    print("shutdown returned", flush=True)


@pytest.mark.skipif(os.name != "posix", reason="Uses POSIX signals and process groups")
def test_forced_shutdown_with_buffered_calls_exits(tmp_path: Path) -> None:
    source_root = Path(__file__).resolve().parents[2]
    # Explicit lifecycle lets the timeout cleanup kill the whole process group.
    # pylint: disable=consider-using-with
    proc = subprocess.Popen(
        [sys.executable, __file__, str(source_root), str(tmp_path / "ready")],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        start_new_session=True,
    )
    try:
        stdout, stderr = proc.communicate(timeout=15)
    finally:
        # Also clean up descendants when the child hangs or fails early.
        try:
            os.killpg(proc.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        proc.communicate()
    assert proc.returncode == 0, stderr
    assert "shutdown returned" in stdout


if __name__ == "__main__":
    sys.path.insert(0, sys.argv[1])
    _run_child(sys.argv[2])
