"""
Logging module.
"""

import logging
import logging.config
import os

import psutil

from ch_backup.util import format_size


def configure(config: dict) -> None:
    """
    Configure logging.
    """
    for handler in config.get('handlers', {}).values():
        filename = handler.get('filename')
        if filename:
            os.makedirs(os.path.dirname(filename), exist_ok=True)

    logging.config.dictConfig(config)


def critical(msg, *args, **kwargs):
    """
    Log a message with severity 'CRITICAL'.
    """
    _get_logger().critical(msg, *args, **kwargs)


def error(msg, *args, **kwargs):
    """
    Log a message with severity 'ERROR'.
    """
    _get_logger().error(msg, *args, **kwargs)


def exception(msg, *args, **kwargs):
    """
    Log a message with severity 'ERROR' with exception information.
    """
    _get_logger().exception(msg, *args, **kwargs)


def warning(msg, *args, **kwargs):
    """
    Log a message with severity 'WARNING'.
    """
    _get_logger().warning(msg, *args, **kwargs)


def info(msg, *args, **kwargs):
    """
    Log a message with severity 'INFO'.
    """
    _get_logger().info(msg, *args, **kwargs)


def debug(msg, *args, **kwargs):
    """
    Log a message with severity 'DEBUG'.
    """
    _get_logger().debug(msg, *args, **kwargs)


def memory_usage():
    """
    Log memory usage information.

    It's assumed that a big amount of memory is shared across main and worker processes. So shared memory is accounted
    only for main process.
    """
    try:
        main_proc = psutil.Process()
        main_proc_usage = main_proc.memory_info().rss

        worker_procs_usage = 0
        for proc in main_proc.children():
            memory_info = proc.memory_info()
            worker_procs_usage += memory_info.rss - memory_info.shared

        total_usage = main_proc_usage + worker_procs_usage

        debug('Memory usage: %s (main process: %s, worker processes: %s)', format_size(total_usage),
              format_size(main_proc_usage), format_size(worker_procs_usage))

    except Exception:
        warning('Unable to get memory usage', exc_info=True)


def _get_logger() -> logging.Logger:
    return logging.getLogger('ch-backup')
