"""
Logging module.
"""

import os
from typing import Any

import psutil

from ch_backup.util import format_size, cached_property
from loguru import logger


class Filter:
    def __init__(self, name):
        self._name = name

    def __call__(self, record):
        return record["extra"].get("name") == self._name
        

def make_filter(name):
    return Filter(name)

def configure(config: dict, config_loguru: dict) -> None:
    """
    Configure logger.
    """
    for handler in config_loguru["handlers"]:
        handler['filter'] = make_filter(handler['name'])

    config_loguru
    logger.configure(
        handlers = config_loguru["handlers"],
        activation = config_loguru["activation"]
    )


def critical(msg, *args, **kwargs):
    """
    Log a message with severity 'CRITICAL'.
    """
    getLogger('ch-backup').critical(msg, *args, **kwargs)


def error(msg, exc_info=False,*args, **kwargs):
    """
    Log a message with severity 'ERROR'.
    """
    getLogger('ch-backup').opt(exception=exc_info).error(msg, *args, **kwargs)


def exception(msg, *args, **kwargs):
    """
    Log a message with severity 'ERROR' with exception information.
    """
    getLogger('ch-backup').exception(msg, *args, **kwargs)


def warning(msg, exc_info=False, *args, **kwargs):
    """
    Log a message with severity 'WARNING'.
    """
    getLogger('ch-backup').opt(exception=exc_info).warning(msg, *args, **kwargs)


def info(msg, *args, **kwargs):
    """
    Log a message with severity 'INFO'.
    """
    getLogger('ch-backup').info(msg, *args, **kwargs)


def debug(msg, *args, **kwargs):
    """
    Log a message with severity 'DEBUG'.
    """
    getLogger('ch-backup').debug(msg, *args, **kwargs)


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

        debug(
            "Memory usage: {} (main process: {}, worker processes: {})",
            format_size(total_usage),
            format_size(main_proc_usage),
            format_size(worker_procs_usage),
        )

    except Exception:
        warning("Unable to get memory usage",exc_info=True)

def getLogger(name: str):
    return logger.bind(name=name)
