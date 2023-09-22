"""
Logging module.
"""

import os
from typing import Any
import logging
import psutil
import inspect
from ch_backup.util import format_size, cached_property
from loguru import logger


class Filter:
    def __init__(self, name):
        self._name = name

    def __call__(self, record):
        if 'name' in record.get('extra', {}):
            return record["extra"].get("name") == self._name

        if record['name'] == self._name:
            record["extra"]['name'] = self._name
            return True
        return False
        
def make_filter(name):
    return Filter(name)

class InterceptHandler(logging.Handler):
    def emit(self, record: logging.LogRecord) -> None:
        # Get corresponding Loguru level if it exists.
        level: str | int
        try:
            level = logger.level(record.levelname).name
        except ValueError:
            level = record.levelno

        # Find caller from where originated the logged message.
        frame, depth = inspect.currentframe(), 0
        while frame and (depth == 0 or frame.f_code.co_filename == logging.__file__):
            frame = frame.f_back
            depth += 1
        logger.opt(depth=depth, exception=record.exc_info).log(level, record.getMessage())


def configure(config: dict, config_loguru: dict) -> None:
    """
    Configure logger.
    """
    for handler in config_loguru["handlers"]:
        handler['filter'] = make_filter(handler['name'])
        del handler['name']
    
    logging.basicConfig(handlers=[InterceptHandler()], level=0, force=True)
    logger.configure(
        handlers = config_loguru["handlers"],
        activation = config_loguru["activation"]
    )


def critical(msg, *args, **kwargs):
    """
    Log a message with severity 'CRITICAL'.
    """
    with_exception = kwargs.get("exc_info", False)
    getLogger('ch-backup').opt(exception=with_exception).critical(msg, *args, **kwargs)


def error(msg, exc_info=False,*args, **kwargs):
    """
    Log a message with severity 'ERROR'.
    """
    with_exception = kwargs.get("exc_info", False)
    getLogger('ch-backup').opt(exception=with_exception).error(msg, *args, **kwargs)



def exception(msg, *args, **kwargs):
    """
    Log a message with severity 'ERROR' with exception information.
    """

    with_exception = kwargs.get("exc_info", False)
    getLogger('ch-backup').opt(exception=with_exception).debug(msg, *args, **kwargs)


def warning(msg, exc_info=False, *args, **kwargs):
    """
    Log a message with severity 'WARNING'.
    """
    with_exception = kwargs.get("exc_info", False)
    getLogger('ch-backup').opt(exception=with_exception).warning(msg, *args, **kwargs)


def info(msg, *args, **kwargs):
    """
    Log a message with severity 'INFO'.
    """
    with_exception = kwargs.get("exc_info", False)
    getLogger('ch-backup').opt(exception=with_exception).info(msg, *args, **kwargs)


def debug(msg, *args, **kwargs):
    """
    Log a message with severity 'DEBUG'.
    """
    with_exception = kwargs.get("exc_info", False)
    getLogger('ch-backup').opt(exception=with_exception).debug(msg, *args, **kwargs)


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
