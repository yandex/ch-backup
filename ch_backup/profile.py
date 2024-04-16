"""
profile module to collect memory usage info.
"""

import os
import time
import tracemalloc
from functools import partial, wraps

from . import logging
from .formatting import format_size


class ProfileDecorator:
    """
    Class ProfileDecorator memory profiling.
    """

    next_snapshot = 0

    def __init__(self, func, limit, interval):
        """
        func - function to decorate, snapshot is taken after function execution.
        limit - write to log top 'limit' memory usages.
        interval - minimum time interval between snapshots in seconds (per process).
                   Making snapshot takes 5-10 seconds, so often calls can
                   drammaticaly reduce performance.
                   0 for snapshot on every call (for single called functions)
        """
        self.func = func
        self.limit = limit
        self.interval = interval

    def __get__(self, obj, objtype=None):
        if obj is None:
            return self.func
        return partial(self, obj)

    def __call__(self, *args, **kwargs):
        use_tracemalloc = int(os.environ.get("PYTHONTRACEMALLOC", "0"))
        if use_tracemalloc > 0:
            if not tracemalloc.is_tracing():
                tracemalloc.start(use_tracemalloc)
        try:
            result = self.func(*args, **kwargs)
        finally:
            if use_tracemalloc > 0:
                if not self.interval:
                    snapshot = tracemalloc.take_snapshot()
                    self.log_top(snapshot)
                else:
                    now = time.time()
                    if now >= ProfileDecorator.next_snapshot:
                        ProfileDecorator.next_snapshot = (
                            ProfileDecorator.next_snapshot
                            if ProfileDecorator.next_snapshot
                            else now
                        ) + self.interval
                        snapshot = tracemalloc.take_snapshot()
                        self.log_top(snapshot)
        return result

    def log_top(self, snapshot):
        """
        Write info form snapshot to log.
        Based on https://docs.python.org/3/library/tracemalloc.html#pretty-top
        """

        snapshot = snapshot.filter_traces(
            (
                tracemalloc.Filter(False, "<frozen importlib._bootstrap>"),
                tracemalloc.Filter(False, "<unknown>"),
            )
        )
        top_stats = snapshot.statistics("traceback")

        logging.debug("TraceMalloc top {} lines", self.limit)
        for index, stat in enumerate(top_stats[: self.limit], 1):
            logging.debug("  #{}: {}", index, format_size(stat.size))
            for line in stat.traceback.format():
                logging.debug("    {}", line)
        other = top_stats[self.limit :]
        if other:
            size = sum(stat.size for stat in other)
            logging.debug("    {} other: {}", len(other), format_size(size))
        total = sum(stat.size for stat in top_stats)
        logging.debug("    Total allocated size: {}", format_size(total))


def profile(limit=10, interval=0):
    """
    Decorator to profile memory usage after function completed.
    """

    def decorator(func):
        td = ProfileDecorator(func=func, limit=limit, interval=interval)
        return wraps(func)(td)

    return decorator
