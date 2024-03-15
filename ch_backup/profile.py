"""
profile module to collect memory usage info.
"""

import os
import time
import tracemalloc
from functools import partial, wraps

from . import logging


class ProfileDecorator:
    """
    Class ProfileDecorator memory profiling.
    """

    next_snapshot = 0

    def __init__(self, func, limit, interval):
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
                tracemalloc.start()
        result = self.func(*args, **kwargs)
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

        logging.info("TraceMalloc top {} lines", self.limit)
        for index, stat in enumerate(top_stats[: self.limit], 1):
            logging.info("  #{}: {} B", index, stat.size)
            for line in stat.traceback.format():
                logging.info("    {}", line)
        other = top_stats[self.limit :]
        if other:
            size = sum(stat.size for stat in other)
            logging.info("    {} other: {} B", len(other), size)
        total = sum(stat.size for stat in top_stats)
        logging.info("    Total allocated size: {} B", total)


def profile(limit, interval=0):
    """
    Decorator to profile memory usage after function completed.
    """

    def decorator(func):
        td = ProfileDecorator(func=func, limit=limit, interval=interval)
        return wraps(func)(td)

    return decorator
