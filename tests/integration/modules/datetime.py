"""
Utility functions for dealing with date and time.
"""
from datetime import datetime, timedelta

from humanfriendly import parse_timespan


def increase_time_str(time_str: str, timespan_str: str) -> str:
    """
    Increase time string on the specified timespan.
    """
    return _shift_time_str(time_str, parse_timespan(timespan_str))


def decrease_time_str(time_str: str, timespan_str: str) -> str:
    """
    Decrease time string on the specified timespan.
    """
    return _shift_time_str(time_str, -parse_timespan(timespan_str))


def _shift_time_str(time_str: str, timespan_seconds: float) -> str:
    time_fmt = "%Y-%m-%d %H:%M:%S.%f %z"

    in_time = datetime.strptime(time_str, time_fmt)

    timespan = timedelta(seconds=timespan_seconds)

    return (in_time + timespan).strftime(time_fmt)
