"""
Suppressing pipeline exception auxiliary function.
"""

from typing import Any, Callable, Sequence, Type, TypeVar

from ch_backup import logging

ExceptionT = TypeVar("ExceptionT", bound=Exception)
ExceptionTypes = Sequence[Type[ExceptionT]]


def suppress_exceptions(func: Callable, exceptions: ExceptionTypes = ()) -> Any:
    """
    Invoke function and catch specified exception types and log it, others are re-raised.
    """
    try:
        return func()
    except tuple(exceptions) as e:
        logging.debug(f"Exception was suppressed while running pipeline: {e}")
