"""
S3 retry helper metaclass.
"""
from abc import ABCMeta
from functools import wraps
from http.client import HTTPException
from inspect import isfunction
from random import uniform
from threading import Lock
from time import sleep
from typing import TYPE_CHECKING, Any, Callable, TypeVar, Union

from botocore.exceptions import BotoCoreError, ClientError
from urllib3.exceptions import HTTPError

if TYPE_CHECKING:
    from ch_backup.storage.engine import (
        S3StorageEngine,  # pylint: disable=wrong-import-position
    )

RT = TypeVar("RT")


class S3RetryingError(Exception):
    """
    Exception indicates that interaction with S3 can be retried.
    """


class S3RetryMeta(ABCMeta):
    """
    Metaclass to wrap all methods of S3StorageEngine with retry mechanism in case of S3 endpoint errors.
    S3 client instance is recreated in case of some errors.
    """

    def __new__(mcs, name, bases, attrs):  # pylint: disable=arguments-differ
        new_attrs = {}
        for attr_name, attr_value in attrs.items():
            if not attr_name.startswith("_") and isfunction(attr_value):
                attr_value = mcs.retry_wrapper(attr_value)
            new_attrs[attr_name] = attr_value

        return super().__new__(mcs, name, bases, new_attrs)

    @classmethod
    def retry_wrapper(mcs, func: Callable[..., RT]) -> Callable[..., RT]:
        """
        Generates retry-wrapper for given function.
        """

        @wraps(func)
        @retry_exponential(
            max_attempts=30, max_interval=180, exception_types=S3RetryingError
        )
        def wrapper(self: "S3StorageEngine", *args: Any, **kwargs: Any) -> RT:
            try:
                return func(self, *args, **kwargs)
            except (ClientError, BotoCoreError, HTTPException, HTTPError) as e:
                self._s3_client_factory.reset()  # pylint: disable=protected-access
                raise S3RetryingError(f"Failed to make S3 operation: {str(e)}") from e

        return wrapper


def retry_exponential(
    exception_types: Union[type, tuple] = Exception,
    max_attempts: int = 5,
    max_interval: float = 5,
    multiplier: float = 0.5,
) -> Callable:
    """
    Decorator for thread safe retry logic with expential wait time.
    """

    class RetryExponential:
        """
        Exponential retry.
        """

        _lock = Lock()
        _retry_count = 0

        def calculate_sleep_time(self, retry_count):
            """
            Calculate time to sleep.
            """
            low_value = 0

            high_value = min(max_interval, 2**retry_count) * multiplier
            return uniform(low_value, min(high_value, max_interval))  # nosec

        def call(self, fn, *args, **kwargs):
            """
            Retry logic. If we execute the inner function with an exception then try again
            until attempts less than max_attempts.
            """
            attempts = 0

            while attempts < max_attempts:
                try:
                    res = fn(*args, **kwargs)
                    return res
                except Exception as exc:
                    # Unknown exception type
                    if not isinstance(exc, exception_types):
                        raise exc

                    attempts += 1
                    # No attempts left.
                    if attempts == max_attempts:
                        raise exc

                    with RetryExponential._lock:
                        retry_count = RetryExponential._retry_count
                        RetryExponential._retry_count += 1
                    sleep(self.calculate_sleep_time(retry_count))

    def wrap(f):
        @wraps(f)
        def wrapped_f(*args, **kw):
            return RetryExponential().call(f, *args, **kw)

        return wrapped_f

    return wrap
