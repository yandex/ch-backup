"""
S3 retry helper metaclass.
"""
from abc import ABCMeta
from functools import wraps
from http.client import HTTPException
from inspect import isfunction
from random import uniform
from time import sleep
from typing import TYPE_CHECKING, Any, Callable, TypeVar, Union

from botocore.exceptions import BotoCoreError, ClientError
from urllib3.exceptions import HTTPError

from ch_backup import logging

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
        retry_wrapper = retry(
            max_attempts=30, max_interval=180, exception_types=S3RetryingError
        )
        for attr_name, attr_value in attrs.items():
            if not attr_name.startswith("_") and isfunction(attr_value):
                attr_value = retry_wrapper(mcs.s3_exception_wrapper(attr_value))
            new_attrs[attr_name] = attr_value

        return super().__new__(mcs, name, bases, new_attrs)

    @classmethod
    def s3_exception_wrapper(mcs, func: Callable[..., RT]) -> Callable[..., RT]:
        """
        Generates s3-exception-wrapper for given function.
        """

        @wraps(func)
        def wrapper(self: "S3StorageEngine", *args: Any, **kwargs: Any) -> RT:
            try:
                return func(self, *args, **kwargs)
            except (ClientError, BotoCoreError, HTTPException, HTTPError) as e:
                self._s3_client_factory.reset()  # pylint: disable=protected-access
                raise S3RetryingError(f"Failed to make S3 operation: {str(e)}") from e

        return wrapper


def retry(
    exception_types: Union[type, tuple] = Exception,
    max_attempts: int = 5,
    max_interval: float = 5,
    multiplier: float = 0.5,
    sleep_function: Callable = sleep,
    verbose: bool = True,
) -> Callable:
    """
    Decorator for thread safe retry logic with exponential wait time.
    """
    retry_exponential = RetryExponential(
        exception_types, max_attempts, max_interval, multiplier, sleep_function, verbose
    )

    def wrap(f):
        @wraps(f)
        def wrapped_f(*args, **kwargs):
            return retry_exponential(f, *args, **kwargs)

        return wrapped_f

    return wrap


class RetryExponential:
    """
    Exponential retry.
    """

    def __init__(
        self,
        exception_types: Union[type, tuple] = Exception,
        max_attempts: int = 5,
        max_interval: float = 5,
        multiplier: float = 0.5,
        sleep_function: Callable = sleep,
        verbose: bool = True,
    ) -> None:
        self.exception_types = exception_types
        self.max_attempts = max_attempts
        self.max_interval = max_interval
        self.multiplier = multiplier
        self.sleep_function = sleep_function
        self.verbose = verbose

    def calculate_sleep_time(self, attempt):
        """
        Calculate time to sleep.
        """

        high_value = (2**attempt) * self.multiplier
        return uniform(0, min(high_value, self.max_interval))  # nosec

    def __call__(self, fn, *args, **kwargs):
        """
        Retry logic. If we execute the inner function with an exception then try again
        until attempts less than max_attempts.
        """
        attempts = 0

        while attempts < self.max_attempts:
            try:
                res = fn(*args, **kwargs)
                return res
            except Exception as exc:
                # Unknown exception type
                if not isinstance(exc, self.exception_types):
                    raise exc

                attempts += 1
                # No attempts left.
                if attempts == self.max_attempts:
                    raise exc

                time_to_sleep = self.calculate_sleep_time(attempts)
                if self.verbose:
                    logging.debug(
                        "Exponential retry {}.{} in {}, attempt: {}, reason: {}",
                        fn.__module__,
                        fn.__qualname__,
                        time_to_sleep,
                        attempts,
                        exc,
                    )

                self.sleep_function(time_to_sleep)
