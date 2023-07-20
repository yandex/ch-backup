"""
S3 retry helper metaclass.
"""
from abc import ABCMeta
from functools import wraps
from http.client import HTTPException
from inspect import isfunction
from typing import TYPE_CHECKING, Any, Callable, TypeVar

from botocore.exceptions import BotoCoreError, ClientError
from ch_backup.util import retry
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
        @retry(max_attempts=30, max_interval=180, exception_types=S3RetryingError)
        def wrapper(self: "S3StorageEngine", *args: Any, **kwargs: Any) -> RT:
            try:
                return func(self, *args, **kwargs)
            except (ClientError, BotoCoreError, HTTPException, HTTPError) as e:
                self._s3_client_factory.reset()  # pylint: disable=protected-access
                raise S3RetryingError(f"Failed to make S3 operation: {str(e)}") from e

        return wrapper
