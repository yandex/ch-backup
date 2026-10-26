"""
S3 retry helper metaclass.
"""

import ssl
from abc import ABCMeta
from enum import Enum
from functools import wraps
from http.client import HTTPException
from inspect import isfunction
from random import uniform
from time import sleep
from typing import TYPE_CHECKING, Any, Callable, TypeVar

import requests
from botocore.exceptions import BotoCoreError, ClientError
from urllib3.exceptions import HTTPError
from urllib3.util.ssl_match_hostname import CertificateError as Urllib3CertificateError

from ch_backup import logging
from ch_backup.command_context import get_current_command
from ch_backup.exceptions import ConfigurationError
from ch_backup.storage.engine.s3.s3_client_factory import S3BalancerUnknownHost

if TYPE_CHECKING:
    from ch_backup.storage.engine import (
        S3StorageEngine,  # pylint: disable=wrong-import-position
    )

RT = TypeVar("RT")


class S3RetryPolicy(str, Enum):
    """S3 retry configuration selected for a ch-backup command."""

    DEFAULT = "default"
    LIST = "list"
    BACKUP = "backup"
    RESTORE = "restore"


_COMMAND_RETRY_POLICIES = {
    "list": S3RetryPolicy.LIST,
    "show": S3RetryPolicy.LIST,
    "backup": S3RetryPolicy.BACKUP,
    "copy": S3RetryPolicy.BACKUP,
    "restore": S3RetryPolicy.RESTORE,
    "restore-from-replica": S3RetryPolicy.RESTORE,
    "restore-access-control": S3RetryPolicy.RESTORE,
    "get-cloud-storage-metadata": S3RetryPolicy.RESTORE,
}


def get_s3_retry_policy() -> S3RetryPolicy:
    """Select an S3 retry policy for the current command."""
    command = get_current_command()
    if command is None:
        return S3RetryPolicy.DEFAULT
    return _COMMAND_RETRY_POLICIES.get(command, S3RetryPolicy.DEFAULT)


def resolve_s3_retry_config(
    config: dict, retry_policy: S3RetryPolicy | None = None
) -> tuple[int, float]:
    """Resolve and validate an S3 retry policy with legacy value fallbacks."""
    retry_policy = retry_policy or get_s3_retry_policy()
    retry_config = config.get("retry", {})
    if not isinstance(retry_config, dict):
        raise ConfigurationError("storage.retry must be a mapping")

    policy_config = retry_config.get(retry_policy.value, {})
    if not isinstance(policy_config, dict):
        raise ConfigurationError(
            f"storage.retry.{retry_policy.value} must be a mapping"
        )

    max_attempts = policy_config.get(
        "max_attempts", config.get("retry_max_attempts", 10)
    )
    max_interval = policy_config.get(
        "max_interval", config.get("retry_max_interval", 60)
    )
    attempts_valid = (
        isinstance(max_attempts, int)
        and not isinstance(max_attempts, bool)
        and max_attempts >= 1
    )
    interval_valid = (
        isinstance(max_interval, (int, float))
        and not isinstance(max_interval, bool)
        and max_interval >= 0
    )
    if not attempts_valid or not interval_valid:
        if retry_policy == S3RetryPolicy.DEFAULT:
            raise ConfigurationError(
                "storage.retry_max_attempts must be an integer of at least 1 "
                "and storage.retry_max_interval must not be negative"
            )
        raise ConfigurationError(
            f"storage.retry.{retry_policy.value}: max_attempts must be "
            "an integer of at least 1 and max_interval must not be negative"
        )
    return max_attempts, max_interval


def validate_s3_retry_config(config: dict) -> None:
    """Validate base values and every command-specific S3 retry policy."""
    for retry_policy in S3RetryPolicy:
        resolve_s3_retry_config(config, retry_policy)


def is_retryable_s3_error(error: ClientError) -> bool:
    """Classify S3 responses while retaining retries for timeout/throttling 4xx."""
    status = error.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    code = error.response.get("Error", {}).get("Code")
    # 507 can mean object data unavailable for days; short retries cannot help.
    if status in (404, 507) or code in (
        "404",
        "NoSuchKey",
        "NotFound",
        "507",
        "InsufficientStorage",
        "AccessDenied",
        "InvalidAccessKeyId",
        "SignatureDoesNotMatch",
        "ExpiredToken",
        "InvalidToken",
        "NoSuchBucket",
        "MalformedXML",
        "InvalidArgument",
        "InvalidRequest",
        "AuthorizationHeaderMalformed",
    ):
        return False
    if code in (
        "RequestTimeout",
        "RequestTimeoutException",
        "SlowDown",
        "Throttling",
        "ThrottlingException",
        "ThrottledException",
        "RequestThrottled",
        "RequestThrottledException",
        "RequestLimitExceeded",
        "TooManyRequestsException",
        "PriorRequestNotComplete",
    ):
        return True
    return status is None or status in (408, 429) or status >= 500


def is_certificate_error(error: BaseException) -> bool:
    """Find verification failures inside Requests/urllib3/botocore wrappers."""
    pending = [error]
    seen: set[int] = set()
    while pending:
        current = pending.pop()
        if id(current) in seen:
            continue
        seen.add(id(current))
        if isinstance(current, (ssl.CertificateError, Urllib3CertificateError)):
            return True
        # Requests keeps urllib3 errors in args; urllib3 keeps the cause in reason.
        # Botocore also stores the original exception in kwargs['error'].
        nested = (
            *current.args,
            current.__cause__,
            current.__context__,
            getattr(current, "reason", None),
            getattr(current, "kwargs", {}).get("error"),
        )
        pending.extend(item for item in nested if isinstance(item, BaseException))
    return False


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
                attr_value = mcs.retry_wrapper(mcs.s3_exception_wrapper(attr_value))
            new_attrs[attr_name] = attr_value

        return super().__new__(mcs, name, bases, new_attrs)

    @classmethod
    def retry_wrapper(mcs, func: Callable[..., RT]) -> Callable[..., RT]:
        """
        Retries the function with the retry limits of the engine it is called on.
        """

        @wraps(func)
        def wrapper(self: "S3StorageEngine", *args: Any, **kwargs: Any) -> RT:
            # pylint: disable=protected-access
            return self._retry(func, self, *args, **kwargs)

        return wrapper

    @classmethod
    def s3_exception_wrapper(mcs, func: Callable[..., RT]) -> Callable[..., RT]:
        """
        Generates s3-exception-wrapper for given function.
        """

        @wraps(func)
        def wrapper(self: "S3StorageEngine", *args: Any, **kwargs: Any) -> RT:
            try:
                return func(self, *args, **kwargs)
            except (
                ClientError,
                BotoCoreError,
                HTTPException,
                HTTPError,
                S3BalancerUnknownHost,
                requests.ConnectionError,
                requests.Timeout,
                requests.HTTPError,
                requests.exceptions.ChunkedEncodingError,
            ) as e:
                if isinstance(e, ClientError) and not is_retryable_s3_error(e):
                    raise
                if is_certificate_error(e):
                    raise
                if isinstance(e, requests.HTTPError) and e.response is not None:
                    code = e.response.status_code
                    if code == 507 or (code not in (408, 429) and code < 500):
                        raise
                self._s3_client_factory.reset()  # pylint: disable=protected-access
                raise S3RetryingError(f"Failed to make S3 operation: {str(e)}")

        return wrapper


# pylint: disable=too-many-positional-arguments
def retry(
    exception_types: type | tuple = Exception,
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

    # pylint: disable=too-many-positional-arguments
    def __init__(
        self,
        exception_types: type | tuple = Exception,
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
        Follow https://aws.amazon.com/ru/blogs/architecture/exponential-backoff-and-jitter/
        """

        high_value = (2**attempt) * self.multiplier
        return uniform(0, min(high_value, self.max_interval))  # nosec

    def __call__(self, fn, *args, **kwargs):
        """
        Retry logic. If we execute the inner function with an exception then try again
        until attempts less than max_attempts.

        We don't update state of the instance variables and use only local ones
        therefore we can use it from the multiple threads.
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
