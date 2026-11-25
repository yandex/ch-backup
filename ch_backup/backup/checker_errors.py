"""Classify failures while checking backup storage."""

from http.client import HTTPException

from botocore.exceptions import BotoCoreError, ClientError
from requests.exceptions import RequestException
from urllib3.exceptions import HTTPError

from ch_backup.exceptions import BackupCheckError, ConfigurationError
from ch_backup.storage.engine.s3 import S3RetryingError


def raise_if_check_failed(
    error: BaseException, operation: str = "check backups"
) -> None:
    """Report an operational error without classifying the backup as broken."""
    if message := check_failure(error, operation):
        raise BackupCheckError(message) from None


def check_failure(error: BaseException, operation: str) -> str | None:
    """Describe an operational failure without including credentials."""
    cause: BaseException | None = error
    while cause is not None:
        if isinstance(cause, BackupCheckError):
            return str(cause)
        if isinstance(cause, ConfigurationError):
            return f"Could not {operation}: {type(cause).__name__} (check local configuration)"
        if isinstance(cause, ClientError):
            code = cause.response.get("Error", {}).get("Code")
            if code not in ("404", "NoSuchKey", "NotFound"):
                return f"Could not {operation}: S3 {code or 'request failed'}"
        elif isinstance(
            cause,
            (
                S3RetryingError,
                BotoCoreError,
                RequestException,
                HTTPError,
                HTTPException,
            ),
        ):
            reason = type(cause).__name__
            if cause.__cause__ is not None:
                reason += f" ({type(cause.__cause__).__name__})"
            return f"Could not {operation}: {reason}"
        elif isinstance(cause, OSError) and cause.errno is not None:
            reason = cause.strerror or type(cause).__name__
            return f"Could not {operation}: {reason}"
        cause = cause.__cause__
    return None


def describe(error: BaseException) -> str:
    """
    Return the root cause of an error in a single line.
    """
    raise_if_check_failed(error)
    while error.__cause__ is not None:
        error = error.__cause__
    message = (str(error).strip().splitlines() or [""])[0]
    return f"{type(error).__name__}: {message}"
