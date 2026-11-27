"""
Errors specific to ch-backup.
"""

from concurrent.futures import BrokenExecutor
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Iterator


def is_broken_executor(error: BaseException) -> bool:
    """Recognize executor failures also wrapped by storage operations."""
    cause: BaseException | None = error
    while cause is not None:
        if isinstance(cause, BrokenExecutor):
            return True
        cause = cause.__cause__
    return False


class ClickhouseBackupError(Exception):
    """
    Base class for ch-backup related errors.
    """


class BackupCheckError(ClickhouseBackupError):
    """The backup check could not finish."""


class StorageError(ClickhouseBackupError):
    """
    Storage layer error (e.g. save or load of data failed).
    """


class ConfigurationError(ClickhouseBackupError):
    """
    Configuration errors (e.g. invalid value of configuration parameter).
    """


class UnknownEncryptionError(ConfigurationError):
    """
    Invalid encryption type.
    """


class BackupNotFound(ClickhouseBackupError):
    """
    Backup doesn't exist.
    """

    def __init__(self, name):
        super().__init__(f"Backup {name} not found.")


class InvalidBackupStruct(ClickhouseBackupError):
    """
    Invalid backup metadata.
    """


class UnknownBackupStateError(ClickhouseBackupError):
    """
    Invalid backup state.
    """


class TerminatingSignal(BaseException):
    """
    Raised in a terminating signal handler.

    Derived from BaseException to avoid unintentional catching,
    for example, by retrying operations.
    """


_deferred_signals: ContextVar[list[int] | None] = ContextVar(
    "deferred_signals", default=None
)


def interrupt(signum: int) -> None:
    """Deliver a termination signal after an active staging cleanup has finished."""
    pending = _deferred_signals.get()
    if pending is None:
        raise TerminatingSignal(f"Execution was interrupted by the signal {signum}")
    if not pending:
        pending.append(signum)


@contextmanager
def defer_termination_signals() -> Iterator[None]:
    """Keep repeated termination signals from interrupting a critical drain."""
    pending: list[int] = []
    token = _deferred_signals.set(pending)
    try:
        yield
    finally:
        _deferred_signals.reset(token)
        if pending:
            interrupt(pending[0])
