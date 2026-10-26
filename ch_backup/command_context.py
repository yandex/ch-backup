"""Current CLI command context, including propagation to worker processes."""

from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any, Callable, Generator

_current_command: ContextVar[str | None] = ContextVar("current_command", default=None)


def get_current_command() -> str | None:
    """Return the name of the command currently being executed."""
    return _current_command.get()


@contextmanager
def command_context(command: str | None) -> Generator[None, None, None]:
    """Make a command name available to all operations in the scope."""
    token = _current_command.set(command)
    try:
        yield
    finally:
        _current_command.reset(token)


def run_with_command_context(command: str | None, func: Callable[[], Any]) -> Any:
    """Run a callable in its command context, including in a spawned worker."""
    with command_context(command):
        return func()
