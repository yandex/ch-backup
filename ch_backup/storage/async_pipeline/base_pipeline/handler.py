"""
Abstract base classes for stage handlers.
"""

from abc import ABC, abstractmethod
from typing import Any, Iterable, Optional


class Handler(ABC):
    """
    Process data values from pipeline and produce optional single value.
    """

    @abstractmethod
    def __call__(self, value: Any, index: int) -> Optional[Any]:
        """
        Process value at number index from pipeline and optionally produce result.

        None value is skipped and not passed to pipeline.
        """
        pass

    def on_start(self) -> Optional[Any]:
        """
        Executed on starting pipeline and return optional value to pipeline.

        None value is skipped and not passed to pipeline.
        """
        pass

    def on_done(self) -> Optional[Any]:
        """
        Executed when all previous stages are done and return optional value to pipeline.

        None value is skipped and not passed to pipeline.
        """
        pass


class IterableHandler(ABC):
    """
    Process data values from pipeline and produce optional iterable value.
    """

    @abstractmethod
    def __call__(self, value: Any, index: int) -> Optional[Iterable[Any]]:
        """
        Process value at number index from pipeline and optionally produce iterable result.

        None value is skipped and not passed to pipeline.
        """
        pass

    def on_start(self) -> Optional[Iterable[Any]]:
        """
        Executed on starting pipeline and return optional iterable value to pipeline.

        None value is skipped and not passed to pipeline.
        """
        pass

    def on_done(self) -> Optional[Iterable[Any]]:
        """
        Executed when all previous stages are done and return optional iterable value to pipeline.

        None value is skipped and not passed to pipeline.
        """
        pass


class InputHandler(ABC):
    """
    Produce iterable data only.
    """

    @abstractmethod
    def __call__(self) -> Optional[Iterable[Any]]:
        """
        Executed on starting pipeline (but after on_start) and return optional iterable value to pipeline.

        None value is skipped and not passed to pipeline.
        """
        pass

    def on_start(self) -> Optional[Any]:
        """
        Executed on starting pipeline and return optional iterable value to pipeline.

        None value is skipped and not passed to pipeline.
        """
        pass

    def on_done(self) -> Optional[Any]:
        """
        Executed when all previous stages are done and return optional iterable value to pipeline.

        None value is skipped and not passed to pipeline.
        """
        pass
