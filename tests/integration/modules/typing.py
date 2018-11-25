"""
Type definitions.
"""
from types import SimpleNamespace
from typing import Union

from behave.runner import Context

ContextT = Union[Context, SimpleNamespace]
