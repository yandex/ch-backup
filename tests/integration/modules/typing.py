"""
Type definitions.
"""

from types import SimpleNamespace

from behave.runner import Context

ContextT = Context | SimpleNamespace
