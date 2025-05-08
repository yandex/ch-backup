"""
Utility functions.
"""

import logging
import re
import string
from functools import wraps
from random import choice as random_choise
from types import SimpleNamespace
from typing import Mapping, MutableMapping, MutableSequence

from packaging.version import parse as parse_version

from .typing import ContextT


def merge(original, update):
    """
    Recursively merge update dict into original.
    """
    for key in update:
        recurse_conditions = [
            key in original,
            isinstance(original.get(key), MutableMapping),
            isinstance(update.get(key), Mapping),
        ]
        if all(recurse_conditions):
            merge(original[key], update[key])
        else:
            original[key] = update[key]
    return original


def format_object(obj, **replacements):
    """
    Replace format placeholders with actual values
    """
    if isinstance(obj, str):
        obj = obj.format(**replacements)
    elif isinstance(obj, MutableMapping):
        for key, value in obj.items():
            obj[key] = format_object(value, **replacements)
    elif isinstance(obj, MutableSequence):
        for idx, val in enumerate(obj):
            obj[idx] = format_object(val, **replacements)
    return obj


def env_stage(event, fail=False):
    """
    Nicely logs env stage.
    """

    def wrapper(fun):
        @wraps(fun)
        def _wrapped_fun(*args, **kwargs):
            stage_name = f"{fun.__module__}.{fun.__name__}"
            logging.info("initiating %s stage %s", event, stage_name)
            try:
                return fun(*args, **kwargs)
            except Exception as e:
                logging.error("%s failed: %s", stage_name, e)
                if fail:
                    raise

        return _wrapped_fun

    return wrapper


def generate_random_string(length: int = 64) -> str:
    """
    Generate random alphanum sequence.
    """
    return "".join(
        random_choise(string.ascii_letters + string.digits) for _ in range(length)
    )


def context_to_dict(context: ContextT) -> dict:
    """
    Convert context to dict representation.

    The context type can be either types.SimpleNamespace or behave.Context.
    """
    if isinstance(context, SimpleNamespace):
        return context.__dict__

    result: dict = {}
    for frame in context._stack:  # pylint: disable=protected-access
        for key, value in frame.items():
            if key not in result:
                result[key] = value

    return result


def normalize_create_query(create_query):
    """
    Normalize create table query for comparison.
    """
    # Override replica parameter for replicated tables. The backup tool does the same.
    match = re.search(
        r"Replicated\S{0,20}MergeTree\('[^']+', (?P<replica>\'\S+\')", create_query
    )
    if match:
        create_query = create_query.replace(match.group("replica"), "'{replica}'")

    return create_query


def version_ge(current_version, comparing_version):
    """
    Return True if `current_version` is greater or equal than `comparing_version`, or False otherwise.
    """
    # "latest" is greater or equal than any known version
    if current_version == "latest":
        return True

    return parse_version(current_version) >= parse_version(comparing_version)  # type: ignore


def version_lt(current_version, comparing_version):
    """
    Return True if `current_version` is less than `comparing_version`, or False otherwise.
    """
    # "latest" is not less than any known version
    if current_version == "latest":
        return False

    return parse_version(current_version) < parse_version(comparing_version)  # type: ignore
