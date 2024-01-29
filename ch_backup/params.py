"""
ParamType declarations.
"""

import json
import re
import typing
from collections import defaultdict

from click import ParamType
from click.types import StringParamType
from humanfriendly import InvalidTimespan, parse_timespan


class List(ParamType):
    """
    List type for command-line parameters.
    """

    name = "list"

    def __init__(self, separator=",", regexp=None):
        self.separator = separator
        self.regexp_str = regexp
        self.regexp = re.compile(regexp) if regexp else None

    def convert(self, value, param, ctx):
        """
        Convert input value into list of items.
        """
        try:
            result = list(map(str.strip, value.split(self.separator)))

            if self.regexp:
                for item in result:
                    if self.regexp.fullmatch(item) is None:
                        raise ValueError()

            return result

        except ValueError:
            msg = f'"{value}" is not a valid list of items'
            if self.regexp:
                msg += f" matching the format: {self.regexp_str}"

            self.fail(msg, param, ctx)


KeyValue = typing.Dict[str, str]


class KeyValueList(List):
    """
    List of key-value type for command-line parameters.
    """

    name = "kvlist"

    def __init__(self, kv_separator=":", list_separator=","):
        super().__init__(separator=list_separator)
        self.kv_separator = kv_separator

    def convert(self, value, param, ctx):
        """
        Convert input value into list of key-value.
        """
        result: KeyValue = {}

        try:
            kvs = super().convert(value, param, ctx)

            for kv in kvs:
                k, v = list(map(str.strip, kv.split(self.kv_separator)))
                result[k] = v

            return result

        except ValueError:
            self.fail(f'"{value}" is not a valid list of key-value', param, ctx)


KeyValues = typing.Dict[str, typing.List[str]]


class KeyValuesList(KeyValueList):
    """
    List of key-values type for command-line parameters.
    """

    name = "kvslist"

    def __init__(self, value_separator=",", kv_separator=":", list_separator=";"):
        super().__init__(kv_separator=kv_separator, list_separator=list_separator)
        self.value_separator = value_separator

    def convert(self, value, param, ctx):
        """
        Convert input value into list of key-values.
        """
        result: KeyValues = defaultdict(list)

        try:
            kvs: KeyValue = super().convert(value, param, ctx)

            for k in kvs.keys():
                vs = list(map(str.strip, kvs[k].split(self.value_separator)))
                result[k].extend(vs)

            return dict(result)

        except ValueError:
            self.fail(f'"{value}" is not a valid list of key-values', param, ctx)


class String(StringParamType):
    """
    String type for command-line parameters with support of macros and
    regexp-based validation.
    """

    name = "string"

    def __init__(self, regexp=None, macros=None):
        self.regexp_str = regexp
        self.regexp = re.compile(regexp) if regexp else None
        self.macros = macros

    def convert(self, value, param, ctx):
        """
        Parse input value.
        """
        if self.macros:
            for macro, replacement in self.macros.items():
                value = value.replace(macro, replacement)

        if self.regexp:
            if self.regexp.fullmatch(value) is None:
                msg = f'"{value}" does not match the format: {self.regexp_str}'
                self.fail(msg, param, ctx)

        return super().convert(value, param, ctx)


class TimeSpan(ParamType):
    """
    TimeSpan type for command-line parameters.
    """

    name = "TimeSpan"

    def convert(self, value, param, ctx):
        """
        Convert timespan string into seconds.
        """
        try:
            return int(parse_timespan(value))
        except InvalidTimespan as e:
            self.fail(f'"{value}" is not a valid timespan: {str(e)}', param, ctx)


class JsonParamType(ParamType):
    """
    JsonParamType type for command-line parameter for JSON value.
    """

    name = "json"

    def convert(self, value, param, ctx):
        try:
            if re.fullmatch(
                r'\s*([\[{"].*|true|false|null|\d+(\.\d+)?)\s*',
                value,
                re.MULTILINE | re.DOTALL,
            ):
                return json.loads(value)
            return value.strip()
        except json.JSONDecodeError:
            self.fail(f'"{value}" is not a valid json value', param, ctx)
