"""
Masking of secrets in ClickHouse statements before they reach the log.
"""

import re

_HIDDEN = "'[HIDDEN]'"
_QUOTES = "'`"
_BRACKETS_OPEN = "({"
_BRACKETS_CLOSE = ")}"
_NAMED_COLLECTION_RE = re.compile(
    r"\s*(?P<action>CREATE|ALTER)\s+NAMED\s+COLLECTION\b", re.IGNORECASE
)
_OVERRIDABLE_RE = re.compile(r"\s+(?:NOT\s+)?OVERRIDABLE\s*$", re.IGNORECASE)
_KEYWORD_RE = {
    keyword: re.compile(rf"\b{keyword}\b", re.IGNORECASE)
    for keyword in ("AS", "SET", "DELETE")
}
_VALUES_KEYWORD = {"CREATE": "AS", "ALTER": "SET"}


def mask_sql_literals(sql: str) -> str:
    """
    Mask SQL literals.

    Values of a named collection are masked regardless of their type, the same way
    ClickHouse does it. Anything that cannot be parsed unambiguously is masked as a
    whole.
    """
    named_collection = _NAMED_COLLECTION_RE.match(sql)
    if not named_collection:
        return _mask_literals(sql)

    keyword = _VALUES_KEYWORD[named_collection.group("action").upper()]
    values_start = _find_keyword(sql, keyword)
    if values_start is None:
        return _mask_literals(sql)

    masked = _mask_values(sql[values_start:])
    if masked is None:
        return sql[:values_start] + f" {_HIDDEN}"
    return sql[:values_start] + masked


def _skip_quoted(sql: str, start: int) -> int | None:
    """
    Return the position right after the quoted string starting at `start`.

    Follows the ClickHouse lexer: both a backslash and a doubled quote escape
    the quote character.
    """
    quote = sql[start]
    index = start + 1
    while index < len(sql):
        if sql[index] == "\\":
            index += 2
            continue
        if sql[index] == quote:
            index += 1
            if index < len(sql) and sql[index] == quote:
                index += 1
                continue
            return index
        index += 1
    return None


def _mask_literals(sql: str) -> str:
    result: list[str] = []
    plain_start = 0
    index = 0
    while index < len(sql):
        if sql[index] not in _QUOTES:
            index += 1
            continue

        result.append(sql[plain_start:index])
        end = _skip_quoted(sql, index)
        if end is None:
            result.append(_HIDDEN)
            return "".join(result)

        result.append(_HIDDEN if sql[index] == "'" else sql[index:end])
        plain_start = end
        index = end

    result.append(sql[plain_start:])
    return "".join(result)


def _find_keyword(sql: str, keyword: str) -> int | None:
    index = 0
    while index < len(sql):
        if sql[index] in _QUOTES:
            end = _skip_quoted(sql, index)
            if end is None:
                return None
            index = end
            continue
        if _KEYWORD_RE[keyword].match(sql, index):
            return index + len(keyword)
        index += 1
    return None


def _hide_value(value: str) -> str:
    overridable = _OVERRIDABLE_RE.search(value)
    return f" {_HIDDEN}" + (overridable.group(0).rstrip() if overridable else "")


def _mask_values(values: str) -> str | None:
    result: list[str] = []
    depth = 0
    pending_start = 0
    in_value = False
    index = 0
    while index < len(values):
        char = values[index]
        if char in _QUOTES:
            end = _skip_quoted(values, index)
            if end is None:
                return None
            index = end
            continue
        if char in _BRACKETS_OPEN:
            depth += 1
        elif char in _BRACKETS_CLOSE:
            depth -= 1
            if depth < 0:
                return None
        elif depth == 0:
            if in_value and _KEYWORD_RE["DELETE"].match(values, index):
                result.append(_hide_value(values[pending_start:index]))
                result.append(" " + _mask_literals(values[index:]))
                return "".join(result)
            if char == "=" and not in_value:
                result.append(values[pending_start : index + 1])
                pending_start = index + 1
                in_value = True
            elif char == ",":
                if not in_value:
                    return None
                result.append(_hide_value(values[pending_start:index]))
                result.append(",")
                pending_start = index + 1
                in_value = False
        index += 1

    if depth != 0 or not in_value:
        return None

    result.append(_hide_value(values[pending_start:]))
    return "".join(result)
