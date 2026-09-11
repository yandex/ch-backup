"""
Masking of secrets in ClickHouse statements before they reach the log.
"""

import re

_HIDDEN = "'[HIDDEN]'"
_QUOTES = "'`"
_NAMED_COLLECTION_RE = re.compile(
    r"\s*CREATE\s+NAMED\s+COLLECTION\b.*?\bAS\b", re.IGNORECASE | re.DOTALL
)


def mask_named_collection(sql: str) -> str | None:
    """
    Hide everything a named collection statement declares, keeping its header.

    Keys of a named collection are chosen by the user, so there is no fixed set of
    names to look for. ClickHouse hides every value of such a statement, and so do we.

    Returns None if the statement declares nothing and thus has nothing to hide.
    """
    header = _NAMED_COLLECTION_RE.match(sql)
    if header is None:
        return None
    return f"{header.group()} {_HIDDEN}"


def mask_sql_literals(sql: str) -> str:
    """
    Replace string literals with a placeholder, keeping quoted identifiers.
    """
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
