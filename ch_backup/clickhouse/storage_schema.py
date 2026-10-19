"""Adapt storage policy and table TTL clauses in ClickHouse-generated DDL.

The two rewrites leave expressions, column definitions and SELECT queries intact.
Regular expressions search a copy with quoted text, comments and bracket contents
blanked out; replacements use the corresponding spans of the original SQL.
"""

import re
from dataclasses import dataclass

from ch_backup.exceptions import ClickhouseBackupError

# Keep literals available for reading policy/destination names, but hide quoted
# text and comments when searching for SQL keywords and separators.
_STRING = re.compile(r"'(?:\\.|''|[^'\\])*'", re.DOTALL)
_QUOTED_OR_COMMENT = re.compile(
    r"""'(?:\\.|''|[^'\\])*'|"(?:\\.|""|[^"\\])*"|`(?:\\.|``|[^`\\])*`
        |--[^\n]*|\#[^\n]*|/\*.*?\*/""",
    re.VERBOSE | re.DOTALL,
)
_INNER_BRACKETS = re.compile(r"\([^()[\]]*\)|\[[^()[\]]*\]")
# Engine and clause boundaries are searched in masked ClickHouse-generated DDL,
# so a keyword inside an expression cannot be mistaken for a clause boundary.
_MERGE_TREE_ENGINE = re.compile(
    r"\bENGINE\s*=?\s*(?P<engine>\w*MergeTree)\b", re.IGNORECASE
)
_POLICY_SETTING = re.compile(
    r"""(?:\bstorage_policy\b|`storage_policy`|"storage_policy")\s*(?P<equals>=)\s*""",
    re.IGNORECASE,
)
_TTL_MOVE = re.compile(
    r"\bTO\s+(?P<kind>DISK|VOLUME)(?:\s+IF\s+EXISTS)?\b", re.IGNORECASE
)
_ATOM = "\x01"
_EXPRESSION_OPERATORS = {
    "AND",
    "OR",
    "NOT",
    "IN",
    "LIKE",
    "ILIKE",
    "BETWEEN",
    "IS",
    "DISTINCT",
    "FROM",
    "WHEN",
    "THEN",
    "ELSE",
    "WHERE",
    "BY",
    "SET",
    "TO",
}
_PREFIX_OPERATORS = {"NOT", "INTERVAL", "CASE", "WHEN", "THEN", "ELSE"}
_SYMBOL_OPERATORS = set(",.=+-*/%<>!|&^?:")


class StorageSchemaError(ClickhouseBackupError):
    """A storage clause cannot be safely adapted to the target server."""


@dataclass(frozen=True)
class StoragePolicy:
    """Volumes and disks in server policy order."""

    name: str
    volumes: tuple[str, ...]
    disks: tuple[str, ...]
    local_disks: tuple[str, ...]


@dataclass(frozen=True)
class StorageSchemaChange:
    """Result of adapting a table's storage clauses."""

    statement: str
    policy: StoragePolicy
    previous_policy: str
    removed_ttls: tuple[str, ...]

    @property
    def changed(self) -> bool:
        """Whether policy placement or TTL semantics changed."""
        return self.previous_policy != self.policy.name or bool(self.removed_ttls)


def adapt_storage_schema(
    statement: str, policies: dict[str, StoragePolicy], default_policy: str = "default"
) -> StorageSchemaChange | None:
    """Replace a missing policy, then remove MOVE rules unavailable in that policy."""
    engine = _find_merge_tree_engine(statement)
    if engine is None:
        return None

    # Only the storage clause after ENGINE and before a view's AS SELECT is edited.
    start = engine.end()
    end = next(
        (
            start + begin
            for name, begin, _ in _storage_clauses(statement[start:])
            if name in {"AS", "COMMENT", "EMPTY", ";"}
        ),
        len(statement),
    )
    storage = statement[start:end]
    # Resolve the target policy first: a TTL destination must belong to that
    # policy, even if a disk with the same name exists elsewhere on the server.
    storage, policy, previous = _replace_missing_policy(
        storage, policies, default_policy
    )
    storage, removed = _rewrite_table_ttl(storage, policy)
    return StorageSchemaChange(
        statement[:start] + storage + statement[end:], policy, previous, removed
    )


def _replace_missing_policy(
    storage: str, policies: dict[str, StoragePolicy], default_policy: str
) -> tuple[str, StoragePolicy, str]:
    surface = _top_level_sql(storage)
    settings = next(
        (end for name, _, end in _storage_clauses(storage) if name == "SETTINGS"),
        None,
    )
    value = None
    if settings is not None:
        # Search the original setting name to support quoted identifiers, then
        # use the masked copy to check that its '=' is outside any expression.
        for setting in _POLICY_SETTING.finditer(_without_comments(storage), settings):
            # Ignore text resembling an assignment inside a string or expression.
            if surface[setting.start("equals")] != "=":
                continue
            value = _STRING.match(storage, setting.end())
            if value is None:
                raise StorageSchemaError("storage_policy must be a string literal")

    # Without an explicit setting, CREATE uses the target server's engine default.
    # Keep any available policy; fall back to 'default' only when it is missing.
    previous = _unquote_sql_string(value.group()) if value else default_policy
    name = previous if previous in policies else "default"
    if name not in policies:
        raise StorageSchemaError(f"Target storage policy {name!r} is not available")
    if previous != name:
        if value:
            storage = storage[: value.start()] + "'default'" + storage[value.end() :]
        else:
            # Override an unavailable implicit default explicitly. Insert before
            # trailing comments so a line comment cannot swallow the new setting.
            end = len(_without_comments(storage).rstrip())
            prefix = ", " if settings is not None else " SETTINGS "
            storage = (
                storage[:end] + prefix + "storage_policy = 'default' " + storage[end:]
            )
    return storage, policies[name], previous


def _rewrite_table_ttl(
    storage: str, policy: StoragePolicy
) -> tuple[str, tuple[str, ...]]:
    clauses = _storage_clauses(storage)
    for index, (name, start, rules_start) in enumerate(clauses):
        if name != "TTL":
            continue
        end = clauses[index + 1][1] if index + 1 < len(clauses) else len(storage)
        rules, removed = remove_unavailable_move_ttls(storage[rules_start:end], policy)
        if not removed:
            return storage, ()
        # If every rule was removed, remove TTL itself too; an empty TTL is invalid.
        replacement = "TTL " + rules.strip() + " " if rules.strip() else ""
        return storage[:start] + replacement + storage[end:], removed
    return storage, ()


def _storage_clauses(sql: str) -> list[tuple[str, int, int]]:
    """Locate clauses only between complete top-level expressions.

    ClickHouse quotes column definitions but can print their references as bare
    keywords: ORDER BY ttl TTL ttl ... . A keyword where an operand is expected
    is an identifier, not a clause. Quoted values and nested expressions count as
    operands while comments remain whitespace; all offsets refer to the input.
    """
    tokens = list(re.finditer(r"\w+|[^\s]", _top_level_sql(sql, keep_atoms=True)))
    clauses = []
    expect_operand = False
    index = 0
    while index < len(tokens):
        token = tokens[index]
        word = token.group().upper()
        following = [item.group().upper() for item in tokens[index + 1 : index + 3]]
        width = 0 if expect_operand else _clause_width(word, following)
        if width:
            clauses.append((word, token.start(), tokens[index + width - 1].end()))
            if word in {"AS", "COMMENT", "EMPTY", ";"}:
                break
            expect_operand = True
            index += width
            continue
        expect_operand = (
            word in _PREFIX_OPERATORS
            or word in _SYMBOL_OPERATORS
            or (not expect_operand and word in _EXPRESSION_OPERATORS)
        )
        index += 1
    return clauses


def _clause_width(word: str, following: list[str]) -> int:
    """Recognize clause syntax, including the assignment after SETTINGS."""
    if following and (word, following[0]) in {
        ("PARTITION", "BY"),
        ("ORDER", "BY"),
        ("SAMPLE", "BY"),
        ("PRIMARY", "KEY"),
        ("UNIQUE", "KEY"),
    }:
        return 2
    if word == "SETTINGS":
        return int(len(following) == 2 and following[1] == "=")
    if word == "COMMENT":
        return int(following[:1] == [_ATOM])
    if word == "AS":
        return int(bool(following) and following[0] in {"SELECT", "WITH"})
    if word == "EMPTY":
        return int(not following or following[0] in {"AS", ";"})
    return int(word in {"TTL", ";"})


def remove_unavailable_move_ttls(
    ttl: str, policy: StoragePolicy
) -> tuple[str, tuple[str, ...]]:
    """Remove whole MOVE rules; preserve all other comma-separated fragments.

    Commas inside expressions and quoted text are hidden. GROUP BY keys and SET
    assignments may also contain top-level commas; those fragments stay untouched.
    """
    surface = _top_level_sql(ttl)
    # Split only at visible commas. A fragment may be a GROUP BY key or SET
    # assignment, so retain it verbatim unless it contains an unavailable MOVE.
    ends = [comma.start() for comma in re.finditer(",", surface)] + [len(ttl)]
    kept, removed = [], []
    start = 0
    for end in ends:
        fragment = ttl[start:end]
        move = _TTL_MOVE.search(surface, start, end)
        start = end + 1
        if move:
            # The destination must be a literal. Reject unfamiliar syntax rather
            # than silently removing a fragment whose meaning is uncertain.
            destination = _without_comments(ttl[move.end() : end]).strip()
            if not _STRING.fullmatch(destination):
                raise StorageSchemaError(f"Cannot parse TTL MOVE rule: {fragment!r}")
            allowed = policy.disks if move["kind"].upper() == "DISK" else policy.volumes
            if _unquote_sql_string(destination) not in allowed:
                removed.append(fragment.strip())
                continue
        kept.append(fragment)
    return ",".join(kept), tuple(removed)


def replicated_engine_path(statement: str) -> str | None:
    """Read an explicit Keeper path, or return None for default engine arguments."""
    engine = _find_merge_tree_engine(statement)
    if engine is None or not engine["engine"].lower().startswith("replicated"):
        raise StorageSchemaError("Cannot find a replicated MergeTree engine")
    # Only an explicit first string argument supplies the path here. The restore
    # coordinator resolves server defaults and macros for engines without it.
    path = re.match(
        r"\s*\(\s*('(?:\\.|''|[^'\\])*')",
        _without_comments(statement[engine.end() :]),
        re.DOTALL,
    )
    return _unquote_sql_string(path[1]) if path else None


def _find_merge_tree_engine(statement: str) -> re.Match[str] | None:
    """Locate the table engine, ignoring column expressions and a view's SELECT."""
    before_select = re.split(
        r"\bAS\s+(?:SELECT|WITH)\b",
        _top_level_sql(statement),
        maxsplit=1,
        flags=re.IGNORECASE,
    )[0]
    return _MERGE_TREE_ENGINE.search(before_select)


def _unquote_sql_string(value: str) -> str:
    """Decode a matched SQL string literal, including doubled and escaped quotes."""
    escapes = {"n": "\n", "r": "\r", "t": "\t", "0": "\0", "b": "\b", "f": "\f"}
    return re.sub(
        r"\\(.)|''",
        lambda match: escapes.get(match[1], match[1]) if match[1] is not None else "'",
        value[1:-1],
        flags=re.DOTALL,
    )


def _blank(match: re.Match[str]) -> str:
    return " " * len(match.group())


def _without_comments(sql: str) -> str:
    # Match strings before comments so '--' or '/*' inside a literal stays intact.
    # Spaces keep match offsets usable against the original SQL.
    return _QUOTED_OR_COMMENT.sub(
        lambda match: match.group() if match.group()[0] in "'\"`" else _blank(match),
        sql,
    )


def _top_level_sql(sql: str, keep_atoms: bool = False) -> str:
    """Hide quoted text, comments and bracket contents, preserving source offsets.

    For example, ``TTL if(x IN (1, 2), d, e) TO DISK 'cold'`` becomes
    ``TTL if                           TO DISK       `` for keyword/comma searches.
    This does not parse expressions or change the SQL sent to ClickHouse.
    """

    def blank_atom(match: re.Match[str]) -> str:
        return _ATOM + " " * (len(match.group()) - 1) if keep_atoms else _blank(match)

    surface = _QUOTED_OR_COMMENT.sub(
        lambda match: (
            blank_atom(match) if match.group()[0] in "'\"`" else _blank(match)
        ),
        sql,
    )
    # An unmatched opener would make subsequent keyword boundaries ambiguous.
    if re.search(r"""['"`]|/\*""", surface):
        raise StorageSchemaError("Unterminated SQL quoted text or comment")
    # Blank innermost bracket pairs until no nested expressions remain.
    while True:
        surface, count = _INNER_BRACKETS.subn(blank_atom, surface)
        if not count:
            break
    if re.search(r"[()[\]]", surface):
        raise StorageSchemaError("Unbalanced SQL brackets")
    return surface
