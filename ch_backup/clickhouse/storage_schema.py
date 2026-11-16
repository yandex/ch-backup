"""Adapt storage policy and table TTL clauses in ClickHouse-generated DDL.

The rewrites leave expressions, column definitions and SELECT queries intact.
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
_DISK_SETTING = re.compile(r"""(?:disk|`disk`|"disk")\s*=\s*""", re.IGNORECASE)
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
# ClickHouse formats INTERVAL and CASE expressions as toInterval*/multiIf/
# caseWithExpression calls, but can leave references to columns named interval,
# case, when, then or else unquoted. Those names complete an operand; treating
# them as prefixes would hide the following TTL or SETTINGS clause. NOT remains
# an operator in formatted SQL and still needs its operand.
_PREFIX_OPERATORS = {"NOT"}
_SYMBOL_OPERATORS = set(",.=+-*/%<>!|&^?:")
_STORAGE_END_CLAUSES = {"AS", "COMMENT", "EMPTY", "DEFINER", "SQL", ";"}


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
    removed_disk: str | None = None

    @property
    def changed(self) -> bool:
        """Whether policy placement or TTL semantics changed."""
        return (
            self.removed_disk is not None
            or self.previous_policy != self.policy.name
            or len(self.removed_ttls) > 0
        )


def adapt_storage_schema(
    statement: str, policies: dict[str, StoragePolicy], default_policy: str = "default"
) -> StorageSchemaChange | None:
    """Remove an explicit disk, resolve the policy, then adapt MOVE destinations."""
    engine = find_merge_tree_engine(statement)
    if engine is None:
        return None

    # Only the storage clause after ENGINE and before a view's AS SELECT is edited.
    start = engine.end()
    end = next(
        (
            start + begin
            for name, begin, _ in _storage_clauses(statement[start:])
            if name in _STORAGE_END_CLAUSES
        ),
        len(statement),
    )
    storage = statement[start:end]
    # An explicit disk bypasses the target server's default policy. Drop it before
    # resolving placement so TTL checks and restored parts use the same policy.
    storage, removed_disk = _remove_disk_setting(storage)
    # Resolve the target policy first: a TTL destination must belong to that
    # policy, even if a disk with the same name exists elsewhere on the server.
    storage, policy, previous = _replace_missing_policy(
        storage, policies, default_policy
    )
    storage, removed = _rewrite_table_ttl(storage, policy)
    return StorageSchemaChange(
        statement[:start] + storage + statement[end:],
        policy,
        previous,
        removed,
        removed_disk,
    )


def _remove_disk_setting(storage: str) -> tuple[str, str | None]:
    """Remove a named disk from table SETTINGS, preserving other assignments."""
    settings = next(
        (
            (start, end)
            for name, start, end in _storage_clauses(storage)
            if name == "SETTINGS"
        ),
        None,
    )
    if settings is None:
        return storage, None
    start, values_start = settings
    values = storage[values_start:]
    surface = _top_level_sql(values)
    # Only top-level commas separate settings: quoted names and nested values can
    # contain commas too. Rejoin retained fragments to handle first/last entries.
    ends = [comma.start() for comma in re.finditer(",", surface)] + [len(values)]
    kept = []
    removed = None
    has_policy = False
    begin = 0
    for end in ends:
        fragment = values[begin:end]
        begin = end + 1
        setting = _without_comments(fragment).strip()
        has_policy = has_policy or _POLICY_SETTING.match(setting) is not None
        disk = _DISK_SETTING.match(setting)
        if disk is None:
            kept.append(fragment)
            continue
        value = _STRING.fullmatch(setting[disk.end() :].strip())
        if value is None:
            raise StorageSchemaError("disk must be a string literal")
        removed = _unquote_sql_string(value.group())
    if removed is None:
        return storage, None
    if has_policy:
        raise StorageSchemaError("disk and storage_policy cannot be specified together")
    if not kept:
        return storage[:start], removed
    rewritten = storage[:values_start] + " " + ",".join(kept).lstrip()
    # Keep a newline terminating a line comment; also separate the next clause
    # when the removed last setting supplied the original trailing whitespace.
    if not rewritten[-1].isspace():
        rewritten += " "
    return rewritten, removed


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
    previous = (
        _unquote_sql_string(value.group()) if value is not None else default_policy
    )
    name = previous if previous in policies else "default"
    if name not in policies:
        raise StorageSchemaError(f"Target storage policy {name!r} is not available")
    if previous != name:
        if value is not None:
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
        if len(removed) == 0:
            return storage, ()
        # If every rule was removed, remove TTL itself too; an empty TTL is invalid.
        replacement = "TTL " + rules.strip() + " " if rules.strip() != "" else ""
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
        if width > 0:
            clauses.append((word, token.start(), tokens[index + width - 1].end()))
            if word in _STORAGE_END_CLAUSES:
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
    if len(following) > 0 and (word, following[0]) in {
        ("PARTITION", "BY"),
        ("ORDER", "BY"),
        ("SAMPLE", "BY"),
        ("PRIMARY", "KEY"),
        ("UNIQUE", "KEY"),
        ("SQL", "SECURITY"),
    }:
        return 2
    if word == "SETTINGS":
        return 1 if len(following) == 2 and following[1] == "=" else 0
    # View security follows the engine's SETTINGS and must remain outside the
    # final setting value, even when that value is the disk being removed.
    if word in {"COMMENT", "DEFINER"}:
        return 1 if following[:1] == ([_ATOM] if word == "COMMENT" else ["="]) else 0
    if word == "AS":
        return 1 if len(following) > 0 and following[0] in {"SELECT", "WITH"} else 0
    if word == "EMPTY":
        return 1 if len(following) == 0 or following[0] in _STORAGE_END_CLAUSES else 0
    return 1 if word in {"TTL", ";"} else 0


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
        if move is not None:
            # The destination must be a literal. Reject unfamiliar syntax rather
            # than silently removing a fragment whose meaning is uncertain.
            destination = _without_comments(ttl[move.end() : end]).strip()
            if _STRING.fullmatch(destination) is None:
                raise StorageSchemaError(f"Cannot parse TTL MOVE rule: {fragment!r}")
            allowed = policy.disks if move["kind"].upper() == "DISK" else policy.volumes
            if _unquote_sql_string(destination) not in allowed:
                removed.append(fragment.strip())
                continue
        kept.append(fragment)
    return ",".join(kept), tuple(removed)


def replicated_engine_path(statement: str) -> str | None:
    """Read an explicit Keeper path, or return None for default engine arguments."""
    engine = find_merge_tree_engine(statement)
    if engine is None or not engine["engine"].lower().startswith("replicated"):
        raise StorageSchemaError("Cannot find a replicated MergeTree engine")
    # Only an explicit first string argument supplies the path here. The restore
    # coordinator resolves server defaults and macros for engines without it.
    path = re.match(
        r"\s*\(\s*('(?:\\.|''|[^'\\])*')",
        _without_comments(statement[engine.end() :]),
        re.DOTALL,
    )
    return _unquote_sql_string(path[1]) if path is not None else None


def find_merge_tree_engine(statement: str) -> re.Match[str] | None:
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
    if re.search(r"""['"`]|/\*""", surface) is not None:
        raise StorageSchemaError("Unterminated SQL quoted text or comment")
    # Blank innermost bracket pairs until no nested expressions remain.
    while True:
        surface, count = _INNER_BRACKETS.subn(blank_atom, surface)
        if count == 0:
            break
    if re.search(r"[()[\]]", surface) is not None:
        raise StorageSchemaError("Unbalanced SQL brackets")
    return surface
