"""Build disposable copies of ClickHouse-generated table definitions."""

import re
from uuid import uuid4

from ch_backup.clickhouse.models import Disk, Table
from ch_backup.clickhouse.schema import rewrite_table_schema
from ch_backup.clickhouse.storage_schema import (
    _QUOTED_OR_COMMENT,
    StoragePolicy,
    _storage_clauses,
    find_merge_tree_engine,
    merge_tree_storage_settings,
)
from ch_backup.exceptions import ClickhouseBackupError
from ch_backup.util import escape

_TOKEN = re.compile(_QUOTED_OR_COMMENT.pattern + r"|\w+|[^\s]", re.VERBOSE | re.DOTALL)
_LOCAL_ENGINES = {
    "Memory",
    "Log",
    "TinyLog",
    "StripeLog",
    "Null",
    "Set",
    "Join",
    "View",
    "MaterializedView",
}


def _tokens(statement: str) -> list[re.Match]:
    return [
        token
        for token in _TOKEN.finditer(statement)
        if not token[0].startswith(("--", "#", "/*"))
    ]


def _identifier(token: str) -> str:
    if token.startswith(("`", '"')):
        quote = token[0]
        return re.sub(r"\\(.)", r"\1", token[1:-1].replace(quote * 2, quote))
    return token


def probe_database(statement: str, name: str) -> str:
    """Give a local database definition a disposable name and UUID."""
    tokens = _tokens(statement)
    if len(tokens) < 3 or tokens[1][0].upper() != "DATABASE":
        raise ClickhouseBackupError("Unsupported database schema header")
    edits = {tokens[0].span(): "CREATE", tokens[2].span(): f"`{escape(name)}`"}
    if len(tokens) > 4 and tokens[3][0].upper() == "UUID":
        edits[tokens[4].span()] = f"'{uuid4()}'"
    for (start, end), replacement in sorted(edits.items(), reverse=True):
        statement = statement[:start] + replacement + statement[end:]
    return statement


def schema_engine(ast: str) -> str | None:
    """Read the actual engine from the server AST, not backup metadata."""
    storage = re.search(
        r"^ +Storage definition[^\n]*\n +Function (\w+)", ast, re.MULTILINE
    )
    return storage[1] if storage is not None else None


def can_create_probe(
    table: Table, ast: str, names: set[str], external_functions: set[str] | None = None
) -> bool:
    """Do not start external engines or execute queries with external dependencies."""
    # pylint: disable=too-many-return-statements
    if not table.is_merge_tree() and table.engine not in _LOCAL_ENGINES:
        return False
    storage = schema_engine(ast)
    if storage is not None:
        if not storage.endswith("MergeTree") and storage not in _LOCAL_ENGINES:
            return False
    elif not table.is_view():
        # Classify the actual statement, including an MV's own engine, rather
        # than trusting only the engine name stored separately in metadata.
        return False
    if not table.is_view() and "SelectWithUnionQuery" in ast:
        return False
    if external_functions is not None and any(
        name.strip() in external_functions
        for name in re.findall(r"^ +Function ([^\n(]+)", ast, re.MULTILINE)
    ):
        return False
    # Table functions and dictionary/executable functions can contact external
    # systems even while CREATE only infers types. Keep those checks syntax-only.
    if re.search(
        r"TableExpression[^\n]*\n +Function|Function (dict|joinGet|modelEvaluate|catboostEvaluate)",
        ast,
    ):
        return False
    if any(
        token[0].upper() in {"POPULATE", "REFRESH"}
        for token in _tokens(table.create_statement)
    ):
        return False
    dependencies = re.findall(
        r"^ +TableIdentifier (.*?)(?: \(alias .*\))?$", ast, re.MULTILINE
    )
    if any(
        name not in names and f"{table.database}.{name}" not in names
        for name in dependencies
    ):
        return False
    return not table.is_view() or _view_is_isolated(table, ast, names)


def has_local_probe_storage(
    table: Table,
    policies: dict[str, StoragePolicy],
    disks: dict[str, Disk],
    config: dict,
) -> bool:
    """Only create MergeTree copies when every effective disk is known to be local."""
    engine = find_merge_tree_engine(table.create_statement)
    if engine is None:
        return True
    settings = dict(config.get("merge_tree") or {})
    if engine["engine"].startswith("Replicated"):
        settings.update(config.get("replicated_merge_tree") or {})
    settings.update(merge_tree_storage_settings(table.create_statement))
    if "disk" in settings:
        name = settings["disk"]
        if name is None:
            return False
        if name not in disks:
            raise ClickhouseBackupError(
                f"Schema preflight for {table.database}.{table.name}: unknown disk {name!r}"
            )
        return disks[name].type == "local"
    name = settings.get("storage_policy", "default")
    if name is None:
        return False
    if name not in policies:
        raise ClickhouseBackupError(
            f"Schema preflight for {table.database}.{table.name}: unknown storage policy {name!r}"
        )
    policy = policies[name]
    return len(policy.disks) > 0 and set(policy.disks) <= set(policy.local_disks)


def _view_is_isolated(table: Table, ast: str, names: set[str]) -> bool:
    # Do not confuse a table alias with a database qualifier when rewriting
    # SELECT. Ambiguous queries still receive the server's syntax check.
    aliases = re.findall(r"\(alias ([^)]*)\)", ast)
    if any(any(name.startswith(alias + ".") for name in names) for alias in aliases):
        return False
    tokens = _tokens(table.create_statement)
    for index, token in enumerate(tokens[:-1]):
        if token[0].upper() in {"(", "AS", "ENGINE"}:
            break
        if token[0].upper() == "TO" and tokens[index + 1][0].upper() != "INNER":
            name = _identifier(tokens[index + 1][0])
            if index + 3 < len(tokens) and tokens[index + 2][0] == ".":
                name += "." + _identifier(tokens[index + 3][0])
            else:
                name = table.database + "." + name
            if name not in names:
                return False
    return True


def probe_table(
    table: Table,
    databases: dict[str, str],
    names: set[tuple[str, str]],
    replicated_policy: str | None = None,
    *,
    replicated_disk: str | None = None,
) -> Table:
    """Change identities, never literals or the original metadata object."""
    # pylint: disable=too-many-locals
    probe = Table.make_dummy(databases[table.database], table.name)
    probe.engine = table.engine
    sql = table.create_statement
    tokens = _tokens(sql)
    words = [token[0].upper() for token in tokens]
    if len(tokens) < 3 or words[0] not in {"CREATE", "ATTACH"}:
        raise ClickhouseBackupError(
            f"Unsupported schema header for {table.database}.{table.name}"
        )
    name_index = 3 if words[1] in {"MATERIALIZED", "LIVE", "WINDOW"} else 2
    if words[name_index - 1] not in {"TABLE", "VIEW"}:
        raise ClickhouseBackupError(
            f"Unsupported schema header for {table.database}.{table.name}"
        )
    name_end = name_index + 2 if tokens[name_index + 1][0] == "." else name_index
    edits = {
        tokens[0].span(): "CREATE",
        (
            tokens[name_index].start(),
            tokens[name_end].end(),
        ): f"`{escape(databases[table.database])}`.`{escape(table.name)}`",
    }
    # UUID and TO INNER UUID belong to the header, before column definitions.
    # Every copy gets separate identities, including an MV's implicit storage.
    for index in range(name_end + 1, len(tokens) - 1):
        if words[index] in {"(", "AS", "ENGINE"}:
            break
        if words[index] == "UUID" and tokens[index + 1][0].startswith("'"):
            edits[tokens[index + 1].span()] = f"'{uuid4()}'"
    for index in range(name_end + 1, len(tokens) - 2):
        if tokens[index + 1][0] != ".":
            continue
        database, name = _identifier(tokens[index][0]), _identifier(
            tokens[index + 2][0]
        )
        if (database, name) in names:
            edits[tokens[index].span()] = f"`{escape(databases[database])}`"
    for (start, end), replacement in sorted(edits.items(), reverse=True):
        sql = sql[:start] + replacement + sql[end:]
    probe.create_statement = sql
    probe.database = databases[table.database]
    probe.uuid = None
    engine = find_merge_tree_engine(sql)
    replicated = engine is not None and engine["engine"].startswith("Replicated")
    rewrite_table_schema(probe, force_non_replicated_engine=True)
    if replicated:
        # Replicated defaults disappear with the engine prefix. In particular,
        # losing a local disk override could expose a remote MergeTree default.
        if replicated_disk is not None:
            probe.create_statement = _with_default_storage(
                probe.create_statement, "disk", replicated_disk
            )
        elif replicated_policy is not None:
            probe.create_statement = _with_default_storage(
                probe.create_statement, "storage_policy", replicated_policy
            )
    return probe


def _with_default_storage(statement: str, setting: str, value: str) -> str:
    """Retain replicated placement defaults after removing replication."""
    explicit = merge_tree_storage_settings(statement)
    if setting in explicit or "disk" in explicit:
        return statement
    engine = find_merge_tree_engine(statement)
    assert engine is not None
    clauses = list(_storage_clauses(statement[engine.end() :]))
    end = next(
        (
            engine.end() + start
            for name, start, _ in clauses
            if name in {"AS", "COMMENT", "EMPTY", "DEFINER", "SQL", ";"}
        ),
        len(statement),
    )
    separator = (
        ", "
        if any(
            name == "SETTINGS" and engine.end() + start < end
            for name, start, _ in clauses
        )
        else " SETTINGS "
    )
    return (
        statement[:end].rstrip()
        + separator
        + setting
        + " = '"
        + value.replace("\\", "\\\\").replace("'", "\\'")
        + "' "
        + statement[end:]
    )
