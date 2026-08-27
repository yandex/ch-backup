# MDB-49029: Reload local access control before schema restore

## Task

Make a combined `ch-backup restore --access --data` reload non-replicated access-control entities in the running ClickHouse server before restoring UDFs, named collections, workload entities, databases, views, tables, and data. Preserve the current standalone `restore-access-control` behavior so it can still restore files without requiring a running ClickHouse server.

## Plan Structure

Extend the ClickHouse control API, integrate conditional user reload into access restore orchestration, add unit and integration regression coverage, and run the required checks sequentially.

## Execution Plan

### Stage 1: Add local access reload support

**What to add/implement:**

- Add `ClickhouseCTL.reload_users()` for `SYSTEM RELOAD USERS`.
- Add `reload_users: bool = False` to `AccessBackup.restore()` and reload only after successful local access restore.
- Pass `reload_users=True` only from the combined `ClickhouseBackup._restore()` flow.

**Files to edit/create:**

- `ch_backup/clickhouse/control.py` - add the query and control method.
- `ch_backup/logic/access.py` - conditionally reload local access storage.
- `ch_backup/ch_backup.py` - enable reload in combined restore.

**Framework/Library Documentation:**

- `/home/bor-dmi/git/ClickHouse/src/Interpreters/InterpreterSystemQuery.cpp` - `SYSTEM RELOAD USERS` implementation.
- `/home/bor-dmi/git/ClickHouse/src/Access/DiskAccessStorage.cpp` - synchronous disk access reload.

**Examples in existing code:**

- `ch_backup/clickhouse/control.py` - `reload_config()` query wrapper.
- `ch_backup/logic/access.py` - local and replicated access restore branches.

**Verification commands:**

- `git diff --check`

### Stage 2: Add regression tests

**What to add/implement:**

- Test the ClickHouse control query and local/replicated/disabled reload behavior.
- Add a one-command restore scenario for a user and a view with `SQL SECURITY DEFINER`.

**Files to edit/create:**

- `tests/unit/test_control.py` - test `reload_users()`.
- `tests/unit/test_access.py` - test conditional reload behavior.
- `tests/integration/features/access_control_backup.feature` - add the regression scenario.

**Examples in existing code:**

- `tests/integration/features/access_control_backup.feature` - access restore scenarios.
- `tests/integration/features/backup_restore_sources.feature` - combined restore source options.

**Verification commands:**

- `git diff --check`

### Stage 3: Run final verification

**What to add/implement:**

- Run lint, unit tests, clean integration containers, and the targeted integration feature in strict order.
- If any check fails, fix it and restart from lint.

**Files to edit/create:**

- `vibe/MDB-49029-plan-track.md` - record completed stages.

**Verification commands:**

- `make lint`
- `make test-unit`
- `make clean-test-env`
- `make test-integration BEHAVE_ARGS="-i access_control_backup"`
