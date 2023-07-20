import pytest

from ch_backup.backup.sources import BackupSources


@pytest.mark.parametrize(
    ["access", "data", "schema", "udf", "schema_only", "expected"],
    [
        # default (all sources)
        (
            False,
            False,
            False,
            False,
            False,
            BackupSources(access=True, data=True, schema=True, udf=True),
        ),
        # schema-only
        (
            False,
            False,
            False,
            False,
            True,
            BackupSources(access=True, data=False, schema=True, udf=True),
        ),
        # only access
        (
            True,
            False,
            False,
            False,
            False,
            BackupSources(access=True, data=False, schema=False, udf=False),
        ),
        # only data
        (
            False,
            True,
            False,
            False,
            False,
            BackupSources(access=False, data=True, schema=True, udf=False),
        ),
        # only schema
        (
            False,
            False,
            True,
            False,
            False,
            BackupSources(access=False, data=False, schema=True, udf=False),
        ),
        # data & schema (like data)
        (
            False,
            True,
            True,
            False,
            False,
            BackupSources(access=False, data=True, schema=True, udf=False),
        ),
        # only udf
        (
            False,
            False,
            False,
            True,
            False,
            BackupSources(access=False, data=False, schema=False, udf=True),
        ),
        # (combinations) access & udf
        (
            True,
            False,
            False,
            True,
            False,
            BackupSources(access=True, data=False, schema=False, udf=True),
        ),
        # (combinations) access & udf & schema
        (
            True,
            False,
            True,
            True,
            False,
            BackupSources(access=True, data=False, schema=True, udf=True),
        ),
        # (combinations) udf & data
        (
            False,
            True,
            False,
            True,
            False,
            BackupSources(access=False, data=True, schema=True, udf=True),
        ),
        # all flags (like default)
        (
            True,
            True,
            True,
            True,
            False,
            BackupSources(access=True, data=True, schema=True, udf=True),
        ),
        # schema-only with new args (ignoring)
        (
            True,
            True,
            True,
            True,
            True,
            BackupSources(access=True, data=True, schema=True, udf=True),
        ),
        # schema-only with new args (ignoring)
        (
            True,
            False,
            False,
            False,
            True,
            BackupSources(access=True, data=False, schema=False, udf=False),
        ),
        # schema-only with new args (ignoring)
        (
            False,
            True,
            False,
            False,
            True,
            BackupSources(access=False, data=True, schema=True, udf=False),
        ),
    ],
)
def test_sources_for_backup(
    access: bool,
    data: bool,
    schema: bool,
    udf: bool,
    schema_only: bool,
    expected: BackupSources,
) -> None:
    assert BackupSources.for_backup(access, data, schema, udf, schema_only) == expected


@pytest.mark.parametrize(
    ["access", "data", "schema", "udf", "schema_only", "expected"],
    [
        # default (all except access)
        (
            False,
            False,
            False,
            False,
            False,
            BackupSources(access=False, data=True, schema=True, udf=True),
        ),
        # schema-only
        (
            False,
            False,
            False,
            False,
            True,
            BackupSources(access=False, data=False, schema=True, udf=True),
        ),
        # only access
        (
            True,
            False,
            False,
            False,
            False,
            BackupSources(access=True, data=False, schema=False, udf=False),
        ),
        # only data
        (
            False,
            True,
            False,
            False,
            False,
            BackupSources(access=False, data=True, schema=True, udf=False),
        ),
        # only schema
        (
            False,
            False,
            True,
            False,
            False,
            BackupSources(access=False, data=False, schema=True, udf=False),
        ),
        # data & schema (like data)
        (
            False,
            True,
            True,
            False,
            False,
            BackupSources(access=False, data=True, schema=True, udf=False),
        ),
        # only udf
        (
            False,
            False,
            False,
            True,
            False,
            BackupSources(access=False, data=False, schema=False, udf=True),
        ),
        # (combinations) access & udf
        (
            True,
            False,
            False,
            True,
            False,
            BackupSources(access=True, data=False, schema=False, udf=True),
        ),
        # (combinations) access & udf & schema
        (
            True,
            False,
            True,
            True,
            False,
            BackupSources(access=True, data=False, schema=True, udf=True),
        ),
        # (combinations) udf & data
        (
            False,
            True,
            False,
            True,
            False,
            BackupSources(access=False, data=True, schema=True, udf=True),
        ),
        # all flags (like default)
        (
            True,
            True,
            True,
            True,
            False,
            BackupSources(access=True, data=True, schema=True, udf=True),
        ),
        # schema-only with new args (ignoring)
        (
            True,
            True,
            True,
            True,
            True,
            BackupSources(access=True, data=True, schema=True, udf=True),
        ),
        # schema-only with new args (ignoring)
        (
            True,
            False,
            False,
            False,
            True,
            BackupSources(access=True, data=False, schema=False, udf=False),
        ),
        # schema-only with new args (ignoring)
        (
            False,
            True,
            False,
            False,
            True,
            BackupSources(access=False, data=True, schema=True, udf=False),
        ),
    ],
)
def test_sources_for_restore(
    access: bool,
    data: bool,
    schema: bool,
    udf: bool,
    schema_only: bool,
    expected: BackupSources,
) -> None:
    assert BackupSources.for_restore(access, data, schema, udf, schema_only) == expected
