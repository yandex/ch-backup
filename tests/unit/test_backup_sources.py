import pytest

from ch_backup.backup.sources import BackupSources


@pytest.mark.parametrize(
    ["access", "data", "schema", "udf", "named_collections", "schema_only", "expected"],
    [
        # default (all sources)
        (
            False,
            False,
            False,
            False,
            False,
            False,
            BackupSources(
                access=True, data=True, schema=True, udf=True, named_collections=True
            ),
        ),
        # schema-only
        (
            False,
            False,
            False,
            False,
            False,
            True,
            BackupSources(
                access=True, data=False, schema=True, udf=True, named_collections=True
            ),
        ),
        # only access
        (
            True,
            False,
            False,
            False,
            False,
            False,
            BackupSources(
                access=True,
                data=False,
                schema=False,
                udf=False,
                named_collections=False,
            ),
        ),
        # only data
        (
            False,
            True,
            False,
            False,
            False,
            False,
            BackupSources(
                access=False, data=True, schema=True, udf=False, named_collections=False
            ),
        ),
        # only schema
        (
            False,
            False,
            True,
            False,
            False,
            False,
            BackupSources(
                access=False,
                data=False,
                schema=True,
                udf=False,
                named_collections=False,
            ),
        ),
        # data & schema (like data)
        (
            False,
            True,
            True,
            False,
            False,
            False,
            BackupSources(
                access=False, data=True, schema=True, udf=False, named_collections=False
            ),
        ),
        # only udf
        (
            False,
            False,
            False,
            True,
            False,
            False,
            BackupSources(
                access=False,
                data=False,
                schema=False,
                udf=True,
                named_collections=False,
            ),
        ),
        # only named_collections
        (
            False,
            False,
            False,
            False,
            True,
            False,
            BackupSources(
                access=False,
                data=False,
                schema=False,
                udf=False,
                named_collections=True,
            ),
        ),
        # (combinations) access & udf
        (
            True,
            False,
            False,
            True,
            False,
            False,
            BackupSources(
                access=True, data=False, schema=False, udf=True, named_collections=False
            ),
        ),
        # (combinations) access & udf & schema
        (
            True,
            False,
            True,
            True,
            False,
            False,
            BackupSources(
                access=True, data=False, schema=True, udf=True, named_collections=False
            ),
        ),
        # (combinations) udf & data
        (
            False,
            True,
            False,
            True,
            False,
            False,
            BackupSources(
                access=False, data=True, schema=True, udf=True, named_collections=False
            ),
        ),
        # (combinations) named_collections & data
        (
            False,
            True,
            False,
            False,
            True,
            False,
            BackupSources(
                access=False, data=True, schema=True, udf=False, named_collections=True
            ),
        ),
        # all flags (like default)
        (
            True,
            True,
            True,
            True,
            True,
            False,
            BackupSources(
                access=True, data=True, schema=True, udf=True, named_collections=True
            ),
        ),
        # schema-only with new args (ignoring)
        (
            True,
            True,
            True,
            True,
            True,
            True,
            BackupSources(
                access=True, data=True, schema=True, udf=True, named_collections=True
            ),
        ),
        # schema-only with new args (ignoring)
        (
            True,
            False,
            False,
            False,
            False,
            True,
            BackupSources(
                access=True,
                data=False,
                schema=False,
                udf=False,
                named_collections=False,
            ),
        ),
        # schema-only with new args (ignoring)
        (
            False,
            True,
            False,
            False,
            False,
            True,
            BackupSources(
                access=False, data=True, schema=True, udf=False, named_collections=False
            ),
        ),
    ],
)
# pylint: disable=too-many-function-args
def test_sources_for_backup(
    access: bool,
    data: bool,
    schema: bool,
    udf: bool,
    named_collections: bool,
    schema_only: bool,
    expected: BackupSources,
) -> None:
    assert (
        BackupSources.for_backup(
            access, data, schema, udf, named_collections, schema_only
        )
        == expected
    )


@pytest.mark.parametrize(
    ["access", "data", "schema", "udf", "named_collections", "schema_only", "expected"],
    [
        # default (all except access)
        (
            False,
            False,
            False,
            False,
            False,
            False,
            BackupSources(
                access=False, data=True, schema=True, udf=True, named_collections=True
            ),
        ),
        # schema-only
        (
            False,
            False,
            False,
            False,
            False,
            True,
            BackupSources(
                access=False, data=False, schema=True, udf=True, named_collections=True
            ),
        ),
        # only access
        (
            True,
            False,
            False,
            False,
            False,
            False,
            BackupSources(
                access=True,
                data=False,
                schema=False,
                udf=False,
                named_collections=False,
            ),
        ),
        # only data
        (
            False,
            True,
            False,
            False,
            False,
            False,
            BackupSources(
                access=False, data=True, schema=True, udf=False, named_collections=False
            ),
        ),
        # only schema
        (
            False,
            False,
            True,
            False,
            False,
            False,
            BackupSources(
                access=False,
                data=False,
                schema=True,
                udf=False,
                named_collections=False,
            ),
        ),
        # data & schema (like data)
        (
            False,
            True,
            True,
            False,
            False,
            False,
            BackupSources(
                access=False, data=True, schema=True, udf=False, named_collections=False
            ),
        ),
        # only udf
        (
            False,
            False,
            False,
            True,
            False,
            False,
            BackupSources(
                access=False,
                data=False,
                schema=False,
                udf=True,
                named_collections=False,
            ),
        ),
        # only named_collections
        (
            False,
            False,
            False,
            False,
            True,
            False,
            BackupSources(
                access=False,
                data=False,
                schema=False,
                udf=False,
                named_collections=True,
            ),
        ),
        # (combinations) access & udf
        (
            True,
            False,
            False,
            True,
            False,
            False,
            BackupSources(
                access=True, data=False, schema=False, udf=True, named_collections=False
            ),
        ),
        # (combinations) access & udf & schema
        (
            True,
            False,
            True,
            True,
            False,
            False,
            BackupSources(
                access=True, data=False, schema=True, udf=True, named_collections=False
            ),
        ),
        # (combinations) udf & data
        (
            False,
            True,
            False,
            True,
            False,
            False,
            BackupSources(
                access=False, data=True, schema=True, udf=True, named_collections=False
            ),
        ),
        # (combinations) named_collections & data
        (
            False,
            True,
            False,
            False,
            True,
            False,
            BackupSources(
                access=False, data=True, schema=True, udf=False, named_collections=True
            ),
        ),
        # all flags (like default)
        (
            True,
            True,
            True,
            True,
            True,
            False,
            BackupSources(
                access=True, data=True, schema=True, udf=True, named_collections=True
            ),
        ),
        # schema-only with new args (ignoring)
        (
            True,
            True,
            True,
            True,
            True,
            True,
            BackupSources(
                access=True, data=True, schema=True, udf=True, named_collections=True
            ),
        ),
        # schema-only with new args (ignoring)
        (
            True,
            False,
            False,
            False,
            False,
            True,
            BackupSources(
                access=True,
                data=False,
                schema=False,
                udf=False,
                named_collections=False,
            ),
        ),
        # schema-only with new args (ignoring)
        (
            False,
            True,
            False,
            False,
            False,
            True,
            BackupSources(
                access=False, data=True, schema=True, udf=False, named_collections=False
            ),
        ),
    ],
)
# pylint: disable=too-many-function-args
def test_sources_for_restore(
    access: bool,
    data: bool,
    schema: bool,
    udf: bool,
    named_collections: bool,
    schema_only: bool,
    expected: BackupSources,
) -> None:
    assert (
        BackupSources.for_restore(
            access, data, schema, udf, named_collections, schema_only
        )
        == expected
    )
