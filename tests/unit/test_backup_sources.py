import pytest

from ch_backup.backup.sources import BackupSources
from ch_backup.exceptions import ConfigurationError


@pytest.mark.parametrize(
    [
        "access",
        "data",
        "schema",
        "udf",
        "named_collections",
        "workload_entities",
        "schema_only",
        "expected",
    ],
    [
        # default (all sources)
        (
            False,
            False,
            False,
            False,
            False,
            False,
            False,
            BackupSources(
                access=True,
                data=True,
                schema=True,
                udf=True,
                named_collections=True,
                workload_entities=True,
            ),
        ),
        # schema-only
        (
            False,
            False,
            False,
            False,
            False,
            False,
            True,
            BackupSources(
                access=True,
                data=False,
                schema=True,
                udf=True,
                named_collections=True,
                workload_entities=True,
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
            False,
            BackupSources(
                access=True,
                data=False,
                schema=False,
                udf=False,
                named_collections=False,
                workload_entities=False,
                http_handlers=False,
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
            False,
            BackupSources(
                access=False,
                data=True,
                schema=True,
                udf=False,
                named_collections=False,
                workload_entities=False,
                http_handlers=False,
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
            False,
            BackupSources(
                access=False,
                data=False,
                schema=True,
                udf=False,
                named_collections=False,
                workload_entities=False,
                http_handlers=False,
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
            False,
            BackupSources(
                access=False,
                data=True,
                schema=True,
                udf=False,
                named_collections=False,
                workload_entities=False,
                http_handlers=False,
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
            False,
            BackupSources(
                access=False,
                data=False,
                schema=False,
                udf=True,
                named_collections=False,
                workload_entities=False,
                http_handlers=False,
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
            False,
            BackupSources(
                access=False,
                data=False,
                schema=False,
                udf=False,
                named_collections=True,
                workload_entities=False,
                http_handlers=False,
            ),
        ),
        # only workload_entities
        (
            False,
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
                named_collections=False,
                workload_entities=True,
                http_handlers=False,
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
            False,
            BackupSources(
                access=True,
                data=False,
                schema=False,
                udf=True,
                named_collections=False,
                workload_entities=False,
                http_handlers=False,
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
            False,
            BackupSources(
                access=True,
                data=False,
                schema=True,
                udf=True,
                named_collections=False,
                workload_entities=False,
                http_handlers=False,
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
            False,
            BackupSources(
                access=False,
                data=True,
                schema=True,
                udf=True,
                named_collections=False,
                workload_entities=False,
                http_handlers=False,
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
            False,
            BackupSources(
                access=False,
                data=True,
                schema=True,
                udf=False,
                named_collections=True,
                workload_entities=False,
                http_handlers=False,
            ),
        ),
        # (combinations) workload_entities & data
        (
            False,
            True,
            False,
            False,
            False,
            True,
            False,
            BackupSources(
                access=False,
                data=True,
                schema=True,
                udf=False,
                named_collections=False,
                workload_entities=True,
                http_handlers=False,
            ),
        ),
        # all flags (like default)
        (
            True,
            True,
            True,
            True,
            True,
            True,
            False,
            BackupSources(
                access=True,
                data=True,
                schema=True,
                udf=True,
                named_collections=True,
                workload_entities=True,
                http_handlers=False,
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
            True,
            BackupSources(
                access=True,
                data=True,
                schema=True,
                udf=True,
                named_collections=True,
                workload_entities=True,
                http_handlers=False,
            ),
        ),
        # schema-only with new args (ignoring)
        (
            True,
            False,
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
                workload_entities=False,
                http_handlers=False,
            ),
        ),
        # schema-only with new args (ignoring)
        (
            False,
            True,
            False,
            False,
            False,
            False,
            True,
            BackupSources(
                access=False,
                data=True,
                schema=True,
                udf=False,
                named_collections=False,
                workload_entities=False,
                http_handlers=False,
            ),
        ),
    ],
)
# pylint: disable=too-many-positional-arguments
def test_sources_for_backup(
    access: bool,
    data: bool,
    schema: bool,
    udf: bool,
    named_collections: bool,
    workload_entities: bool,
    schema_only: bool,
    expected: BackupSources,
) -> None:
    assert (
        BackupSources.for_backup(
            access, data, schema, udf, named_collections, workload_entities, schema_only
        )
        == expected
    )


@pytest.mark.parametrize(
    [
        "access",
        "data",
        "schema",
        "udf",
        "named_collections",
        "workload_entities",
        "schema_only",
        "expected",
    ],
    [
        # default (all except access)
        (
            False,
            False,
            False,
            False,
            False,
            False,
            False,
            BackupSources(
                access=False,
                data=True,
                schema=True,
                udf=True,
                named_collections=True,
                workload_entities=True,
            ),
        ),
        # schema-only
        (
            False,
            False,
            False,
            False,
            False,
            False,
            True,
            BackupSources(
                access=False,
                data=False,
                schema=True,
                udf=True,
                named_collections=True,
                workload_entities=True,
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
            False,
            BackupSources(
                access=True,
                data=False,
                schema=False,
                udf=False,
                named_collections=False,
                workload_entities=False,
                http_handlers=False,
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
            False,
            BackupSources(
                access=False,
                data=True,
                schema=True,
                udf=False,
                named_collections=False,
                workload_entities=False,
                http_handlers=False,
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
            False,
            BackupSources(
                access=False,
                data=False,
                schema=True,
                udf=False,
                named_collections=False,
                workload_entities=False,
                http_handlers=False,
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
            False,
            BackupSources(
                access=False,
                data=True,
                schema=True,
                udf=False,
                named_collections=False,
                workload_entities=False,
                http_handlers=False,
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
            False,
            BackupSources(
                access=False,
                data=False,
                schema=False,
                udf=True,
                named_collections=False,
                workload_entities=False,
                http_handlers=False,
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
            False,
            BackupSources(
                access=False,
                data=False,
                schema=False,
                udf=False,
                named_collections=True,
                workload_entities=False,
                http_handlers=False,
            ),
        ),
        # only workload_entities
        (
            False,
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
                named_collections=False,
                workload_entities=True,
                http_handlers=False,
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
            False,
            BackupSources(
                access=True,
                data=False,
                schema=False,
                udf=True,
                named_collections=False,
                workload_entities=False,
                http_handlers=False,
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
            False,
            BackupSources(
                access=True,
                data=False,
                schema=True,
                udf=True,
                named_collections=False,
                workload_entities=False,
                http_handlers=False,
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
            False,
            BackupSources(
                access=False,
                data=True,
                schema=True,
                udf=True,
                named_collections=False,
                workload_entities=False,
                http_handlers=False,
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
            False,
            BackupSources(
                access=False,
                data=True,
                schema=True,
                udf=False,
                named_collections=True,
                workload_entities=False,
                http_handlers=False,
            ),
        ),
        # (combinations) workload_entities & data
        (
            False,
            True,
            False,
            False,
            False,
            True,
            False,
            BackupSources(
                access=False,
                data=True,
                schema=True,
                udf=False,
                named_collections=False,
                workload_entities=True,
                http_handlers=False,
            ),
        ),
        # all flags (like default)
        (
            True,
            True,
            True,
            True,
            True,
            True,
            False,
            BackupSources(
                access=True,
                data=True,
                schema=True,
                udf=True,
                named_collections=True,
                workload_entities=True,
                http_handlers=False,
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
            True,
            BackupSources(
                access=True,
                data=True,
                schema=True,
                udf=True,
                named_collections=True,
                workload_entities=True,
                http_handlers=False,
            ),
        ),
        # schema-only with new args (ignoring)
        (
            True,
            False,
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
                workload_entities=False,
                http_handlers=False,
            ),
        ),
        # schema-only with new args (ignoring)
        (
            False,
            True,
            False,
            False,
            False,
            False,
            True,
            BackupSources(
                access=False,
                data=True,
                schema=True,
                udf=False,
                named_collections=False,
                workload_entities=False,
                http_handlers=False,
            ),
        ),
    ],
)
# pylint: disable=too-many-positional-arguments
def test_sources_for_restore(
    access: bool,
    data: bool,
    schema: bool,
    udf: bool,
    named_collections: bool,
    workload_entities: bool,
    schema_only: bool,
    expected: BackupSources,
) -> None:
    assert (
        BackupSources.for_restore(
            access, data, schema, udf, named_collections, workload_entities, schema_only
        )
        == expected
    )


@pytest.mark.parametrize(
    ["config", "expected"],
    [
        ({}, BackupSources()),
        (
            {"schema": True},
            BackupSources(
                access=False,
                data=False,
                schema=True,
                udf=False,
                named_collections=False,
                workload_entities=False,
                http_handlers=False,
            ),
        ),
        (
            {"data": True, "access": True},
            BackupSources(
                access=True,
                data=True,
                schema=True,
                udf=False,
                named_collections=False,
                workload_entities=False,
                http_handlers=False,
            ),
        ),
        (None, BackupSources()),
        ({"schema": True, "future_source": True}, BackupSources()),
        ({"schema": True, "dat": True}, BackupSources()),
    ],
)
def test_sources_from_config(config: dict | None, expected: BackupSources) -> None:
    assert BackupSources.from_config(config) == expected


def test_sources_from_config_rejects_non_boolean_values() -> None:
    with pytest.raises(ConfigurationError):
        BackupSources.from_config({"schema": True, "data": "false"})
