"""Operation-specific matching keeps logical names and physical owners separate."""

from collections.abc import Callable
from functools import partial

import pytest

from ch_backup.backup.metadata import PartMetadata
from ch_backup.backup.part_matching import (
    backup_part_names_match,
    linked_part_matches,
    local_copy_key,
    materialized_part_key,
    materialized_part_matches,
    part_name_without_mutation,
)


@pytest.fixture(name="make_part")
def fixture_make_part() -> Callable[..., PartMetadata]:
    return partial(
        PartMetadata,
        database="db",
        table="table",
        name="all_1_2_0",
        checksum="checksum",
        size=100,
        files=["a.bin", "b.bin"],
        tarball=True,
        disk_name="default",
        encrypted=True,
        link="owner",
        link_part_name="all_1_2_0",
    )


@pytest.mark.parametrize(
    "name,expected",
    [
        ("all_1_2_0", ("all", 1, 2, 0)),
        ("all_1_2_0_7", ("all", 1, 2, 0)),
        ("20230101_20230122_19234_19448_5_20538", ("202301", 19234, 19448, 5)),
        ("20230101_20230122_20230123_0_20538", ("20230101", 20230122, 20230123, 0)),
    ],
)
def test_name_key_preserves_parser_interpretation(name, expected):
    assert part_name_without_mutation(name) == expected


@pytest.mark.parametrize("name", ["broken", "all_2_1_0", "all_1_2_x"])
def test_name_key_rejects_malformed_names(name):
    with pytest.raises(ValueError):
        part_name_without_mutation(name)


@pytest.mark.parametrize(
    "current,stored,expected",
    [
        ("all_1_2_0", "all_1_2_0", True),
        ("all_1_2_0_7", "all_1_2_0", True),
        ("all_1_2_0_7", "all_1_2_0_3", True),
        ("all_1_2_0", "other_1_2_0", False),
        ("all_1_2_0", "all_0_2_0", False),
        ("all_1_2_0", "all_1_3_0", False),
        ("all_1_2_0", "all_1_2_1", False),
        ("broken", "broken", True),
        (
            "20230101_20230122_19234_19448_5_20538",
            "20230101_20230122_19234_19448_5",
            True,
        ),
    ],
)
def test_backup_name_matching(current, stored, expected):
    assert backup_part_names_match(current, stored) is expected


@pytest.mark.parametrize(
    "current,stored",
    [("broken", "other"), ("broken", "all_1_2_0"), ("all_1_2_0", "broken")],
)
def test_backup_matching_rejects_unequal_malformed_names(current, stored):
    with pytest.raises(ValueError):
        backup_part_names_match(current, stored)


@pytest.mark.parametrize(
    "change",
    [
        {"database": "other"},
        {"table": "other"},
        {"name": "all_1_3_0"},
        {"checksum": "other"},
        {"disk_name": "other"},
        {"size": 101},
        {"tarball": False},
        {"encrypted": False},
        {"files": ["a.bin", "c.bin"]},
        {"files": ["a.bin", "b.bin", "b.bin"]},
    ],
)
def test_local_copy_rejects_incompatible_parts(make_part, change):
    assert local_copy_key(make_part()) != local_copy_key(make_part(**change))


def test_local_copy_ignores_mutation_file_order_and_source_owner(make_part):
    key = local_copy_key(make_part())
    candidate = make_part(
        name="all_1_2_0_7",
        files=["b.bin", "a.bin"],
        link="another_owner",
        link_part_name="all_1_2_0_3",
    )

    assert local_copy_key(candidate) in {key}


def test_local_copy_rejects_ambiguous_file_name_concatenation(make_part):
    assert local_copy_key(make_part(files=["a", "bc"])) != local_copy_key(
        make_part(files=["ab", "c"])
    )


@pytest.mark.parametrize("other,expected", [("broken", True), ("other", False)])
def test_local_copy_uses_exact_malformed_names(make_part, other, expected):
    assert (
        local_copy_key(make_part(name="broken"))
        == local_copy_key(make_part(name=other))
    ) is expected


@pytest.mark.parametrize(
    "change",
    [
        {"database": "other"},
        {"table": "other"},
        {"link": "other"},
        {"link_part_name": "all_1_2_0_7"},
        {"checksum": "other"},
        {"disk_name": "other"},
        {"tarball": False},
        {"encrypted": False},
    ],
)
def test_materialized_copy_rejects_other_dependencies(make_part, change):
    assert materialized_part_key(make_part()) != materialized_part_key(
        make_part(**change)
    )


def test_materialized_copy_reuses_physical_dependency_despite_logical_rename(make_part):
    key = materialized_part_key(make_part())

    assert materialized_part_key(make_part(name="all_1_2_0_7")) in {key}


def test_materialized_copy_uses_logical_name_without_stored_name(make_part):
    assert materialized_part_key(
        make_part(link_part_name=None)
    ) == materialized_part_key(make_part())
    assert materialized_part_key(
        make_part(link_part_name=None, name="all_1_2_0_7")
    ) != materialized_part_key(make_part())


def test_materialized_copy_preserves_absent_owner(make_part):
    key = materialized_part_key(make_part(link=None))

    assert key.source_backup is None
    assert key != materialized_part_key(make_part())


@pytest.mark.parametrize(
    "change,expected",
    [
        ({}, True),
        ({"database": "other"}, False),
        ({"table": "other"}, False),
        ({"name": "all_1_2_0_7"}, False),
        ({"checksum": "different"}, False),
        ({"link": "another_owner"}, False),
        ({"disk_name": "other"}, True),
        ({"tarball": False}, True),
        ({"encrypted": False}, True),
    ],
)
def test_existing_link_matches_physical_owner_without_format_checks(
    make_part, change, expected
):
    source = make_part(name="all_1_2_0_7")
    stored = make_part(**{"link": None, "link_part_name": None, **change})

    assert linked_part_matches(source, stored) is expected


@pytest.mark.parametrize(
    "change,expected",
    [
        ({}, True),
        ({"database": "other"}, False),
        ({"table": "other"}, False),
        ({"name": "all_1_2_0"}, False),
        ({"checksum": "different"}, False),
        ({"link": "another_owner"}, False),
        ({"disk_name": "other"}, False),
        ({"tarball": False}, False),
        ({"encrypted": False}, False),
    ],
)
def test_materialized_match_requires_logical_address_and_compatible_files(
    make_part, change, expected
):
    source = make_part(name="all_1_2_0_7")
    stored = make_part(
        **{"name": "all_1_2_0_7", "link": None, "link_part_name": None, **change}
    )

    assert materialized_part_matches(source, stored) is expected
