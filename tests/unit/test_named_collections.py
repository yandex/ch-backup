"""Unit tests for named collections restore helpers."""

# pylint: disable=protected-access

from unittest.mock import MagicMock, patch

import pytest

from ch_backup.logic.named_collections import NamedCollectionsBackup


@pytest.mark.parametrize(
    ("statement", "expected"),
    [
        (
            "CREATE NAMED COLLECTION collection AS key = 'value'",
            "CREATE NAMED COLLECTION IF NOT EXISTS collection AS key = 'value'",
        ),
        (
            "  create named collection collection AS key = 'value'",
            "  create named collection IF NOT EXISTS collection AS key = 'value'",
        ),
        (
            "CREATE NAMED COLLECTION IF NOT EXISTS collection AS key = 'value'",
            "CREATE NAMED COLLECTION IF NOT EXISTS collection AS key = 'value'",
        ),
    ],
)
def test_add_if_not_exists(statement, expected):
    assert NamedCollectionsBackup._add_if_not_exists(statement) == expected


def test_add_if_not_exists_rejects_non_named_collection_statement():
    with pytest.raises(RuntimeError, match="Expected CREATE NAMED COLLECTION"):
        NamedCollectionsBackup._add_if_not_exists("CREATE TABLE test (id UInt64)")


def _restore_context(existing_names, backup_statement):
    context = MagicMock()
    context.ch_ctl.ch_version_ge.return_value = True
    context.ch_ctl.get_named_collections_query.return_value = existing_names
    context.backup_meta.get_named_collections.return_value = ["collection"]
    context.backup_layout.get_named_collection_create_statement.return_value = (
        backup_statement
    )
    return context


@pytest.mark.parametrize(
    ("existing_names", "existing_statement", "drop_expected"),
    [
        (["collection"], "CREATE NAMED COLLECTION collection AS key = 'value'", False),
        (
            ["collection"],
            "CREATE NAMED COLLECTION IF NOT EXISTS collection AS key = 'value'",
            False,
        ),
        (["collection"], "CREATE NAMED COLLECTION collection AS key = 'old'", True),
        ([], None, False),
    ],
)
def test_restore_compares_existing_collection_and_creates_idempotently(
    existing_names, existing_statement, drop_expected
):
    backup_statement = "CREATE NAMED COLLECTION collection AS key = 'value'"
    context = _restore_context(existing_names, backup_statement)
    nc_config = MagicMock()

    with (
        patch(
            "ch_backup.logic.named_collections.NamedCollectionsStorageConfig."
            "from_ch_config",
            return_value=nc_config,
        ),
        patch.object(
            NamedCollectionsBackup,
            "_get_existing_create_statement",
            return_value=existing_statement,
        ) as get_existing,
    ):
        NamedCollectionsBackup().restore(context)

    if existing_names and not drop_expected:
        context.ch_ctl.drop_named_collection.assert_not_called()
        context.ch_ctl.restore_named_collection.assert_not_called()
        get_existing.assert_called_once_with(context, nc_config, "collection")
        return

    if drop_expected:
        context.ch_ctl.drop_named_collection.assert_called_once_with("collection")
        get_existing.assert_called_once_with(context, nc_config, "collection")
    else:
        get_existing.assert_not_called()

    context.ch_ctl.restore_named_collection.assert_called_once_with(
        "CREATE NAMED COLLECTION IF NOT EXISTS collection AS key = 'value'"
    )
