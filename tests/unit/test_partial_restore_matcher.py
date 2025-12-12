"""
Unit tests partial restore matcher.
"""

import pytest

from ch_backup.logic.partial_restore import PartialRestoreFilter
from tests.unit.utils import parametrize


@parametrize(
    {
        "id": "Included filter and db presented",
        "args": {
            "patterns": ["db1.table1"],
            "inverted": False,
            "db_name": "db1",
            "result": True,
        },
    },
    {
        "id": "Included filter and db not presented",
        "args": {
            "patterns": ["db1.table1"],
            "inverted": False,
            "db_name": "db2",
            "result": False,
        },
    },
    {
        "id": "Included pattern filter and db presented",
        "args": {
            "patterns": ["db1.table*"],
            "inverted": False,
            "db_name": "db1",
            "result": True,
        },
    },
    {
        "id": "Included pattern filter and db not presented",
        "args": {
            "patterns": ["db1.*"],
            "inverted": False,
            "db_name": "db2",
            "result": False,
        },
    },
    {
        "id": "Included mixed filter and db presented",
        "args": {
            "patterns": ["db1.table1", "db2.*"],
            "inverted": False,
            "db_name": "db1",
            "result": True,
        },
    },
    {
        "id": "Excluded pattern filter and definitely not presented",
        "args": {
            "patterns": ["db1.*"],
            "inverted": True,
            "db_name": "db1",
            "result": False,
        },
    },
    {
        "id": "Excluded pattern filter and definitely presented",
        "args": {
            "patterns": ["db1.*"],
            "inverted": True,
            "db_name": "db2",
            "result": True,
        },
    },
    {
        "id": "Excluded filter and excluded database",
        "args": {
            "patterns": ["db1.table1"],
            "inverted": True,
            "db_name": "db2",
            "result": True,
        },
    },
    {
        "id": "Excluded filter and not enough info",
        "args": {
            "patterns": ["db1.table1", "db1.table2"],
            "inverted": True,
            "db_name": "db1",
            "result": True,
        },
    },
)
def test_contains_database(patterns, inverted, db_name, result):
    pattern_matcher = PartialRestoreFilter(patterns=patterns, inverted=inverted)
    assert pattern_matcher.is_possibly_contains_database(db_name) == result


@pytest.mark.parametrize(
    ("patterns", "db_name", "table_name", "included_result"),
    (
        [["db1.table1"], "db1", "table1", True],
        [["db1.table1"], "db1", "table2", False],
        [["db1.table1", "db1.table2"], "db1", "table2", True],
        [["db1.table1", "db2.*"], "db1", "table2", False],
        [["db1.table*"], "db1", "table2", True],
        [["db1.table*"], "db2", "table2", False],
        [["db1.table1", "db2.table2"], "db2", "table2", True],
        [["db1.table1", "db2.table2"], "db1", "table2", False],
        [["db1.table*", "db2.table2"], "db1", "table2", True],
        [["db1.*"], "db1", "abacaba", True],
        [["db1.*"], "db2", "abacaba", False],
        [["спец.символы"], "спец", "символы", True],
    ),
)
def test_contains_table(patterns, db_name, table_name, included_result):
    pattern_matcher = PartialRestoreFilter(patterns=patterns, inverted=False)
    assert pattern_matcher.accept_table(db_name, table_name) == included_result
    pattern_matcher_inverted = PartialRestoreFilter(patterns=patterns, inverted=True)
    assert pattern_matcher_inverted.accept_table(db_name, table_name) != included_result
