"""
Unit tests partial restore matcher.
"""

import pytest

from ch_backup.logic.partial_restore import PartialRestoreFilter


@pytest.mark.parametrize(
    ("patterns", "db_name", "table_name", "included_result"),
    (
        [["db1.table1"], "db1", "table1", True],
        [["db1.table1"], "db1", "table2", False],
        [["db1.table1", "db1.table2"], "db1", "table2", True],
        [["db1.table1", "db2.*"], "db1", "table2", False],
        [["db1.table*"], "db1", "table2", True],
        [["db1.table*"], "db2", "table2", False],
        [["db1.*table*"], "db1", "pre_table_suf", True],
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
