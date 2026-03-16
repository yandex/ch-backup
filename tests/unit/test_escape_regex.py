"""
Unit tests for escape function with regex special characters.
"""

from ch_backup.util import escape


def test_escape_sql_and_regex():
    """Test that escape function handles both SQL backticks and regex special characters"""
    # SQL backtick escaping only
    assert escape("normal_table") == "normal_table"
    assert escape("test`table") == r"test\`table"

    # Regex special characters escaping
    assert escape("test.table", regex=True) == r"test\.table"
    assert escape("test*table", regex=True) == r"test\*table"
    assert escape("test[1]", regex=True) == r"test\[1\]"
    assert escape("test(backup)", regex=True) == r"test\(backup\)"
    assert escape("test|table", regex=True) == r"test\|table"
    assert escape("test$table", regex=True) == r"test\$table"

    # Combined
    assert escape("test`.table", regex=True) == r"test\\`\.table"
    assert escape("test.table*[1]", regex=True) == r"test\.table\*\[1\]"
