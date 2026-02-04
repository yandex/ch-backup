"""
Clickhouse partial restore helper
"""

import fnmatch
from typing import List


class PartialRestorePattern:
    """
    Single pattern filter for filtering restoring tables
    """

    def __init__(self, database: str, pattern_str: str):
        self.database = database
        self.table_pattern = pattern_str

    @classmethod
    def from_pattern(cls, pattern: str) -> "PartialRestorePattern":
        """
        Create PartialRestorePattern instance from string
        """
        db, table = pattern.split(".", 1)
        return cls(database=db, pattern_str=table)

    def matches(self, db: str, table: str) -> bool:
        """
        Check if table matches filter by name or by regexp
        """
        if self.database != db:
            return False

        return fnmatch.fnmatch(table, self.table_pattern)

    def related_to_db(self, db: str) -> bool:
        """
        Has database in filter
        """
        return self.database == db


class PartialRestoreFilter:
    """
    Contains filtering logic for partial restore command
    """

    def __init__(self, inverted: bool, patterns: List[str]):
        """
        @param inverted: include(false) or exclude(true) matcher
        @param patterns: table patterns in format db_name.table_name with possible * in table_name
        """
        self.inverted = inverted
        self.patterns = (
            [PartialRestorePattern.from_pattern(x) for x in patterns]
            if patterns
            else []
        )

    def accept_table(self, db: str, table: str) -> bool:
        """
        Checks if table is suitable to filter.
        """
        if self.is_empty():
            return True

        for pattern in self.patterns:
            if pattern.matches(db, table):
                return not self.inverted

        return self.inverted

    def is_empty(self) -> bool:
        """
        Is filter accepting all tables
        """
        return len(self.patterns) == 0
