"""
Unit tests for WorkloadEntity.from_create_statement
"""

import pytest

from ch_backup.clickhouse.models import WorkloadEntityType
from ch_backup.logic.workload_entities import WorkloadEntity


@pytest.mark.parametrize(
    ["statement", "expected_name", "expected_type", "expected_parent"],
    [
        # Simple workload, no parent, no settings
        (
            "CREATE WORKLOAD all",
            "all",
            WorkloadEntityType.WORKLOAD,
            None,
        ),
        # Workload with backtick-quoted plain name
        (
            "CREATE WORKLOAD `all`",
            "all",
            WorkloadEntityType.WORKLOAD,
            None,
        ),
        # Workload with backtick-quoted name containing spaces
        (
            "CREATE WORKLOAD `space test`",
            "space test",
            WorkloadEntityType.WORKLOAD,
            None,
        ),
        # Workload with IN parent (plain names)
        (
            "CREATE WORKLOAD production IN all",
            "production",
            WorkloadEntityType.WORKLOAD,
            "all",
        ),
        # Workload with backtick-quoted name and backtick-quoted parent
        (
            "CREATE WORKLOAD `my workload` IN `root workload`",
            "my workload",
            WorkloadEntityType.WORKLOAD,
            "root workload",
        ),
        # Workload with SETTINGS
        (
            "CREATE WORKLOAD all SETTINGS max_bytes_per_second = 2147483648",
            "all",
            WorkloadEntityType.WORKLOAD,
            None,
        ),
        # Workload with IN and SETTINGS
        (
            "CREATE WORKLOAD production IN all SETTINGS max_bytes_per_second = 1073741824",
            "production",
            WorkloadEntityType.WORKLOAD,
            "all",
        ),
        # Workload with backtick-quoted names and SETTINGS
        (
            "CREATE WORKLOAD `space test` IN `root node` SETTINGS max_bytes_per_second = 500",
            "space test",
            WorkloadEntityType.WORKLOAD,
            "root node",
        ),
        # Workload with trailing semicolon
        (
            "CREATE WORKLOAD all;",
            "all",
            WorkloadEntityType.WORKLOAD,
            None,
        ),
        # Case-insensitive keyword
        (
            "create workload all",
            "all",
            WorkloadEntityType.WORKLOAD,
            None,
        ),
        # Simple resource
        (
            "CREATE RESOURCE s3_write (WRITE DISK s3)",
            "s3_write",
            WorkloadEntityType.RESOURCE,
            None,
        ),
        # Resource with backtick-quoted plain name
        (
            "CREATE RESOURCE `s3_write` (WRITE DISK s3)",
            "s3_write",
            WorkloadEntityType.RESOURCE,
            None,
        ),
        # Resource with backtick-quoted name containing spaces
        (
            "CREATE RESOURCE `my resource` (READ DISK s3)",
            "my resource",
            WorkloadEntityType.RESOURCE,
            None,
        ),
        # Resource with trailing semicolon
        (
            "CREATE RESOURCE s3_read (READ DISK s3);",
            "s3_read",
            WorkloadEntityType.RESOURCE,
            None,
        ),
        # Leading/trailing whitespace
        (
            "  CREATE WORKLOAD all  ",
            "all",
            WorkloadEntityType.WORKLOAD,
            None,
        ),
    ],
)
def test_from_create_statement(
    statement, expected_name, expected_type, expected_parent
):
    entity = WorkloadEntity.from_create_statement(statement)
    assert entity.name == expected_name
    assert entity.type == expected_type
    assert entity.parent == expected_parent
    assert entity.create_statement == statement
