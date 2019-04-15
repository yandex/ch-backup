Feature: Metadata

  Background:
    Given default configuration
    And a working s3
    And a working clickhouse on clickhouse01
    And clickhouse on clickhouse01 has test schema

  Scenario: Create backup
    Given clickhouse01 has test clickhouse data test1
    When we create clickhouse01 clickhouse backup
    """
    labels:
      name: test_backup
    """
    Then clickhouse01 backup #0 metadata contains
    """
    version: {{ version }}
    ch_version: {{ clickhouse_version('clickhouse01') }}
    labels:
      environment: testing
      name: test_backup
    """
