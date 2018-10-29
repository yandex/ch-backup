Feature: Lables

  Background:
    Given default configuration
    And a working s3
    And a working clickhouse on clickhouse01
    And clickhouse on clickhouse01 has test schema

  Scenario: Create backup with labels
    Given clickhouse01 has test clickhouse data test1
    When we create clickhouse01 clickhouse backup
    """
    labels:
      name: test_backup
    """
    Then clickhouse01 backup #0 metadata contains
    """
    labels:
      environment: testing
      name: test_backup
    """
