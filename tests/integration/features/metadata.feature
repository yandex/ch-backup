Feature: Metadata

  Background:
    Given default configuration
    And a working s3
    And a working clickhouse on clickhouse01

  Scenario: Create backup
    Given ch-backup configuration on clickhouse01
    """
    backup:
      labels:
        environment: testing
    """
    When we create clickhouse01 clickhouse backup
    """
    labels:
      name: test_backup
    """
    Then metadata of clickhouse01 backup #0 contains
    """
    version: {{ version }}
    ch_version: {{ clickhouse_version('clickhouse01') }}
    labels:
      environment: testing
      name: test_backup
    """
