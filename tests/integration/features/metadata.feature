Feature: Metadata

  Background:
    Given default configuration
    And a working s3
    And a working zookeeper on zookeeper01
    And a working clickhouse on clickhouse01

  @require_version_22.3
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
    labels:
      environment: testing
      name: test_backup
    """
    And metadata of clickhouse01 backup #0 contains no
    """
    schema_only: true
    """

  @require_version_22.3
  Scenario: Create "schema-only" backup
    When we create clickhouse01 clickhouse backup
    """
    schema_only: true
    """
    Then metadata of clickhouse01 backup #0 contains
    """
    version: {{ version }}
    schema_only: true
    """
