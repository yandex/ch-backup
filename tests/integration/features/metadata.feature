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

  @require_version_22.3
  Scenario: Check fail reason
    Given we have executed queries on clickhouse01
    """
    CREATE TABLE foo (
        id UInt32
    ) 
    ENGINE = MergeTree()
    ORDER BY id;

    INSERT INTO foo SELECT number FROM numbers(100);
    """
    Given we have executed command on clickhouse01
    """
    rm -rf /var/lib/clickhouse/data/default/foo/all_1_1_0
    """
    When we try to create clickhouse01 clickhouse backup
    Then metadata of clickhouse01 backup #0 contains value for "fail_reason" which begins with
    """
    FileNotFoundError: [Errno 2] No such file or directory
    """
