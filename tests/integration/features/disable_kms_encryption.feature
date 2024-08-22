Feature: Disable KMS encryption (since there is server-side kms encryption)

  Background:
    Given default configuration
    And a working s3
    And a working zookeeper on zookeeper01
    And a working clickhouse on clickhouse01
    And a working clickhouse on clickhouse02
    And clickhouse on clickhouse01 has test schema

  Scenario: Create backup, disable encryption and restore from backup
    Given we have executed queries on clickhouse01
    """
    CREATE TABLE test_db_01.table_rus (
        EventDate DateTime,
        CounterID UInt32,
        `Пользователь` UInt32
    )
    ENGINE = MergeTree()
    PARTITION BY CounterID % 10
    ORDER BY (CounterID, EventDate)
    """
    And clickhouse01 has test clickhouse data test1
    When we create clickhouse01 clickhouse backup
    Then we got the following backups on clickhouse01
      | num | state    | data_count | link_count   | title         |
      | 0   | created  | 4          | 0            | data          |
    When we restore clickhouse backup #0 to clickhouse02
    Then we got same clickhouse data at clickhouse01 clickhouse02

  Scenario: Disable encryption, create backup and restore from backup
    Given we have executed queries on clickhouse01
    """
    CREATE TABLE test_db_01.table_rus (
        EventDate DateTime,
        CounterID UInt32,
        `Пользователь` UInt32
    )
    ENGINE = MergeTree()
    PARTITION BY CounterID % 10
    ORDER BY (CounterID, EventDate)
    """
    And clickhouse01 has test clickhouse data test1
    When we create clickhouse01 clickhouse backup
    Then we got the following backups on clickhouse01
      | num | state    | data_count | link_count   | title         |
      | 0   | created  | 4          | 0            | data          |
    When we restore clickhouse backup #0 to clickhouse02
    Then we got same clickhouse data at clickhouse01 clickhouse02
