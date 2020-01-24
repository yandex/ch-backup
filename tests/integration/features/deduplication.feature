Feature: Deduplication

  Background:
    Given default configuration
    And a working s3
    And a working zookeeper on zookeeper01
    And a working clickhouse on clickhouse01
    And a working clickhouse on clickhouse02
    And we have executed queries on clickhouse01
    """
    CREATE DATABASE test_db1;

    CREATE TABLE test_db1.test_table1 (partition_id Int32, n Int32)
    ENGINE = MergeTree() PARTITION BY partition_id ORDER BY (partition_id, n);

    INSERT INTO test_db1.test_table1 SELECT number % 2, number FROM system.numbers LIMIT 100;
    """
    And ch-backup configuration on clickhouse01
    """
    backup:
        deduplicate_parts: True
    """
    And we have created clickhouse01 clickhouse backup
    And we have executed queries on clickhouse01
    """
    CREATE TABLE test_db1.test_table2 (partition_id Int32, n Int32)
    ENGINE = MergeTree() PARTITION BY partition_id ORDER BY (partition_id, n);

    INSERT INTO test_db1.test_table2 SELECT number % 3, number FROM system.numbers LIMIT 100;
    """

  Scenario: Create backup with enabled deduplication
    When we create clickhouse01 clickhouse backup
    Then we got the following backups on clickhouse01
      | num | state    | data_count | link_count   |
      | 0   | created  | 3          | 2            |
      | 1   | created  | 2          | 0            |
    When we restore clickhouse backup #0 to clickhouse02
    Then we got same clickhouse data at clickhouse01 clickhouse02

  Scenario: Create backup with disabled deduplication
    Given ch-backup configuration on clickhouse01
    """
    backup:
        deduplicate_parts: False
    """
    When we create clickhouse01 clickhouse backup
    Then we got the following backups on clickhouse01
      | num | state    | data_count | link_count   |
      | 0   | created  | 5          | 0            |
      | 1   | created  | 2          | 0            |
    When we restore clickhouse backup #0 to clickhouse02
    Then we got same clickhouse data at clickhouse01 clickhouse02

  Scenario Outline: Failed backups are used in deduplication
    Given metadata of clickhouse01 backup #0 was adjusted with
    """
    meta:
        state: <state>
    """
    And file "data/test_db1/test_table1/0_1_1_0/n.bin" was deleted from clickhouse01 backup #0
    When we create clickhouse01 clickhouse backup
    Then we got the following backups on clickhouse01
      | num | state    | data_count | link_count   |
      | 0   | created  | 4          | 1            |
      | 1   | <state>  | 2          | 0            |
    When we restore clickhouse backup #0 to clickhouse02
    Then we got same clickhouse data at clickhouse01 clickhouse02

    Examples:
      | state             |
      | creating          |
      | failed            |

  Scenario Outline: Deleting or partially deletded backups are not used in deduplication
    Given metadata of clickhouse01 backup #0 was adjusted with
    """
    meta:
        state: <state>
    """
    When we create clickhouse01 clickhouse backup
    Then we got the following backups on clickhouse01
      | num | state    | data_count | link_count   |
      | 0   | created  | 5          | 0            |
      | 1   | <state>  | 2          | 0            |
    When we restore clickhouse backup #0 to clickhouse02
    Then we got same clickhouse data at clickhouse01 clickhouse02

    Examples:
      | state             |
      | deleting          |
      | partially_deleted |

  Scenario: Backups mismatched age policy are not used in deduplication
    Given ch-backup configuration on clickhouse01
    """
    backup:
        deduplication_age_limit:
            days: 7
    """
    Given metadata of clickhouse01 backup #0 was adjusted with
    """
    meta:
        state: created
        start_time: {{ backup.start_time | decrease_on('7 days') }}
        end_time: {{ backup.end_time | decrease_on('7 days') }}
    """
    When we create clickhouse01 clickhouse backup
    Then we got the following backups on clickhouse01
      | num | state    | data_count | link_count   |
      | 0   | created  | 5          | 0            |
      | 1   | created  | 2          | 0            |
    When we restore clickhouse backup #0 to clickhouse02
    Then we got same clickhouse data at clickhouse01 clickhouse02
