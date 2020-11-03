@dependent-scenarios
Feature: Backup & Restore

  Background:
    Given default configuration
    And a working s3
    And a working zookeeper on zookeeper01
    And a working clickhouse on clickhouse01
    And a working clickhouse on clickhouse02
    And clickhouse on clickhouse01 has test schema

  Scenario: Create backup
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

  Scenario: Create backup with increments
    Given clickhouse01 has test clickhouse data test2
    When we create clickhouse01 clickhouse backup
    Then we got the following backups on clickhouse01
      | num | state    | data_count | link_count   | title         |
      | 0   | created  | 4          | 4            | data+links    |
      | 1   | created  | 4          | 0            | shared        |

  Scenario: Restore from backup
    When we restore clickhouse backup #0 to clickhouse02
    Then we got same clickhouse data at clickhouse01 clickhouse02

  Scenario: Restore schema from backup without data
    When we drop all databases at clickhouse02
    And we restore clickhouse backup #1 to clickhouse02
    """
    schema_only: true
    """
    Then clickhouse02 has same schema as clickhouse01
    But on clickhouse02 tables are empty

  Scenario: Create a "schema-only"
    When we create clickhouse01 clickhouse backup
    """
    schema_only: True
    """
    Then we got the following backups on clickhouse01
      | num | state    | data_count | link_count   | title         |
      | 0   | created  | 0          | 0            | schema-only   |
      | 1   | created  | 4          | 4            | data+links    |
      | 2   | created  | 4          | 0            | shared        |

  Scenario: Restore from "schema-only" backup
    When we drop all databases at clickhouse02
    And we restore clickhouse backup #0 to clickhouse02
    Then clickhouse02 has same schema as clickhouse01
    But on clickhouse02 tables are empty

  Scenario: Restore from "schema-only" backup without data
    When we drop all databases at clickhouse02
    And we restore clickhouse backup #0 to clickhouse02
    """
    schema_only: true
    """
    Then clickhouse02 has same schema as clickhouse01
    But on clickhouse02 tables are empty

  Scenario: Backup & Restore with multipart upload/download
    When we drop all databases at clickhouse01
    And we drop all databases at clickhouse02
    Given we have executed queries on clickhouse01
    """
    CREATE DATABASE test_db;
    CREATE TABLE test_db.table_01 (n Int32) ENGINE = MergeTree() PARTITION BY n % 1 ORDER BY n;
    INSERT INTO test_db.table_01 SELECT number FROM system.numbers LIMIT 10000000;
    """
    When we create clickhouse01 clickhouse backup
    And we restore clickhouse backup #0 to clickhouse02
    Then we got same clickhouse data at clickhouse01 clickhouse02
# TODO: check deduplication with overdue backups