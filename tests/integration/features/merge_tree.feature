Feature: Backup of MergeTree tables with different configurations

  Background:
    Given default configuration
    And a working s3
    And a working clickhouse on clickhouse01

  Scenario: Create backup
    Given test data on clickhouse01 that was created as follows
    """
    CREATE DATABASE test_db;
    CREATE TABLE test_db.table_01 (date Date, n Int32) ENGINE = MergeTree(date, date, 8192);
    INSERT INTO test_db.table_01 SELECT today(), number FROM system.numbers LIMIT 1000;
    CREATE TABLE test_db.table_02 (date Date, n Int32) ENGINE = MergeTree() PARTITION BY date ORDER BY date;
    INSERT INTO test_db.table_02 SELECT today(), number FROM system.numbers LIMIT 1000;
    CREATE TABLE test_db.table_03 (n Int32) ENGINE = MergeTree() PARTITION BY n % 10 ORDER BY n;
    INSERT INTO test_db.table_03 SELECT number FROM system.numbers LIMIT 1000;
    """
    When we create clickhouse01 clickhouse backup
    Then ch_backup entries of clickhouse01 are in proper condition
      | num | state    | data_count | link_count   | title         |
      | 0   | created  | 12         | 0            | data          |

  Scenario: Restore from backup
    Given a working clickhouse on clickhouse02
    When we restore clickhouse #0 backup to clickhouse02
    Then we got same clickhouse data at clickhouse01 clickhouse02
