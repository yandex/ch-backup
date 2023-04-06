Feature: Restore backup from previous versions

  Background:
    Given default configuration
    And a working s3
    And a working zookeeper on zookeeper01
    And a working clickhouse on clickhouse01
    And a working clickhouse on clickhouse02

  Scenario: Restore backup with incomplete database metadata
    Given we have executed queries on clickhouse01
    """
    CREATE DATABASE test_db;

    CREATE TABLE test_db.table_01 (date Date, n Int32)
    ENGINE = MergeTree() PARTITION BY date ORDER BY date;
    INSERT INTO test_db.table_01 SELECT today(), number FROM system.numbers LIMIT 1000;

    CREATE TABLE test_db.table_02 (n Int32)
    ENGINE = MergeTree() PARTITION BY n % 10 ORDER BY n;
    INSERT INTO test_db.table_02 SELECT number FROM system.numbers LIMIT 1000;
    """
    When we create clickhouse01 clickhouse backup
    Then we got the following backups on clickhouse01
      | num | state    | data_count | link_count   |
      | 0   | created  | 11         | 0            |
    When metadata paths of clickhouse01 backup #0 was deleted
    """
    - databases.test_db.engine
    - databases.test_db.metadata_path
    """
    When we restore clickhouse backup #0 to clickhouse02
    Then clickhouse02 has same schema as clickhouse01
    And we got same clickhouse data at clickhouse01 clickhouse02
