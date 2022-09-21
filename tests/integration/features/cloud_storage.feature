Feature: Backup & Clean & Restore

  Background:
    Given default configuration
    And a working s3
    And a working zookeeper on zookeeper01
    And a working clickhouse on clickhouse01
    And clickhouse on clickhouse01 has test schema

  @require_version_22.6
  Scenario: All backup data is deleted including data of removed tables
    Given ch-backup configuration on clickhouse01
    """
    backup:
        deduplicate_parts: True
    """
    And clickhouse01 has test clickhouse data test_deleted_table
    When we execute queries on clickhouse01
    """
    CREATE DATABASE IF NOT EXISTS test_db;
    CREATE TABLE test_db.deleted_table (
        CounterID UInt32,
        UserID UInt32
    )
    ENGINE = MergeTree()
    ORDER BY UserID
    SETTINGS storage_policy = 's3';
    INSERT INTO test_db.deleted_table VALUES(0, 42), (1, 777);
    """
    And we create clickhouse01 clickhouse backup
    """
    name: 12345678T123456
    """
    Then we got the following backups on clickhouse01
      | num | state   | data_count | link_count | title   |
      | 0   | created | 5          | 0          | deleted |
    And s3 contains 14 objects
    And s3 bucket cloud-storage-01 contains 10 objects
    And we got the following s3 backup directories on clickhouse01
    """
    ["12345678T123456"]
    """
    When we execute query on clickhouse01
    """
    DROP TABLE test_db.deleted_table;
    """
    And we delete clickhouse01 clickhouse backup #0
    Then we got the following backups on clickhouse01
      | num | state | data_count | link_count | title |
    And s3 contains 0 objects
    # Some files such as operation logs and schema version are not deleted
    And s3 bucket cloud-storage-01 contains 3 objects
    And we got the following s3 backup directories on clickhouse01
    """
    []
    """
