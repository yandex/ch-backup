Feature: Backup & Restore multiple disks and S3

  Background:
    Given default configuration
    And a working s3
    And a working clickhouse on clickhouse01
    And a working clickhouse on clickhouse02

  @require_version_20.8
  Scenario: Backup table with multiple disks storage policy
    Given we have executed queries on clickhouse01
    """
    CREATE DATABASE IF NOT EXISTS test_db;
    CREATE TABLE test_db.table_01 ON CLUSTER 'cluster_name' (
        CounterID UInt32,
        UserID UInt32
    )
    ENGINE = MergeTree()
    PARTITION BY CounterID % 3
    ORDER BY UserID
    SETTINGS storage_policy = 'multiple_disks';

    INSERT INTO test_db.table_01 SELECT 0, number FROM system.numbers LIMIT 10;
    INSERT INTO test_db.table_01 SELECT 1, number FROM system.numbers LIMIT 10;
    INSERT INTO test_db.table_01 SELECT 2, number FROM system.numbers LIMIT 10;

    ALTER TABLE test_db.table_01 MOVE PARTITION '0' TO DISK 'hdd1';
    ALTER TABLE test_db.table_01 MOVE PARTITION '1' TO DISK 'hdd2';
    """
    When we create clickhouse01 clickhouse backup
    Then we got the following backups on clickhouse01
      | num | state   | data_count | link_count |
      | 0   | created | 3          | 0          |
    And metadata of clickhouse01 backup #0 contains no
    """
    s3_revisions:
      s3: 1
    """
    When we restore clickhouse backup #0 to clickhouse02
    Then we got same clickhouse data at clickhouse01 clickhouse02

  @require_version_21.1
  Scenario: Backup table with S3 storage policy
    Given we have executed queries on clickhouse01
    """
    CREATE DATABASE IF NOT EXISTS test_db;
    CREATE TABLE test_db.table_02 ON CLUSTER 'cluster_name' (
        CounterID UInt32,
        UserID UInt32
    )
    ENGINE = MergeTree()
    PARTITION BY CounterID % 3
    ORDER BY UserID
    SETTINGS storage_policy = 's3';

    INSERT INTO test_db.table_02 SELECT 0, number FROM system.numbers LIMIT 10;
    INSERT INTO test_db.table_02 SELECT 1, number FROM system.numbers LIMIT 10;
    """
    When we create clickhouse01 clickhouse backup
    Then metadata of clickhouse01 backup #0 contains
    """
    s3_revisions:
      s3: 1
    """

  @require_version_21.1
  Scenario: Backup table with S3-cold storage policy
    Given we have executed queries on clickhouse01
    """
    CREATE DATABASE IF NOT EXISTS test_db;
    CREATE TABLE test_db.table_03 ON CLUSTER 'cluster_name' (
        CounterID UInt32,
        UserID UInt32
    )
    ENGINE = MergeTree()
    PARTITION BY CounterID % 3
    ORDER BY UserID
    SETTINGS storage_policy = 's3_cold';

    INSERT INTO test_db.table_03 SELECT 0, number FROM system.numbers LIMIT 10;
    INSERT INTO test_db.table_03 SELECT 1, number FROM system.numbers LIMIT 10;

    ALTER TABLE test_db.table_03 MOVE PARTITION '1' TO DISK 's3';
    """
    When we create clickhouse01 clickhouse backup
    Then metadata of clickhouse01 backup #0 contains
    """
    s3_revisions:
      s3: 1
    """
