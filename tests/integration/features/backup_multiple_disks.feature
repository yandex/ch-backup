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
    CREATE TABLE test_db.table_01 (
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
    When we restore clickhouse backup #0 to clickhouse02
    Then we got same clickhouse data at clickhouse01 clickhouse02

  @require_version_21.6
  Scenario: Backup table with S3 storage policy
    Given we have executed queries on clickhouse01
    """
    CREATE DATABASE IF NOT EXISTS test_db;
    CREATE TABLE test_db.table_01 (
        CounterID UInt32,
        UserID UInt32
    )
    ENGINE = MergeTree()
    PARTITION BY CounterID % 3
    ORDER BY UserID
    SETTINGS storage_policy = 's3';

    INSERT INTO test_db.table_01 SELECT 0, number FROM system.numbers LIMIT 10;
    INSERT INTO test_db.table_01 SELECT 1, number FROM system.numbers LIMIT 10;
    """
    When we create clickhouse01 clickhouse backup
    Then we got the following backups on clickhouse01
      | num | state   | data_count | link_count |
      | 0   | created | 2          | 0          |
    When we restore clickhouse backup #0 to clickhouse02
    """
    cloud_storage_source_bucket: 'cloud-storage-01'
    cloud_storage_source_path: 'data'
    """
    And we execute query on clickhouse02
    """
    SELECT count(*) cnt FROM system.parts WHERE table = 'table_01' and disk_name = 's3'
    """
    Then we get response
    """
    2
    """
    Then we got same clickhouse data at clickhouse01 clickhouse02

  @require_version_21.6
  Scenario: Backup table with S3-cold storage policy
    Given we have executed queries on clickhouse01
    """
    CREATE DATABASE IF NOT EXISTS test_db;
    CREATE TABLE test_db.table_01 (
        CounterID UInt32,
        UserID UInt32
    )
    ENGINE = MergeTree()
    PARTITION BY CounterID % 3
    ORDER BY UserID
    SETTINGS storage_policy = 's3_cold';

    INSERT INTO test_db.table_01 SELECT 0, number FROM system.numbers LIMIT 10;
    INSERT INTO test_db.table_01 SELECT 1, number FROM system.numbers LIMIT 10;

    ALTER TABLE test_db.table_01 MOVE PARTITION '1' TO DISK 's3';
    """
    When we create clickhouse01 clickhouse backup
    Then we got the following backups on clickhouse01
      | num | state   | data_count | link_count |
      | 0   | created | 2          | 0          |
    When we restore clickhouse backup #0 to clickhouse02
    """
    cloud_storage_source_bucket: 'cloud-storage-01'
    cloud_storage_source_path: 'data'
    """
    And we execute query on clickhouse02
    """
    SELECT count(*) cnt FROM system.parts WHERE table = 'table_01' and disk_name = 's3'
    """
    Then we get response
    """
    1
    """
    Then we got same clickhouse data at clickhouse01 clickhouse02

  @require_version_21.6
  Scenario: Backup multiple tables with S3 storage policy
    Given we have executed queries on clickhouse01
    """
    CREATE DATABASE IF NOT EXISTS test_db;

    CREATE TABLE test_db.table_01 (CounterID UInt32, UserID UInt32)
    ENGINE = MergeTree() ORDER BY UserID SETTINGS storage_policy = 's3';

    CREATE TABLE test_db.table_02 (CounterID UInt32, UserID UInt32)
    ENGINE = MergeTree() ORDER BY UserID SETTINGS storage_policy = 's3';

    CREATE TABLE test_db.table_03 (CounterID UInt32, UserID UInt32)
    ENGINE = MergeTree() ORDER BY UserID SETTINGS storage_policy = 's3';

    CREATE MATERIALIZED VIEW test_db.mview_01
    ENGINE = MergeTree() PARTITION BY CounterID % 10 ORDER BY CounterID SETTINGS storage_policy = 's3'
    AS SELECT CounterID, CounterID * CounterID AS "CounterID2"
    FROM test_db.table_01;

    INSERT INTO test_db.table_01 SELECT 0, number FROM system.numbers LIMIT 1000;
    INSERT INTO test_db.table_01 SELECT 1, number FROM system.numbers LIMIT 1000;
    INSERT INTO test_db.table_02 SELECT 0, number FROM system.numbers LIMIT 1000;
    INSERT INTO test_db.table_03 SELECT 0, number FROM system.numbers LIMIT 1000;
    """
    When we create clickhouse01 clickhouse backup
    Then we got the following backups on clickhouse01
      | num | state   | data_count | link_count |
      | 0   | created | 6          | 0          |
    When we restore clickhouse backup #0 to clickhouse02
    """
    cloud_storage_source_bucket: 'cloud-storage-01'
    cloud_storage_source_path: 'data'
    """
    And we execute query on clickhouse02
    """
    SELECT count(*) cnt FROM system.parts WHERE disk_name = 's3'
    """
    Then we get response
    """
    6
    """
    Then we got same clickhouse data at clickhouse01 clickhouse02

  @require_version_21.6
  Scenario: Multiple backups with S3 storage policy
    When we execute queries on clickhouse01
    """
    CREATE DATABASE IF NOT EXISTS test_db;

    CREATE TABLE test_db.table_01 (
        CounterID UInt32,
        UserID UInt32
    )
    ENGINE = MergeTree()
    PARTITION BY CounterID % 3
    ORDER BY UserID
    SETTINGS storage_policy = 's3';
    """
    And we execute query on clickhouse01
    """
    INSERT INTO test_db.table_01 SELECT 0, number FROM system.numbers LIMIT 1000;
    """
    And we create clickhouse01 clickhouse backup
    And we execute query on clickhouse01
    """
    INSERT INTO test_db.table_01 SELECT 1, number FROM system.numbers LIMIT 1000;
    """
    And we create clickhouse01 clickhouse backup
    And we restore clickhouse backup #0 to clickhouse02
    """
    cloud_storage_source_bucket: 'cloud-storage-01'
    cloud_storage_source_path: 'data'
    """
    Then we got same clickhouse data at clickhouse01 clickhouse02
    When we execute query on clickhouse01
    """
    ALTER TABLE test_db.table_01 DROP PARTITION '1';
    """
    And we drop restore context at clickhouse02
    And we drop all databases at clickhouse02
    And we restore clickhouse backup #1 to clickhouse02
    """
    cloud_storage_source_bucket: 'cloud-storage-01'
    cloud_storage_source_path: 'data'
    """
    Then we got same clickhouse data at clickhouse01 clickhouse02

  @require_version_21.6
  Scenario: Resetup with S3 storage policy
    Given we have executed queries on clickhouse01
    """
    CREATE DATABASE IF NOT EXISTS test_db;
    CREATE TABLE test_db.table_01 (
        CounterID UInt32,
        UserID UInt32
    )
    ENGINE = MergeTree()
    PARTITION BY CounterID % 3
    ORDER BY UserID
    SETTINGS storage_policy = 's3';

    INSERT INTO test_db.table_01 SELECT 0, number FROM system.numbers LIMIT 10;
    INSERT INTO test_db.table_01 SELECT 1, number FROM system.numbers LIMIT 10;
    """
    When we create clickhouse01 clickhouse backup
    Then we got the following backups on clickhouse01
      | num | state   | data_count | link_count |
      | 0   | created | 2          | 0          |
    When we restore clickhouse backup #0 to clickhouse02
    """
    cloud_storage_source_bucket: 'cloud-storage-01'
    cloud_storage_source_path: 'data'
    """
    When we dirty remove clickhouse data at clickhouse01
    When we restore clickhouse backup #0 to clickhouse01
    """
    cloud_storage_source_bucket: 'cloud-storage-01'
    cloud_storage_source_path: 'data'
    cloud_storage_latest: true
    """
    And we execute query on clickhouse02
    """
    SELECT count(*) cnt FROM system.parts WHERE table = 'table_01' and disk_name = 's3'
    """
    Then we get response
    """
    2
    """
    Then we got same clickhouse data at clickhouse01 clickhouse02