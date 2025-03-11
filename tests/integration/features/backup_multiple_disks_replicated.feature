Feature: Backup & Restore multiple disks and S3 with replication

  Background:
    Given default configuration
    And a working s3
    And a working zookeeper on zookeeper01
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
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/shard_01/test_db.table_01', '{replica}')
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

  @require_version_22.8
  Scenario Outline: Backup table with S3 storage policy
    Given ch-backup configuration on clickhouse01
    """
    cloud_storage:
        compression: <compression>
    """
    And we have executed queries on clickhouse01
    """
    CREATE DATABASE IF NOT EXISTS test_db;
    CREATE TABLE test_db.table_01 (
        CounterID UInt32,
        UserID UInt32
    )
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/shard_01/test_db.table_01', '{replica}')
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
    SELECT count() FROM system.parts WHERE table = 'table_01' and disk_name = 's3'
    """
    Then we get response
    """
    2
    """
    Then we got same clickhouse data at clickhouse01 clickhouse02

    Examples:
      | compression       |
      | True              |
      | False             |

  @require_version_22.8
  Scenario: Backup table with S3-cold storage policy
    Given we have executed queries on clickhouse01
    """
    CREATE DATABASE IF NOT EXISTS test_db;
    CREATE TABLE test_db.table_01 (
        CounterID UInt32,
        UserID UInt32
    )
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/shard_01/test_db.table_01', '{replica}')
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
    SELECT count() FROM system.parts WHERE table = 'table_01' and disk_name = 's3'
    """
    Then we get response
    """
    1
    """
    Then we got same clickhouse data at clickhouse01 clickhouse02

  @require_version_22.8
  Scenario Outline: Backup multiple tables with S3 storage policy
    Given ch-backup configuration on clickhouse01
    """
    cloud_storage:
        compression: <compression>
    """
    And we have executed queries on clickhouse01
    """
    CREATE DATABASE IF NOT EXISTS test_db;

    CREATE TABLE test_db.table_01 (CounterID UInt32, UserID UInt32)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/shard_01/test_db.table_01', '{replica}')
    ORDER BY UserID
    SETTINGS storage_policy = 's3';

    CREATE TABLE test_db.table_02 (CounterID UInt32, UserID UInt32)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/shard_01/test_db.table_02', '{replica}')
    ORDER BY UserID
    SETTINGS storage_policy = 's3';

    CREATE TABLE test_db.table_03 (CounterID UInt32, UserID UInt32)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/shard_01/test_db.table_03', '{replica}')
    ORDER BY UserID
    SETTINGS storage_policy = 's3';

    CREATE MATERIALIZED VIEW test_db.mview_01
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/shard_01/test_db.mview_01', '{replica}')
    PARTITION BY CounterID % 10
    ORDER BY CounterID
    SETTINGS storage_policy = 's3'
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
    SELECT count() FROM system.parts WHERE disk_name = 's3'
    """
    Then we get response
    """
    6
    """
    Then we got same clickhouse data at clickhouse01 clickhouse02

    Examples:
      | compression       |
      | True              |
      | False             |

  @require_version_22.8
  Scenario Outline: Multiple backups with S3 storage policy
    Given ch-backup configuration on clickhouse01
      """
      cloud_storage:
          compression: <compression>
      """
    When we execute queries on clickhouse01
    """
    CREATE DATABASE IF NOT EXISTS test_db;

    CREATE TABLE test_db.table_01 (
        CounterID UInt32,
        UserID UInt32
    )
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/shard_01/test_db.table_01', '{replica}')
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

    Examples:
      | compression       |
      | True              |
      | False             |

  Scenario Outline: Backup from object storage and <number> workers on restore.
    Given ch-backup configuration on clickhouse01
    """
      multiprocessing:
        cloud_storage_restore_workers: <number>
    """
    And we have executed queries on clickhouse01
    """
    CREATE DATABASE IF NOT EXISTS test_db;

    CREATE TABLE test_db.table_01 (CounterID UInt32, UserID UInt32)
    ENGINE = MergeTree() ORDER BY UserID SETTINGS storage_policy = 's3';

    CREATE TABLE test_db.table_02 (CounterID UInt32, UserID UInt32)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/shard_01/test_db.table_02', '{replica}')
    ORDER BY UserID
    SETTINGS storage_policy = 's3';

    INSERT INTO test_db.table_01 SELECT number%30, number FROM system.numbers LIMIT 1000;
    INSERT INTO test_db.table_02 SELECT number%30, number FROM system.numbers LIMIT 1000;
    """
    When we create clickhouse01 clickhouse backup
    When we restore clickhouse backup #0 to clickhouse02
    """
    cloud_storage_source_bucket: 'cloud-storage-01'
    cloud_storage_source_path: 'data'
    """
    Then we got same clickhouse data at clickhouse01 clickhouse02

    @require_version_22.8
    Examples:
    | number |
    | 1      |

    @require_version_23.3
    Examples:
    | number |
    | 4      |

    @require_version_24.2
    Examples:
    | number |
    | 8      |

Scenario: Inplace data restore on the another host.
  Given we use the same object storage bucket for clickhouse01 as on clickhouse02
  And ch-backup configuration on clickhouse02
  """
    restore:
      use_inplace_cloud_restore: True
  """
  And we have executed queries on clickhouse01
  """
  CREATE DATABASE IF NOT EXISTS test_db;

  CREATE TABLE test_db.table_01 (CounterID UInt32, UserID UInt32)
  ENGINE = MergeTree() ORDER BY UserID SETTINGS storage_policy = 's3';

  CREATE TABLE test_db.table_02 (CounterID UInt32, UserID UInt32)
  ENGINE = ReplicatedMergeTree('/clickhouse/tables/shard_01/test_db.table_02', '{replica}')
  ORDER BY UserID
  SETTINGS storage_policy = 's3';

  INSERT INTO test_db.table_01 SELECT number%30, number FROM system.numbers LIMIT 1000;
  INSERT INTO test_db.table_02 SELECT number%30, number FROM system.numbers LIMIT 1000;
  """
  When we create clickhouse01 clickhouse backup
  When we restore clickhouse backup #0 to clickhouse02
  """
  cloud_storage_source_bucket: 'cloud-storage-02'
  cloud_storage_source_path: 'data'
  """
  Then we got same clickhouse data at clickhouse01 clickhouse02

Scenario Outline: Inplace data restore on the same host with <name>
  Given ch-backup configuration on <host>
  """
    restore:
      use_inplace_cloud_restore: True
  """

  And we have executed queries on <host>
  """
  CREATE DATABASE IF NOT EXISTS test_db;

  CREATE TABLE test_db.table_01 (CounterID UInt32, UserID UInt32)
  ENGINE = MergeTree() ORDER BY UserID SETTINGS storage_policy = 's3';

  CREATE TABLE test_db.table_02 (CounterID UInt32, UserID UInt32)
  ENGINE = ReplicatedMergeTree('/clickhouse/tables/shard_01/test_db.table_02', '{replica}')
  ORDER BY UserID
  SETTINGS storage_policy = 's3';

  INSERT INTO test_db.table_01 SELECT number%30, number FROM system.numbers LIMIT 1000;
  INSERT INTO test_db.table_02 SELECT number%30, number FROM system.numbers LIMIT 1000;
  """

  When we save all user's data in context on <host>
  And we create <host> clickhouse backup
  And we execute query on <host>
  """
  DROP DATABASE test_db SYNC;
  """

  When we restore clickhouse backup #0 to <host>
  """
  cloud_storage_source_bucket: 'cloud-storage-02'
  cloud_storage_source_path: 'data'
  """
  Then the user's data equal to saved one on <host>
  Examples:
  | name                              | host          |
  | virtual hosted s3 endpoint        | clickhouse01  |
  | path style s3 endpoint            | clickhouse02  |
