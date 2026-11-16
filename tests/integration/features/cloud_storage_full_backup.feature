Feature: Full backup of cloud storage data

  Background:
    Given default configuration
    And a working s3
    And a working zookeeper on zookeeper01
    And a working clickhouse on clickhouse01
    And a working clickhouse on clickhouse02

  @object_storage_copy
  @require_version_24.1
  Scenario: Deleting a backup deletes copied cloud storage data
    Given we have executed queries on clickhouse01
    """
    CREATE DATABASE IF NOT EXISTS test_db;
    CREATE TABLE test_db.table_s3 (
        CounterID UInt32,
        UserID    UInt32
    )
    ENGINE = MergeTree()
    ORDER BY UserID
    SETTINGS storage_policy = 's3';

    INSERT INTO test_db.table_s3 SELECT 0, number FROM system.numbers LIMIT 100;
    """
    # ClickHouse writes cloud storage data under a name with '-' replaced by '_',
    # so a dashed name is the case where deletion can miss the data.
    When we create clickhouse01 clickhouse backup
    """
    name: test-backup
    copy_cloud_storage_data: true
    """
    Then s3 bucket ch-backup contains objects with prefix "ch_backup/test_backup/cloud_storage/s3/"
    When we delete clickhouse01 clickhouse backup #0
    Then s3 bucket ch-backup contains no objects with prefix "ch_backup/test_backup/"
    And s3 bucket ch-backup contains no objects with prefix "ch_backup/test-backup/"

  @object_storage_copy
  @require_version_22.8
  Scenario: Restore without copied data still requires the source bucket
    Given we have executed queries on clickhouse01
    """
    CREATE DATABASE IF NOT EXISTS test_db;
    CREATE TABLE test_db.table_s3 (
        CounterID UInt32,
        UserID    UInt32
    )
    ENGINE = MergeTree()
    ORDER BY UserID
    SETTINGS storage_policy = 's3';

    INSERT INTO test_db.table_s3 SELECT 0, number FROM system.numbers LIMIT 10;
    """
    When we create clickhouse01 clickhouse backup
    """
    name: test_backup
    """
    And we try to execute command on clickhouse02
    """
    ch-backup -c /etc/yandex/ch-backup/ch-backup.conf restore test_backup
    """
    Then we get response contains
    """
    Cloud storage source bucket must be set
    """

  @object_storage_copy
  @require_version_24.1
  Scenario: Inplace restore refuses a backup with copied cloud storage data
    Given ch-backup configuration on clickhouse02
    """
    restore:
      use_inplace_cloud_restore: True
    """
    And we have executed queries on clickhouse01
    """
    CREATE DATABASE IF NOT EXISTS test_db;
    CREATE TABLE test_db.table_s3 (
        CounterID UInt32,
        UserID    UInt32
    )
    ENGINE = MergeTree()
    ORDER BY UserID
    SETTINGS storage_policy = 's3';

    INSERT INTO test_db.table_s3 SELECT 0, number FROM system.numbers LIMIT 10;
    """
    When we create clickhouse01 clickhouse backup
    """
    name: test_backup
    copy_cloud_storage_data: true
    """
    And we try to execute command on clickhouse02
    """
    ch-backup -c /etc/yandex/ch-backup/ch-backup.conf restore test_backup
    """
    Then we get response contains
    """
    it cannot be restored with use_inplace_cloud_restore
    """

  @object_storage_copy
  @require_version_24.1
  Scenario Outline: Restore from a copy of cloud storage data of <part_format> parts
    Given we have executed queries on clickhouse01
    """
    CREATE DATABASE IF NOT EXISTS test_db;
    CREATE TABLE test_db.table_s3 (
        CounterID UInt32,
        UserID    UInt32,
        Payload   String
    )
    ENGINE = MergeTree()
    PARTITION BY CounterID % 8
    ORDER BY UserID
    SETTINGS storage_policy = 's3', min_bytes_for_wide_part = <min_bytes_for_wide_part>;

    SYSTEM STOP MERGES test_db.table_s3;

    INSERT INTO test_db.table_s3 SELECT number, number, repeat('a', 128) FROM system.numbers LIMIT 2000;
    INSERT INTO test_db.table_s3 SELECT number, number, repeat('b', 128) FROM system.numbers LIMIT 2000;
    INSERT INTO test_db.table_s3 SELECT number, number, repeat('c', 128) FROM system.numbers LIMIT 2000;
    """
    When we execute query on clickhouse01
    """
    SELECT count() FROM system.parts
    WHERE database = 'test_db' AND table = 'table_s3' AND active AND part_type != '<part_format>'
    """
    Then we get response
    """
    0
    """
    When we save all user's data in context on clickhouse01
    And we save data part checksums in context on clickhouse01
    And we create clickhouse01 clickhouse backup
    """
    name: test_backup
    copy_cloud_storage_data: true
    """
    Then s3 bucket ch-backup contains objects with prefix "ch_backup/test_backup/cloud_storage/s3/"
    When we execute command on clickhouse01
    """
    ch-backup -c /etc/yandex/ch-backup/ch-backup.conf show --pretty test_backup
    """
    Then we get response contains
    """
    "data_copied": true
    """
    # Without this step the restore would silently read the original objects
    # and the scenario would pass even if nothing had been copied.
    When we delete all objects in s3 bucket cloud-storage-01
    Then s3 bucket cloud-storage-01 contains 0 objects
    When we restore clickhouse backup #0 to clickhouse02
    Then the user's data equal to saved one on clickhouse02
    And data part checksums equal to saved ones on clickhouse02

    Examples:
      | part_format | min_bytes_for_wide_part |
      | Compact     | 10000000                |
      | Wide        | 0                       |

  @object_storage_copy
  @require_version_24.1
  Scenario: Restore from a copy of cloud storage data spread over two disks
    Given we have executed queries on clickhouse01
    """
    CREATE DATABASE IF NOT EXISTS test_db;
    CREATE TABLE test_db.table_s3 (
        CounterID UInt32,
        UserID    UInt32,
        Payload   String
    )
    ENGINE = MergeTree()
    PARTITION BY CounterID
    ORDER BY UserID
    SETTINGS storage_policy = 'multiple_s3';

    INSERT INTO test_db.table_s3 SELECT 0, number, repeat('a', 256) FROM system.numbers LIMIT 1000;
    INSERT INTO test_db.table_s3 SELECT 1, number, repeat('b', 256) FROM system.numbers LIMIT 1000;

    ALTER TABLE test_db.table_s3 MOVE PARTITION 1 TO DISK 's3_second';
    """
    # Without this check the scenario silently degrades to the single disk case.
    When we execute query on clickhouse01
    """
    SELECT countDistinct(disk_name) FROM system.parts
    WHERE database = 'test_db' AND table = 'table_s3' AND active
    """
    Then we get response
    """
    2
    """
    When we save all user's data in context on clickhouse01
    And we save data part checksums in context on clickhouse01
    And we create clickhouse01 clickhouse backup
    """
    name: test_backup
    copy_cloud_storage_data: true
    """
    Then s3 bucket ch-backup contains objects with prefix "ch_backup/test_backup/cloud_storage/s3/"
    And s3 bucket ch-backup contains objects with prefix "ch_backup/test_backup/cloud_storage/s3_second/"
    When we delete all objects in s3 bucket cloud-storage-01
    And we restore clickhouse backup #0 to clickhouse02
    Then the user's data equal to saved one on clickhouse02
    And data part checksums equal to saved ones on clickhouse02

  @object_storage_copy
  @require_version_24.1
  Scenario: Restore a table stored on both a local and a cloud storage disk
    Given we have executed queries on clickhouse01
    """
    CREATE DATABASE IF NOT EXISTS test_db;
    CREATE TABLE test_db.table_s3 (
        CounterID UInt32,
        UserID    UInt32,
        Payload   String
    )
    ENGINE = MergeTree()
    PARTITION BY CounterID
    ORDER BY UserID
    SETTINGS storage_policy = 's3_cold';

    INSERT INTO test_db.table_s3 SELECT 0, number, repeat('a', 256) FROM system.numbers LIMIT 1000;
    INSERT INTO test_db.table_s3 SELECT 1, number, repeat('b', 256) FROM system.numbers LIMIT 1000;

    ALTER TABLE test_db.table_s3 MOVE PARTITION 1 TO VOLUME 'external';
    """
    When we execute query on clickhouse01
    """
    SELECT count() FROM system.parts
    WHERE database = 'test_db' AND table = 'table_s3' AND active AND disk_name = 'default'
    """
    Then we get response
    """
    1
    """
    When we save all user's data in context on clickhouse01
    And we save data part checksums in context on clickhouse01
    And we create clickhouse01 clickhouse backup
    """
    name: test_backup
    copy_cloud_storage_data: true
    """
    Then s3 bucket ch-backup contains objects with prefix "ch_backup/test_backup/cloud_storage/s3/"
    When we delete all objects in s3 bucket cloud-storage-01
    And we restore clickhouse backup #0 to clickhouse02
    Then the user's data equal to saved one on clickhouse02
    And data part checksums equal to saved ones on clickhouse02

  @object_storage_copy
  @require_version_24.1
  Scenario: Backup of a table without data on a cloud storage disk needs no source bucket
    Given we have executed queries on clickhouse01
    """
    CREATE DATABASE IF NOT EXISTS test_db;
    CREATE TABLE test_db.table_s3 (
        CounterID UInt32,
        UserID    UInt32
    )
    ENGINE = MergeTree()
    ORDER BY UserID
    SETTINGS storage_policy = 's3';
    """
    When we create clickhouse01 clickhouse backup
    """
    name: test_backup
    copy_cloud_storage_data: true
    """
    Then s3 bucket ch-backup contains no objects with prefix "ch_backup/test_backup/cloud_storage/"
    When we restore clickhouse backup #0 to clickhouse02
    Then clickhouse02 has same schema as clickhouse01
    And on clickhouse02 tables are empty

  @object_storage_copy
  @require_version_24.1
  Scenario: Repeated backup of an unchanged table copies no data
    Given we have executed queries on clickhouse01
    """
    CREATE DATABASE IF NOT EXISTS test_db;
    CREATE TABLE test_db.table_s3 (
        CounterID UInt32,
        UserID    UInt32,
        Payload   String
    )
    ENGINE = MergeTree()
    PARTITION BY CounterID
    ORDER BY UserID
    SETTINGS storage_policy = 's3';

    INSERT INTO test_db.table_s3 SELECT 0, number, repeat('a', 256) FROM system.numbers LIMIT 1000;
    INSERT INTO test_db.table_s3 SELECT 1, number, repeat('b', 256) FROM system.numbers LIMIT 1000;
    INSERT INTO test_db.table_s3 SELECT 2, number, repeat('c', 256) FROM system.numbers LIMIT 1000;
    """
    When we create clickhouse01 clickhouse backup
    """
    name: test_backup1
    copy_cloud_storage_data: true
    """
    And we save all user's data in context on clickhouse01
    And we save data part checksums in context on clickhouse01
    And we create clickhouse01 clickhouse backup
    """
    name: test_backup2
    copy_cloud_storage_data: true
    """
    Then we got the following backups on clickhouse01
      | num | state   | data_count | link_count |
      | 0   | created | 0          | 3          |
      | 1   | created | 3          | 0          |
    And s3 bucket ch-backup contains no objects with prefix "ch_backup/test_backup2/cloud_storage/"
    When we delete all objects in s3 bucket cloud-storage-01
    And we restore clickhouse backup #0 to clickhouse02
    Then the user's data equal to saved one on clickhouse02
    And data part checksums equal to saved ones on clickhouse02

  @object_storage_copy
  @require_version_24.1
  Scenario: Deduplicated parts leave no objects behind when the table is dropped
    Given we have executed queries on clickhouse01
    """
    CREATE DATABASE IF NOT EXISTS test_db;
    CREATE TABLE test_db.table_s3 (
        CounterID UInt32,
        UserID    UInt32,
        Payload   String
    )
    ENGINE = MergeTree()
    PARTITION BY CounterID
    ORDER BY UserID
    SETTINGS storage_policy = 's3';

    INSERT INTO test_db.table_s3 SELECT 0, number, repeat('a', 256) FROM system.numbers LIMIT 1000;
    INSERT INTO test_db.table_s3 SELECT 1, number, repeat('b', 256) FROM system.numbers LIMIT 1000;
    """
    When we create clickhouse01 clickhouse backup
    """
    name: test_backup1
    copy_cloud_storage_data: true
    """
    And we create clickhouse01 clickhouse backup
    """
    name: test_backup2
    copy_cloud_storage_data: true
    """
    # Without this check the scenario stays green when nothing was deduplicated.
    Then we got the following backups on clickhouse01
      | num | state   | data_count | link_count |
      | 0   | created | 0          | 2          |
      | 1   | created | 2          | 0          |
    # Frozen data of a live backup holds the objects, so both backups go first.
    # Deleting one releases only the references it still has: the reference of a
    # deduplicated part was released at backup time and has to have been released
    # through ClickHouse, or the objects are left in the bucket after DROP.
    When we delete clickhouse01 clickhouse backup "test_backup2"
    And we delete clickhouse01 clickhouse backup "test_backup1"
    And we execute queries on clickhouse01
    """
    DROP TABLE test_db.table_s3 SYNC;
    """
    Then s3 bucket cloud-storage-01 contains 0 objects

  @object_storage_copy
  @require_version_24.1
  Scenario: A backup that did not copy the data is not a source of links
    Given we have executed queries on clickhouse01
    """
    CREATE DATABASE IF NOT EXISTS test_db;
    CREATE TABLE test_db.table_s3 (
        CounterID UInt32,
        UserID    UInt32,
        Payload   String
    )
    ENGINE = MergeTree()
    PARTITION BY CounterID
    ORDER BY UserID
    SETTINGS storage_policy = 's3';

    INSERT INTO test_db.table_s3 SELECT 0, number, repeat('a', 256) FROM system.numbers LIMIT 1000;
    INSERT INTO test_db.table_s3 SELECT 1, number, repeat('b', 256) FROM system.numbers LIMIT 1000;
    INSERT INTO test_db.table_s3 SELECT 2, number, repeat('c', 256) FROM system.numbers LIMIT 1000;
    """
    When we create clickhouse01 clickhouse backup
    """
    name: test_backup1
    """
    And we save all user's data in context on clickhouse01
    And we save data part checksums in context on clickhouse01
    And we create clickhouse01 clickhouse backup
    """
    name: test_backup2
    copy_cloud_storage_data: true
    """
    # The first backup left the data in the source bucket. Linking to it would
    # make the second backup unrestorable as soon as that bucket is gone, which
    # is exactly what the rest of the scenario does.
    Then we got the following backups on clickhouse01
      | num | state   | data_count | link_count |
      | 0   | created | 3          | 0          |
      | 1   | created | 3          | 0          |
    And s3 bucket ch-backup contains objects with prefix "ch_backup/test_backup2/cloud_storage/s3/"
    When we delete all objects in s3 bucket cloud-storage-01
    And we restore clickhouse backup #0 to clickhouse02
    Then the user's data equal to saved one on clickhouse02
    And data part checksums equal to saved ones on clickhouse02

  @object_storage_copy
  @require_version_24.1
  Scenario: Restore of the third backup in a chain of deduplicated parts
    Given we have executed queries on clickhouse01
    """
    CREATE DATABASE IF NOT EXISTS test_db;
    CREATE TABLE test_db.table_s3 (
        CounterID UInt32,
        UserID    UInt32,
        Payload   String
    )
    ENGINE = MergeTree()
    PARTITION BY CounterID
    ORDER BY UserID
    SETTINGS storage_policy = 's3';

    INSERT INTO test_db.table_s3 SELECT 0, number, repeat('a', 256) FROM system.numbers LIMIT 1000;
    INSERT INTO test_db.table_s3 SELECT 1, number, repeat('b', 256) FROM system.numbers LIMIT 1000;
    INSERT INTO test_db.table_s3 SELECT 2, number, repeat('c', 256) FROM system.numbers LIMIT 1000;
    """
    When we create clickhouse01 clickhouse backup
    """
    name: test_backup1
    copy_cloud_storage_data: true
    """
    And we execute queries on clickhouse01
    """
    ALTER TABLE test_db.table_s3
    UPDATE Payload = repeat('z', 256) WHERE CounterID = 0
    SETTINGS mutations_sync = 2;
    """
    And we create clickhouse01 clickhouse backup
    """
    name: test_backup2
    copy_cloud_storage_data: true
    """
    And we execute queries on clickhouse01
    """
    ALTER TABLE test_db.table_s3
    UPDATE Payload = repeat('y', 256) WHERE CounterID = 1
    SETTINGS mutations_sync = 2;
    """
    And we save all user's data in context on clickhouse01
    And we save data part checksums in context on clickhouse01
    And we create clickhouse01 clickhouse backup
    """
    name: test_backup3
    copy_cloud_storage_data: true
    """
    # Every mutation renames the parts of untouched partitions, so the part of
    # partition 2 is two renames away from its data in the first backup. The
    # third backup links to two backups at once: partition 0 to the second one,
    # partition 2 to the first one.
    Then we got the following backups on clickhouse01
      | num | state   | data_count | link_count |
      | 0   | created | 1          | 2          |
      | 1   | created | 1          | 2          |
      | 2   | created | 3          | 0          |
    And s3 bucket ch-backup contains objects with prefix "ch_backup/test_backup1/cloud_storage/s3/"
    And s3 bucket ch-backup contains objects with prefix "ch_backup/test_backup2/cloud_storage/s3/"
    When we delete all objects in s3 bucket cloud-storage-01
    And we restore clickhouse backup "test_backup3" to clickhouse02
    Then the user's data equal to saved one on clickhouse02
    And data part checksums equal to saved ones on clickhouse02

  @object_storage_copy
  @require_version_24.1
  Scenario: Purge keeps cloud storage data linked by a retained backup
    # The time policy is checked after the count one and keeps every backup
    # made today, so it has to be zeroed for the count to delete anything.
    Given ch-backup configuration on clickhouse01
    """
    backup:
        retain_time:
            weeks: 0
            days: 0
        retain_count: 1
    """
    And we have executed queries on clickhouse01
    """
    CREATE DATABASE IF NOT EXISTS test_db;
    CREATE TABLE test_db.table_s3 (
        CounterID UInt32,
        UserID    UInt32,
        Payload   String
    )
    ENGINE = MergeTree()
    PARTITION BY CounterID
    ORDER BY UserID
    SETTINGS storage_policy = 's3';

    INSERT INTO test_db.table_s3 SELECT 0, number, repeat('a', 256) FROM system.numbers LIMIT 1000;
    INSERT INTO test_db.table_s3 SELECT 1, number, repeat('b', 256) FROM system.numbers LIMIT 1000;
    INSERT INTO test_db.table_s3 SELECT 2, number, repeat('c', 256) FROM system.numbers LIMIT 1000;
    """
    When we create clickhouse01 clickhouse backup
    """
    name: test_backup1
    copy_cloud_storage_data: true
    """
    And we create clickhouse01 clickhouse backup
    """
    name: test_backup2
    copy_cloud_storage_data: true
    """
    And we save all user's data in context on clickhouse01
    And we save data part checksums in context on clickhouse01
    And we create clickhouse01 clickhouse backup
    """
    name: test_backup3
    copy_cloud_storage_data: true
    """
    And we purge clickhouse01 clickhouse backups
    # The retained backup holds nothing but links, so the data of the purged
    # first backup has to outlive it.
    Then we got the following backups on clickhouse01
      | num | state             | data_count | link_count |
      | 0   | created           | 0          | 3          |
      | 1   | partially_deleted | 3          | 0          |
    And s3 bucket ch-backup contains objects with prefix "ch_backup/test_backup1/cloud_storage/s3/"
    When we delete all objects in s3 bucket cloud-storage-01
    And we restore clickhouse backup "test_backup3" to clickhouse02
    Then the user's data equal to saved one on clickhouse02
    And data part checksums equal to saved ones on clickhouse02
    When we delete clickhouse01 clickhouse backup "test_backup3"
    """
    purge_partial: true
    """
    Then we got no backups on clickhouse01
    And s3 bucket ch-backup contains no objects with prefix "ch_backup/test_backup1/"

  @object_storage_copy
  @require_version_24.1
  Scenario: Data of several tables is copied in parallel
    Given ch-backup configuration on clickhouse01
    """
    multiprocessing:
        cloud_storage_backup_workers: 4
    """
    And we have executed queries on clickhouse01
    """
    CREATE DATABASE IF NOT EXISTS test_db;

    CREATE TABLE test_db.table_01 (CounterID UInt32, UserID UInt32, Payload String)
    ENGINE = MergeTree() ORDER BY UserID SETTINGS storage_policy = 's3';
    CREATE TABLE test_db.table_02 (CounterID UInt32, UserID UInt32, Payload String)
    ENGINE = MergeTree() ORDER BY UserID SETTINGS storage_policy = 's3';
    CREATE TABLE test_db.table_03 (CounterID UInt32, UserID UInt32, Payload String)
    ENGINE = MergeTree() ORDER BY UserID SETTINGS storage_policy = 's3';
    CREATE TABLE test_db.table_04 (CounterID UInt32, UserID UInt32, Payload String)
    ENGINE = MergeTree() ORDER BY UserID SETTINGS storage_policy = 's3';
    CREATE TABLE test_db.table_05 (CounterID UInt32, UserID UInt32, Payload String)
    ENGINE = MergeTree() ORDER BY UserID SETTINGS storage_policy = 's3';
    CREATE TABLE test_db.table_06 (CounterID UInt32, UserID UInt32, Payload String)
    ENGINE = MergeTree() ORDER BY UserID SETTINGS storage_policy = 's3';

    INSERT INTO test_db.table_01 SELECT 1, number, repeat('a', 256) FROM system.numbers LIMIT 500;
    INSERT INTO test_db.table_02 SELECT 2, number, repeat('b', 256) FROM system.numbers LIMIT 500;
    INSERT INTO test_db.table_03 SELECT 3, number, repeat('c', 256) FROM system.numbers LIMIT 500;
    INSERT INTO test_db.table_04 SELECT 4, number, repeat('d', 256) FROM system.numbers LIMIT 500;
    INSERT INTO test_db.table_05 SELECT 5, number, repeat('e', 256) FROM system.numbers LIMIT 500;
    INSERT INTO test_db.table_06 SELECT 6, number, repeat('f', 256) FROM system.numbers LIMIT 500;
    """
    # Copies of several tables share one temporary disk, and this is the only
    # scenario where more than one of them is created at a time.
    When we save all user's data in context on clickhouse01
    And we save data part checksums in context on clickhouse01
    And we create clickhouse01 clickhouse backup
    """
    name: test_backup
    copy_cloud_storage_data: true
    """
    Then we got the following backups on clickhouse01
      | num | state   | data_count | link_count |
      | 0   | created | 6          | 0          |
    And s3 bucket ch-backup contains objects with prefix "ch_backup/test_backup/cloud_storage/s3/"
    When we delete all objects in s3 bucket cloud-storage-01
    And we restore clickhouse backup #0 to clickhouse02
    Then the user's data equal to saved one on clickhouse02
    And data part checksums equal to saved ones on clickhouse02
