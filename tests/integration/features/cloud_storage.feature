Feature: Backup & Clean & Restore

  Background:
    Given default configuration
    And a working s3
    And a working zookeeper on zookeeper01
    And a working clickhouse on clickhouse01
    And clickhouse on clickhouse01 has test schema

  Scenario Outline: Download backup metadata
    When we execute queries on clickhouse01
    """
    CREATE DATABASE IF NOT EXISTS test_db;
    CREATE TABLE test_db.table_01 (
        CounterID UInt32,
        UserID UInt32
    )
    ENGINE = MergeTree()
    ORDER BY UserID
    SETTINGS storage_policy = 's3';
    INSERT INTO test_db.table_01 VALUES(0, 42), (1, 777);
    """
    And we create clickhouse01 clickhouse backup
    """
    name: test_backup
    """
    And we execute command on clickhouse01
    """
    ls /var/lib/clickhouse/disks/s3/shadow/
    """
    Then we get response
    """
    test_backup
    """
    When we execute command on clickhouse01
    """
    rm -r /var/lib/clickhouse/disks/s3/shadow/test_backup
    """
    And we execute command on clickhouse01
    """
    ch-backup -c /etc/yandex/ch-backup/ch-backup.conf get-cloud-storage-metadata --disk s3 <name>
    """
    And we execute command on clickhouse01
    """
    ls /var/lib/clickhouse/disks/s3/shadow/
    """
    Then we get response
    """
    test_backup
    """
    When we execute command on clickhouse01
    """
    ch-backup -c /etc/yandex/ch-backup/ch-backup.conf get-cloud-storage-metadata --disk s3 <name>
    """
    Then we get response contains
    """
    is already present
    """
    Examples:
      | name              |
      | test_backup       |
      | LAST              |

  Scenario: Download backup metadata to local path
    When we execute queries on clickhouse01
    """
    CREATE DATABASE IF NOT EXISTS test_db;
    CREATE TABLE test_db.table_01 (
        CounterID UInt32,
        UserID UInt32
    )
    ENGINE = MergeTree()
    ORDER BY UserID
    SETTINGS storage_policy = 's3';
    INSERT INTO test_db.table_01 VALUES(0, 42), (1, 777);
    """
    And we create clickhouse01 clickhouse backup
    """
    name: test_backup
    """
    And we execute command on clickhouse01
    """
    ls /var/lib/clickhouse/disks/s3/shadow/
    """
    Then we get response
    """
    test_backup
    """
    When we execute command on clickhouse01
    """
    rm -r /var/lib/clickhouse/disks/s3/shadow/test_backup
    """
    And we execute command on clickhouse01
    """
    ch-backup -c /etc/yandex/ch-backup/ch-backup.conf get-cloud-storage-metadata --local-path /tmp/cloud-storage-metadata-pipe --disk s3 LAST
    """
    And we execute command on clickhouse01
    """
    ls /tmp/cloud-storage-metadata-pipe
    """
    Then we get response
    """
    /tmp/cloud-storage-metadata-pipe
    """

  @require_version_22.6
  @require_version_less_than_23.4
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
    And s3 contains 15 objects
    And s3 bucket cloud-storage-01 contains 8 objects
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
    And s3 bucket cloud-storage-01 contains 0 objects
    And we got the following s3 backup directories on clickhouse01
    """
    []
    """

  @require_version_23.4
  @require_version_less_than_23.7
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
    And s3 contains 15 objects
    And s3 bucket cloud-storage-01 contains 9 objects
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
    And s3 bucket cloud-storage-01 contains 0 objects
    And we got the following s3 backup directories on clickhouse01
    """
    []
    """

  Scenario Outline: All backup data is deleted including data of removed tables
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
    And s3 contains 15 objects
    And s3 bucket cloud-storage-01 contains <object_count> objects
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
    And s3 bucket cloud-storage-01 contains 0 objects
    And we got the following s3 backup directories on clickhouse01
    """
    []
    """
    @require_version_23.7
    @require_version_less_than_25.8
    Examples:
    | object_count |
    | 10           |

    @require_version_25.8
    Examples:
    | object_count |
    | 11           |

Scenario: Double restore.
    Given we have executed queries on clickhouse01
    """
    CREATE DATABASE test_db;
    CREATE TABLE test_db.test (n Int32) ENGINE = MergeTree() PARTITION BY n%100 ORDER BY n;
    INSERT INTO test_db.test SELECT 1
    """
    When we create clickhouse01 clickhouse backup
    And we restore clickhouse backup #0 to clickhouse01
    And we restore clickhouse backup #0 to clickhouse02
    Then we got same clickhouse data at clickhouse01 clickhouse02
