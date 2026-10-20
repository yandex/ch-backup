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
    And s3 contains 11 objects
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
    And s3 contains 11 objects
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
    And s3 contains 11 objects
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
    @require_version_less_than_26.8
    Examples:
    | object_count |
    | 11           |

    @require_version_26.8
    Examples:
    | object_count |
    | 12           |

  Scenario: Restore succeeds when target node has no storage_configuration section
    # Regression test for: ClickHouseTemporaryDisks.__enter__ raised KeyError when
    # storage_configuration was absent from the ClickHouse config.
    # The backup here uses only the default disk (no cloud_storage disks),
    # so __enter__ must complete without error even on a node with no
    # storage_configuration section.
    Given a working clickhouse on clickhouse02
    When we execute queries on clickhouse01
    """
    CREATE DATABASE IF NOT EXISTS test_db;
    CREATE TABLE test_db.table_no_s3 (
        CounterID UInt32,
        UserID    UInt32
    )
    ENGINE = MergeTree()
    ORDER BY UserID;
    INSERT INTO test_db.table_no_s3 VALUES (0, 1), (1, 2);
    """
    And we create clickhouse01 clickhouse backup
    Then we got the following backups on clickhouse01
      | num | state   | data_count | link_count |
      | 0   | created | 1          | 0          |
    When we replace config file storage_configuration.xml in favor of no_storage_configuration.xml on clickhouse02 with restart
    And we restore clickhouse backup #0 to clickhouse02
    Then we got same clickhouse data at clickhouse01 clickhouse02

  @require_version_22.8
  Scenario: Restore with cloud_storage disks fails with a clear error when disk is absent from target config
    # Regression test for: _create_temporary_disk raised an opaque KeyError instead of
    # ClickHouseDisksException when the disk referenced in backup cloud_storage metadata
    # was missing from the target node's storage_configuration.
    # We use storage_configuration_without_s3.xml on clickhouse02: it keeps the s3/s3_cold
    # storage policies (so the table schema can be restored) but removes the actual 's3'
    # disk entry, which triggers ClickHouseDisksException in __enter__.
    Given a working clickhouse on clickhouse02
    When we execute queries on clickhouse01
    """
    CREATE DATABASE IF NOT EXISTS test_db;
    CREATE TABLE test_db.table_s3 (
        CounterID UInt32,
        UserID    UInt32
    )
    ENGINE = MergeTree()
    ORDER BY UserID
    SETTINGS storage_policy = 's3';
    INSERT INTO test_db.table_s3 VALUES (0, 1), (1, 2);
    """
    And we create clickhouse01 clickhouse backup
    Then we got the following backups on clickhouse01
      | num | state   | data_count | link_count |
      | 0   | created | 1          | 0          |
    When we replace config file storage_configuration.xml in favor of storage_configuration_without_s3.xml on clickhouse02 with restart
    And we try to execute command on clickhouse02
    """
    ch-backup -c /etc/yandex/ch-backup/ch-backup.conf restore LAST \
        --cloud-storage-source-bucket cloud-storage-01 \
        --cloud-storage-source-path data
    """
    Then we get response contains
    """
    missing from ClickHouse storage_configuration
    """

  @require_version_24.7
  Scenario: Restore retries a partially copied S3 part into its original directory
    # MDB-51258: copy into an existing detached/<part>/ must not create
    # detached/<part>/<part>/. A missing data object leaves an incomplete
    # destination on the first attempt; retry must restore the complete part.
    Given a working clickhouse on clickhouse02
    And ch-backup configuration on clickhouse02
    """
    backup:
        restore_fail_on_attach_error: true
    multiprocessing:
        cloud_storage_restore_workers: 1
    """
    And we have executed queries on clickhouse01
    """
    CREATE DATABASE IF NOT EXISTS test_db;
    CREATE TABLE test_db.restore_retry (id UInt64, payload String)
    ENGINE = MergeTree ORDER BY id
    SETTINGS storage_policy = 'multiple_s3', min_bytes_for_wide_part = 0;
    INSERT INTO test_db.restore_retry SELECT number, toString(number) FROM numbers(100);
    """
    When we execute query on clickhouse01
    """
    SELECT name, disk_name, part_type FROM system.parts
    WHERE database = 'test_db' AND table = 'restore_retry' AND active;
    """
    Then we get response
    """
    all_1_1_0	s3	Wide
    """
    When we save all user's data in context on clickhouse01
    And we save data part checksums in context on clickhouse01
    And we create clickhouse01 clickhouse backup
    """
    name: restore_retry
    """
    # id.bin is required to attach a Wide part; columns.txt can be regenerated.
    And we save and delete the S3 objects of a data part file on clickhouse01
    """
    database: test_db
    table: restore_retry
    part: all_1_1_0
    file: id.bin
    bucket: cloud-storage-01
    path: data
    """
    And we try to execute command on clickhouse02
    """
    ch-backup -c /etc/yandex/ch-backup/ch-backup.conf restore restore_retry \
        --cloud-storage-source-bucket cloud-storage-01 \
        --cloud-storage-source-path data
    """
    Then command exits with code 1
    And we get response contains
    """
    clickhouse-disks call failed
    """
    When we execute query on clickhouse02
    """
    SELECT count() FROM test_db.restore_retry;
    """
    Then we get response
    """
    0
    """
    # Leave the destination and restore state intact. Only repair the source.
    When we restore saved S3 objects
    And we restore clickhouse backup #0 to clickhouse02
    """
    cloud_storage_source_bucket: cloud-storage-01
    cloud_storage_source_path: data
    """
    Then the user's data equal to saved one on clickhouse02
    And data part checksums equal to saved ones on clickhouse02
