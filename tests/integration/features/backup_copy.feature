Feature: Copy backups to another storage

  Background:
    Given default configuration
    And a working s3
    And a working zookeeper on zookeeper01
    And a working clickhouse on clickhouse01
    And a working clickhouse on clickhouse02

  Scenario Outline: Copy backup to another bucket
    Given clickhouse on clickhouse01 has test schema
    And clickhouse01 has test clickhouse data test1
    And we have created clickhouse01 clickhouse backup
    And ch-backup configuration on clickhouse02
    """
    storage:
        credentials:
            bucket: ch-backup-copy
    """
    When we execute command on clickhouse01
    """
    ch-backup -c /etc/yandex/ch-backup/ch-backup.conf copy LAST <options> \
        --target-config-parameter storage.credentials.bucket '"ch-backup-copy"'
    """
    Then we get response contains
    """
    : copied
    """
    And we got the following backups on clickhouse02
      | num | state   | link_count |
      | 0   | created | 0          |
    When we restore clickhouse backup #0 to clickhouse02
    Then we got same clickhouse data at clickhouse01 clickhouse02
    When we execute command on clickhouse01
    """
    ch-backup -c /etc/yandex/ch-backup/ch-backup.conf copy LAST <options> \
        --target-config-parameter storage.credentials.bucket '"ch-backup-copy"'
    """
    Then we get response contains
    """
    skipped, already present in target storage
    """

    Examples:
      | options               |
      | --server-side-copy    |
      | --no-server-side-copy |

  Scenario: Copy backup to another path in the same bucket
    Given clickhouse on clickhouse01 has test schema
    And clickhouse01 has test clickhouse data test1
    And we have created clickhouse01 clickhouse backup
    And ch-backup configuration on clickhouse02
    """
    backup:
        path_root: ch_backup_copy/
    """
    When we execute command on clickhouse01
    """
    ch-backup -c /etc/yandex/ch-backup/ch-backup.conf copy LAST \
        --target-config-parameter backup.path_root '"ch_backup_copy/"'
    """
    Then we got the following backups on clickhouse02
      | num | state   |
      | 0   | created |
    When we restore clickhouse backup #0 to clickhouse02
    Then we got same clickhouse data at clickhouse01 clickhouse02

  Scenario: Copy only the latest deduplicated backup
    Given we have executed queries on clickhouse01
    """
    CREATE DATABASE test_db1;

    CREATE TABLE test_db1.test_table1 (partition_id Int32, n Int32)
    ENGINE = MergeTree() PARTITION BY partition_id ORDER BY (partition_id, n);

    INSERT INTO test_db1.test_table1 SELECT number % 2, number FROM system.numbers LIMIT 100;
    """
    And we have created clickhouse01 clickhouse backup
    And we have executed queries on clickhouse01
    """
    CREATE TABLE test_db1.test_table2 (partition_id Int32, n Int32)
    ENGINE = MergeTree() PARTITION BY partition_id ORDER BY (partition_id, n);

    INSERT INTO test_db1.test_table2 SELECT number % 3, number FROM system.numbers LIMIT 100;
    """
    And ch-backup configuration on clickhouse02
    """
    storage:
        credentials:
            bucket: ch-backup-copy
    """
    When we create clickhouse01 clickhouse backup
    Then we got the following backups on clickhouse01
      | num | state   | data_count | link_count |
      | 0   | created | 3          | 2          |
      | 1   | created | 2          | 0          |
    When we execute command on clickhouse01
    """
    ch-backup -c /etc/yandex/ch-backup/ch-backup.conf copy LAST \
        --target-config-parameter storage.credentials.bucket '"ch-backup-copy"'
    """
    Then we got the following backups on clickhouse02
      | num | state   | data_count | link_count |
      | 0   | created | 5          | 0          |
    When we restore clickhouse backup #0 to clickhouse02
    Then we got same clickhouse data at clickhouse01 clickhouse02

  Scenario: Copy all deduplicated backups keeps deduplication
    Given we have executed queries on clickhouse01
    """
    CREATE DATABASE test_db1;

    CREATE TABLE test_db1.test_table1 (partition_id Int32, n Int32)
    ENGINE = MergeTree() PARTITION BY partition_id ORDER BY (partition_id, n);

    INSERT INTO test_db1.test_table1 SELECT number % 2, number FROM system.numbers LIMIT 100;
    """
    And we have created clickhouse01 clickhouse backup
    And we have executed queries on clickhouse01
    """
    CREATE TABLE test_db1.test_table2 (partition_id Int32, n Int32)
    ENGINE = MergeTree() PARTITION BY partition_id ORDER BY (partition_id, n);

    INSERT INTO test_db1.test_table2 SELECT number % 3, number FROM system.numbers LIMIT 100;
    """
    And ch-backup configuration on clickhouse02
    """
    storage:
        credentials:
            bucket: ch-backup-copy
    """
    When we create clickhouse01 clickhouse backup
    And we execute command on clickhouse01
    """
    ch-backup -c /etc/yandex/ch-backup/ch-backup.conf copy --all \
        --target-config-parameter storage.credentials.bucket '"ch-backup-copy"'
    """
    Then we got the following backups on clickhouse02
      | num | state   | data_count | link_count |
      | 0   | created | 3          | 2          |
      | 1   | created | 2          | 0          |
    When we restore clickhouse backup #0 to clickhouse02
    Then we got same clickhouse data at clickhouse01 clickhouse02

  Scenario: Copy fails if target storage contains another backup with the same name
    Given clickhouse on clickhouse01 has test schema
    And clickhouse01 has test clickhouse data test1
    And we have created clickhouse01 clickhouse backup
    """
    name: test_backup
    """
    And ch-backup configuration on clickhouse02
    """
    storage:
        credentials:
            bucket: ch-backup-copy
    """
    And we have created clickhouse02 clickhouse backup
    """
    name: test_backup
    """
    When we try to execute command on clickhouse01
    """
    ch-backup -c /etc/yandex/ch-backup/ch-backup.conf copy test_backup \
        --target-config-parameter storage.credentials.bucket '"ch-backup-copy"'
    """
    Then we get response contains
    """
    already exists in target storage and it is not a copy of the source backup
    """

  @require_version_22.8
  Scenario: Copied backup with data on S3 disk doesn't depend on the source
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
    And we have created clickhouse01 clickhouse backup
    """
    name: 12345678T123456
    """
    And ch-backup configuration on clickhouse02
    """
    storage:
        credentials:
            bucket: ch-backup-copy
    """
    When we execute command on clickhouse01
    """
    ch-backup -c /etc/yandex/ch-backup/ch-backup.conf copy 12345678T123456 \
        --target-config-parameter storage.credentials.bucket '"ch-backup-copy"' \
        --target-cloud-storage-bucket cloud-storage-02 \
        --target-cloud-storage-path copy
    """
    Then we get response contains
    """
    12345678T123456: copied
    """
    When we execute query on clickhouse01
    """
    DROP TABLE test_db.table_01
    """
    And we delete clickhouse01 clickhouse backup #0
    Then s3 bucket cloud-storage-01 contains 0 objects
    When we restore clickhouse backup #0 to clickhouse02
    And we execute query on clickhouse02
    """
    SELECT count() FROM test_db.table_01
    """
    Then we get response
    """
    20
    """
    When we execute query on clickhouse02
    """
    SELECT sum(UserID) FROM test_db.table_01
    """
    Then we get response
    """
    90
    """

  @require_version_22.8
  Scenario: Copy backup with data on S3 disk and default backup name
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
    And we have created clickhouse01 clickhouse backup
    And ch-backup configuration on clickhouse02
    """
    storage:
        credentials:
            bucket: ch-backup-copy
    """
    When we execute command on clickhouse01
    """
    ch-backup -c /etc/yandex/ch-backup/ch-backup.conf copy LAST \
        --target-config-parameter storage.credentials.bucket '"ch-backup-copy"' \
        --target-cloud-storage-bucket cloud-storage-02 \
        --target-cloud-storage-path copy
    """
    And we restore clickhouse backup #0 to clickhouse02
    Then we got same clickhouse data at clickhouse01 clickhouse02

  @require_version_22.8
  Scenario: Deleting copy of backup with data on S3 disk doesn't affect the source backup
    Given ch-backup configuration on clickhouse02
    """
    storage:
        credentials:
            bucket: ch-backup
    """
    And we have executed queries on clickhouse01
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
    And we have created clickhouse01 clickhouse backup
    """
    name: 12345678T123456
    """
    When we execute command on clickhouse01
    """
    ch-backup -c /etc/yandex/ch-backup/ch-backup.conf copy 12345678T123456 \
        --target-config-parameter storage.credentials.bucket '"ch-backup-copy"' \
        --target-cloud-storage-bucket cloud-storage-02 \
        --target-cloud-storage-path copy
    """
    And we execute command on clickhouse01
    """
    ch-backup -c /etc/yandex/ch-backup/ch-backup.conf \
        --config-parameter storage.credentials.bucket '"ch-backup-copy"' \
        delete 12345678T123456
    """
    Then s3 bucket ch-backup-copy contains no objects with prefix "ch_backup/12345678T123456/"
    And we got the following backups on clickhouse02
      | num | name            | state   |
      | 0   | 12345678T123456  | created |
    When we execute query on clickhouse01
    """
    DROP TABLE test_db.table_01
    """
    # Restore the original: deleting the copy must not unfreeze the source's data.
    And we restore clickhouse backup "12345678T123456" to clickhouse02
    """
    cloud_storage_source_bucket: 'cloud-storage-01'
    cloud_storage_source_path: 'data'
    """
    And we execute query on clickhouse02
    """
    SELECT count() FROM test_db.table_01
    """
    Then we get response
    """
    20
    """

  Scenario Outline: Copy respects the <held_lock> storage lock
    Given clickhouse on clickhouse01 has test schema
    And we have created clickhouse01 clickhouse backup
    """
    name: lock-copy
    schema_only: true
    """
    And ch-backup configuration on clickhouse01
    """
    lock:
        zk_flock: true
        zk_flock_path: /ch_backup/copy-source
        lock_timeout: 1
        exitcode: 71
    """
    And ch-backup configuration on clickhouse02
    """
    storage:
        credentials:
            bucket: ch-backup-copy
    lock:
        zk_flock: true
        zk_flock_path: /ch_backup/copy-target
    """
    When we acquire zookeeper lock on zookeeper01 with path /ch_backup/copy-<held_lock>
    And we try to execute command on clickhouse01
    """
    ch-backup -c /etc/yandex/ch-backup/ch-backup.conf copy lock-copy \
        --target-config-parameter storage.credentials.bucket '"ch-backup-copy"' \
        --target-config-parameter lock.zk_flock_path '"/ch_backup/copy-target"'
    """
    Then command exits with code 71
    And s3 bucket ch-backup-copy contains 0 objects
    When we release zookeeper lock on zookeeper01 with path /ch_backup/copy-<held_lock>
    And we execute command on clickhouse01
    """
    ch-backup -c /etc/yandex/ch-backup/ch-backup.conf copy lock-copy \
        --target-config-parameter storage.credentials.bucket '"ch-backup-copy"' \
        --target-config-parameter lock.zk_flock_path '"/ch_backup/copy-target"'
    """
    Then we got the following backups on clickhouse02
      | num | name      | state   |
      | 0   | lock-copy | created |
    When we delete clickhouse02 clickhouse backup "lock-copy"
    Then s3 bucket ch-backup-copy contains 0 objects

    Examples:
      | held_lock |
      | source    |
      | target    |

  @require_version_24.1
  Scenario Outline: Copy full cloud storage backups with <selection>
    Given we have executed queries on clickhouse01
    """
    CREATE DATABASE test_db;
    CREATE TABLE test_db.table_s3 (partition_id UInt32, n UInt32)
    ENGINE = MergeTree() PARTITION BY partition_id ORDER BY n
    SETTINGS storage_policy = 's3';
    INSERT INTO test_db.table_s3 SELECT 0, number FROM numbers(10);
    """
    And we have created clickhouse01 clickhouse backup
    """
    name: full-base
    copy_cloud_storage_data: true
    """
    And we have executed queries on clickhouse01
    """
    INSERT INTO test_db.table_s3 SELECT 1, number FROM numbers(10);
    """
    And we have created clickhouse01 clickhouse backup
    """
    name: full-next
    copy_cloud_storage_data: true
    """
    And ch-backup configuration on clickhouse02
    """
    storage:
        credentials:
            bucket: ch-backup-copy
    backup:
        path_root: copied/
    """
    Then we got the following backups on clickhouse01
      | num | state   | data_count | link_count |
      | 0   | created | 1          | 1          |
      | 1   | created | 1          | 0          |
    When we execute command on clickhouse01
    """
    ch-backup -c /etc/yandex/ch-backup/ch-backup.conf copy <selection> \
        --target-config-parameter storage.credentials.bucket '"ch-backup-copy"' \
        --target-config-parameter backup.path_root '"copied/"'
    """
    And we delete all objects in s3 bucket ch-backup
    And we delete all objects in s3 bucket cloud-storage-01
    And we restore clickhouse backup #0 to clickhouse02
    And we execute query on clickhouse02
    """
    SELECT count(), sum(n) FROM test_db.table_s3
    """
    Then we get response
    """
    20	90
    """
    When we delete clickhouse02 clickhouse backup #0
    Then s3 bucket ch-backup-copy contains no objects with prefix "copied/full_next/"
    And s3 bucket ch-backup-copy contains no objects with prefix "copied/full-next/"

    Examples:
      | selection |
      | LAST      |
      | --all     |
