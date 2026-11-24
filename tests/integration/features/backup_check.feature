Feature: Check of backups

  Background:
    Given default configuration
    And a working s3
    And a working zookeeper on zookeeper01
    And a working clickhouse on clickhouse01
    And we have executed queries on clickhouse01
    """
    CREATE DATABASE test_db;
    CREATE TABLE test_db.test_table (partition_id Int32, n Int32)
    ENGINE = MergeTree() PARTITION BY partition_id ORDER BY (partition_id, n);

    INSERT INTO test_db.test_table SELECT number % 2, number FROM system.numbers LIMIT 100;
    """
    And we have created clickhouse01 clickhouse backup
    And we have executed queries on clickhouse01
    """
    CREATE TABLE test_db.test_table2 (partition_id Int32, n Int32)
    ENGINE = MergeTree() PARTITION BY partition_id ORDER BY (partition_id, n);

    INSERT INTO test_db.test_table2 SELECT number % 3, number FROM system.numbers LIMIT 100;
    """
    And we have created clickhouse01 clickhouse backup

  Scenario Outline: Intact backups pass the check
    When we execute command on clickhouse01
    """
    ch-backup -c /etc/yandex/ch-backup/ch-backup.conf check --all <options> --format json 2>/dev/null | python3 -c 'import json, sys; [print(r["status"], r["unverified"]) for r in json.load(sys.stdin)]'
    """
    Then we get response
    """
    ok 0
    ok 0
    """

    Examples:
      | options |
      |         |
      | --deep  |

  Scenario Outline: Missing part breaks backups linking to it
    Given file "data/test_db/test_table/0_1_1_0/0_1_1_0.tar" was deleted from clickhouse01 backup #1
    When we try to execute command on clickhouse01
    """
    ch-backup -c /etc/yandex/ch-backup/ch-backup.conf check <options> $(ch-backup -c /etc/yandex/ch-backup/ch-backup.conf list | head -1)
    """
    Then command exits with code 2
    And we get response contains
    """
    part 0_1_1_0 of "test_db"."test_table" in backup
    """
    And we get response contains
    """
    data is missing or has wrong size
    """

    Examples:
      | options |
      |         |
      | --deep  |

  Scenario: Corrupted part content is found by deep check only
    Given file "data/test_db/test_table/0_1_1_0/0_1_1_0.tar" in clickhouse01 backup #1 is corrupted keeping its size
    When we execute command on clickhouse01
    """
    ch-backup -c /etc/yandex/ch-backup/ch-backup.conf check --all
    """
    And we try to execute command on clickhouse01
    """
    ch-backup -c /etc/yandex/ch-backup/ch-backup.conf check --all --deep
    """
    Then command exits with code 2
    And we get response contains
    """
    part 0_1_1_0 of "test_db"."test_table" in backup
    """
    And we get response contains
    """
    CryptoError
    """

  Scenario Outline: Unreadable metadata and schemas break the backup
    Given file "<path>" in clickhouse01 backup #0 data set to
    """
    garbage
    """
    When we try to execute command on clickhouse01
    """
    ch-backup -c /etc/yandex/ch-backup/ch-backup.conf check --all
    """
    Then command exits with code 2
    And we get response contains
    """
    <problem>
    """

    Examples:
      | path                 | problem                     |
      | backup_struct.json   | metadata:                   |
      | metadata/test_db.tar | table schemas of "test_db": |

  Scenario: Content of not encrypted parts is reported as unverified
    Given ch-backup configuration on clickhouse01
    """
    encryption:
      enabled: False
    """
    And we have executed queries on clickhouse01
    """
    CREATE TABLE test_db.test_table3 (partition_id Int32, n Int32)
    ENGINE = MergeTree() PARTITION BY partition_id ORDER BY (partition_id, n);

    INSERT INTO test_db.test_table3 SELECT number % 2, number FROM system.numbers LIMIT 100;
    """
    And we have created clickhouse01 clickhouse backup
    When we execute command on clickhouse01
    """
    ch-backup -c /etc/yandex/ch-backup/ch-backup.conf check --deep --format json $(ch-backup -c /etc/yandex/ch-backup/ch-backup.conf list | head -1)
    """
    Then we get response contains
    """
    "unverified": 2
    """

  @object_storage_copy
  @require_version_24.1
  Scenario: Missing copied data of cloud storage breaks the backup with '-' in its name
    Given we have executed queries on clickhouse01
    """
    CREATE TABLE test_db.table_s3 (
        CounterID UInt32,
        UserID    UInt32
    )
    ENGINE = MergeTree()
    ORDER BY UserID
    SETTINGS storage_policy = 's3';

    INSERT INTO test_db.table_s3 SELECT 0, number FROM system.numbers LIMIT 100;
    """
    When we create clickhouse01 clickhouse backup
    """
    name: test-backup
    copy_cloud_storage_data: true
    """
    And we execute command on clickhouse01
    """
    ch-backup -c /etc/yandex/ch-backup/ch-backup.conf check --all --deep
    """
    And we delete objects with prefix "ch_backup/test_backup/cloud_storage/s3/" in s3 bucket ch-backup
    And we try to execute command on clickhouse01
    """
    ch-backup -c /etc/yandex/ch-backup/ch-backup.conf check test-backup
    """
    Then command exits with code 2
    And we get response contains
    """
    objects of copied data are missing or have wrong size
    """
