Feature: Backups with unreadable metadata

  Background:
    Given default configuration
    And a working s3
    And a working zookeeper on zookeeper01
    And a working clickhouse on clickhouse01
    And a working clickhouse on clickhouse02
    And we have executed queries on clickhouse01
    """
    CREATE DATABASE test_db;
    CREATE TABLE test_db.test_table (partition_id Int32, n Int32)
    ENGINE = MergeTree() PARTITION BY partition_id ORDER BY (partition_id, n);
    INSERT INTO test_db.test_table SELECT number % 2, number FROM system.numbers LIMIT 100;
    """
    And we have created clickhouse01 clickhouse backup
    """
    name: old_backup
    """
    And we have executed queries on clickhouse01
    """
    INSERT INTO test_db.test_table SELECT 2, number FROM system.numbers LIMIT 100;
    """

  Scenario: Unreadable light metadata of a referenced backup blocks only delete and purge
    Given we have created clickhouse01 clickhouse backup
    """
    name: new_backup
    """
    And file "backup_light_struct.json" in clickhouse01 backup #1 data set to
    """
    {not a json
    """
    Then we got the following backups on clickhouse01
      | num | name       | state   | link_count |
      | 0   | new_backup | created | 2          |
    When we execute command on clickhouse01
    """
    ch-backup -c /etc/yandex/ch-backup/ch-backup.conf list -a -v --format json
    """
    Then we get response contains
    """
    "state": "unknown"
    """
    When we restore clickhouse backup "new_backup" to clickhouse02
    """
    access: true
    data: true
    udf: true
    nc: true
    keep_going: true
    """
    Then we got same clickhouse data at clickhouse01 clickhouse02
    When we try to execute command on clickhouse01
    """
    ch-backup -c /etc/yandex/ch-backup/ch-backup.conf restore LAST
    """
    Then command exits with code 2
    And we get response contains
    """
    Cannot resolve LAST, metadata is unreadable for: old_backup
    """
    When we try to execute command on clickhouse01
    """
    ch-backup -c /etc/yandex/ch-backup/ch-backup.conf delete new_backup
    """
    Then command exits with code 1
    And we get response contains
    """
    Failed to read metadata of backups: old_backup
    """
    When we try to execute command on clickhouse01
    """
    ch-backup -c /etc/yandex/ch-backup/ch-backup.conf --config-parameter backup.retain_count 0 purge
    """
    Then command exits with code 1
    When we try to execute command on clickhouse01
    """
    ch-backup -c /etc/yandex/ch-backup/ch-backup.conf delete old_backup
    """
    Then command exits with code 1
    And we get response contains
    """
    Backup old_backup has unreadable metadata and is referenced by other backups
    """
    And s3 bucket ch-backup contains objects with prefix "ch_backup/old_backup/"
    And s3 bucket ch-backup contains objects with prefix "ch_backup/new_backup/"
    When we create clickhouse01 clickhouse backup
    """
    name: next_backup
    """
    Then we got the following backups on clickhouse01
      | num | name        | state   |
      | 0   | next_backup | created |
      | 1   | new_backup  | created |

  Scenario: Unreferenced backup with unreadable light metadata is deleted
    Given ch-backup configuration on clickhouse01
    """
    backup:
        deduplicate_parts: False
    """
    And we have created clickhouse01 clickhouse backup
    """
    name: new_backup
    """
    And file "backup_light_struct.json" in clickhouse01 backup #1 is empty
    When we execute command on clickhouse01
    """
    ch-backup -c /etc/yandex/ch-backup/ch-backup.conf delete old_backup
    """
    Then we get response contains
    """
    SYSTEM UNFREEZE WITH NAME 'old_backup'
    """
    And s3 bucket ch-backup contains no objects with prefix "ch_backup/old_backup/"
    And we got the following backups on clickhouse01
      | num | name       | state   |
      | 0   | new_backup | created |

  Scenario: Unreferenced backup with unreadable full metadata is deleted
    Given ch-backup configuration on clickhouse01
    """
    backup:
        deduplicate_parts: False
    """
    And we have created clickhouse01 clickhouse backup
    """
    name: new_backup
    """
    And file "backup_struct.json" in clickhouse01 backup #1 data set to
    """
    {not a json
    """
    When we delete clickhouse01 clickhouse backup "old_backup"
    Then s3 bucket ch-backup contains no objects with prefix "ch_backup/old_backup/"
    And we got the following backups on clickhouse01
      | num | name       | state   |
      | 0   | new_backup | created |

  Scenario: Referenced backup with unreadable full metadata is kept
    Given we have created clickhouse01 clickhouse backup
    """
    name: new_backup
    """
    And file "backup_struct.json" in clickhouse01 backup #1 data set to
    """
    {not a json
    """
    When we try to execute command on clickhouse01
    """
    ch-backup -c /etc/yandex/ch-backup/ch-backup.conf delete old_backup
    """
    Then command exits with code 1
    And we get response contains
    """
    Failed to decrypt legacy backup metadata
    """
    And s3 bucket ch-backup contains objects with prefix "ch_backup/old_backup/"
    When we restore clickhouse backup "new_backup" to clickhouse02
    Then we got same clickhouse data at clickhouse01 clickhouse02
