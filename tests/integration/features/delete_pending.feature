@object_storage_copy
@require_version_24.1
Feature: Retry deletion of backups without readable manifests

  Background:
    Given default configuration
    And a working s3
    And a working zookeeper on zookeeper01
    And a working clickhouse on clickhouse01
    And a working clickhouse on clickhouse02
    And ch-backup configuration on clickhouse01
    """
    backup:
        deduplicate_parts: true
    lock:
        zk_flock: true
        zk_flock_path: /delete-pending-test
    """

  Scenario Outline: Keep linked data until its last dependent backup disappears
    Given we have executed queries on clickhouse01
    """
    CREATE DATABASE test_db;
    CREATE TABLE test_db.pending_data (id UInt32)
    ENGINE = MergeTree() PARTITION BY id ORDER BY id
    SETTINGS storage_policy = '<policy>';
    INSERT INTO test_db.pending_data VALUES (1), (2);
    """
    And we have created clickhouse01 clickhouse backup
    """
    name: owner
    copy_cloud_storage_data: true
    """
    When we execute queries on clickhouse01
    """
    ALTER TABLE test_db.pending_data DROP PARTITION 2;
    """
    And we create clickhouse01 clickhouse backup
    """
    name: dependent
    copy_cloud_storage_data: true
    """
    Then all parts in clickhouse01 backup "dependent" link to "owner"
    When both manifests of clickhouse01 backup "owner" are removed
    And we execute command on clickhouse01
    """
    ch-backup -c /etc/yandex/ch-backup/ch-backup.conf delete owner --delete-unreadable
    """
    Then deletion marker of clickhouse01 backup "owner" references "dependent"
    When we restore clickhouse backup #0 to clickhouse02
    Then we got same clickhouse data at clickhouse01 clickhouse02
    When we execute command on clickhouse01
    """
    ch-backup -c /etc/yandex/ch-backup/ch-backup.conf copy dependent \
        --target-config-parameter storage.credentials.bucket '"ch-backup-copy"'
    """
    Then we get response contains
    """
    dependent: copied
    """
    When we execute command on clickhouse01
    """
    ch-backup -c /etc/yandex/ch-backup/ch-backup.conf delete dependent
    ch-backup -c /etc/yandex/ch-backup/ch-backup.conf purge
    """
    Then s3 bucket ch-backup contains no objects with prefix "ch_backup/owner/"
    And s3 bucket ch-backup contains no objects with prefix "ch_backup/dependent/"

    Examples:
      | policy  |
      | default |
      | s3      |
