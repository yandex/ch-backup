Feature: Lock

  Background:
    Given default configuration
    And a working s3
    And a working zookeeper on zookeeper01
    And a working clickhouse on clickhouse01

  Scenario: Skip lock while creating "schema-only" backup
    Given ch-backup configuration on clickhouse01
    """
    backup:
        skip_lock_for_schema_only:
            backup: true
    lock:
      flock: false
      zk_flock: true
      zk_flock_path: /ch_backup/zk_flock_path
    """
    When we acquire zookeeper lock on zookeeper01 with path /ch_backup/zk_flock_path
    And we trying create clickhouse01 clickhouse backup
    """
    schema_only: true
    """
    And we release zookeeper lock on zookeeper01 with path /ch_backup/zk_flock_path

    Then we got the following backups on clickhouse01
      | num | state   | schema_only |
      | 0   | created | True        |
