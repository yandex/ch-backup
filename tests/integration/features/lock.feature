Feature: Lock

  Background:
    Given default configuration
    And a working s3
    And a working zookeeper on zookeeper01
    And a working clickhouse on clickhouse01
    And a working clickhouse on clickhouse02

  @require_version_22.3
  Scenario: No backup created while zk lock
    When ch-backup configuration on clickhouse01
    """
    backup:
        skip_lock_for_schema_only:
            backup: false
    lock:
        zk_flock: true
        zk_flock_path: /ch_backup/zk_flock_path
    """
    And on zookeeper01 we create /ch_backup/zk_flock_path
    When we create clickhouse01 clickhouse backup
    Then we got no backups on clickhouse01

  @require_version_22.3
  Scenario: No "schema-only" backup created while zk lock
    When ch-backup configuration on clickhouse01
    """
    backup:
        skip_lock_for_schema_only:
            backup: false
    lock:
        zk_flock: true
        zk_flock_path: /ch_backup/zk_flock_path
    """
    And on zookeeper01 we create /ch_backup/zk_flock_path
    When we create clickhouse01 clickhouse backup
    """
    schema_only: true
    """
    Then we got no backups on clickhouse01

  @require_version_22.3
  Scenario: Skip zk lock while creating "schema-only" backup
    When ch-backup configuration on clickhouse01
    """
    backup:
        skip_lock_for_schema_only:
            backup: true
    lock:
        zk_flock: true
        zk_flock_path: /ch_backup/zk_flock_path
    """
    And on zookeeper01 we create /ch_backup/zk_flock_path
    When we create clickhouse01 clickhouse backup
    """
    schema_only: true
    """
    Then we got the following backups on clickhouse01
      | num | state   | schema_only |
      | 0   | created | True        |
