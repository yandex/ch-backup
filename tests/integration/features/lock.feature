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
        zk_flock: false
        flock: true
        flock_path: /tmp/flock.lock
    """
    When we create filesystem lock on clickhouse01 with path /tmp/flock.lock
    And we trying create clickhouse01 clickhouse backup
    """
    schema_only: true
    """
    Then we got the following backups on clickhouse01
      | num | state   | schema_only |
      | 0   | created | True        |
    And we delete filesystem lock on clickhouse01 with path /tmp/flock.lock
