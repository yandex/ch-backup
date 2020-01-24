Feature: Min interval between backups

  Background:
    Given default configuration
    And a working s3
    And a working zookeeper on zookeeper01
    And a working clickhouse on clickhouse01
    And ch-backup configuration on clickhouse01
    """
    backup:
        min_interval:
            hours: 1
    """
    And we have executed queries on clickhouse01
    """
    CREATE DATABASE test_db;
    CREATE TABLE test_db.table_01 (n Int32) ENGINE = MergeTree() PARTITION BY n % 10 ORDER BY n;
    INSERT INTO test_db.table_01 SELECT number FROM system.numbers LIMIT 1000;
    """
    And we have created clickhouse01 clickhouse backup

  Scenario Outline: Attempt to create backup when min interval is not passed
    Given metadata of clickhouse01 backup #0 was adjusted with
    """
    meta:
        state: <state>
    """
    When we create clickhouse01 clickhouse backup
    Then we got the following backups on clickhouse01
      | num | state    | data_count | link_count |
      | 0   | <state>  | 10         | 0          |

    Examples:
      | state             |
      | created           |
      | creating          |
      | deleting          |
      | partially_deleted |
      | failed            |

  Scenario: Create backup when min interval is passed
    Given metadata of clickhouse01 backup #0 was adjusted with
    """
    meta:
        state: created
        start_time: {{ backup.start_time | decrease_on('1 hour') }}
        end_time: {{ backup.end_time | decrease_on('1 hour') }}
    """
    When we create clickhouse01 clickhouse backup
    Then we got the following backups on clickhouse01
      | num | state    | data_count | link_count |
      | 0   | created  | 0          | 10         |
      | 1   | created  | 10         | 0          |

  Scenario: Create backup bypassing min interval with force option
    When we create clickhouse01 clickhouse backup
    """
    force: True
    """
    Then we got the following backups on clickhouse01
      | num | state    | data_count | link_count |
      | 0   | created  | 0          | 10         |
      | 1   | created  | 10         | 0          |
