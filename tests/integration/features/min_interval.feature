@dependent-scenarios
Feature: Min interval between backups

  Background:
    Given default configuration
    And a working s3
    And a working clickhouse on clickhouse01
    And ch-backup config on clickhouse01 was merged with following
    """
    backup:
        min_interval:
            hours: 1
    """

  Scenario: Create backup
    Given test data on clickhouse01 that was created as follows
    """
    CREATE DATABASE test_db;
    CREATE TABLE test_db.table_01 (n Int32) ENGINE = MergeTree() PARTITION BY n % 10 ORDER BY n;
    INSERT INTO test_db.table_01 SELECT number FROM system.numbers LIMIT 1000;
    """
    When we create clickhouse01 clickhouse backup
    Then we got the following backups on clickhouse01
      | num | state    | data_count | link_count |
      | 0   | created  | 10         | 0          |

  Scenario Outline: Attempt to create backup results to no new backups
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
      | 1   | created  | 0          | 10         |
      | 2   | created  | 10         | 0          |
