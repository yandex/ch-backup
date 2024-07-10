Feature: Parallel freeze

  Background:
    Given default configuration
    And a working s3
    And a working zookeeper on zookeeper01
    And a working clickhouse on clickhouse01
    And a working clickhouse on clickhouse02
    And clickhouse on clickhouse01 has test schema with 5 databases and 10 tables
    And clickhouse01 has test clickhouse data test1 with 5 databases, 10 tables, 100 rows and 5 partitions

  Scenario: Create backup with single freeze worker
    Given ch-backup configuration on clickhouse01
    """
    multiprocessing:
        freeze_threads: 1
    """
    When we create clickhouse01 clickhouse backup
    Then we got the following backups on clickhouse01
      | num | state    | data_count | link_count   | title         |
      | 0   | created  | 250        | 0            | shared        |
    When we restore clickhouse backup #0 to clickhouse02
    Then we got same clickhouse data at clickhouse01 clickhouse02

  Scenario: Create backup with default number of freeze workers
    When we create clickhouse01 clickhouse backup
    Then we got the following backups on clickhouse01
      | num | state    | data_count | link_count   | title         |
      | 0   | created  | 250        | 0            | shared        |
    When we restore clickhouse backup #0 to clickhouse02
    Then we got same clickhouse data at clickhouse01 clickhouse02
