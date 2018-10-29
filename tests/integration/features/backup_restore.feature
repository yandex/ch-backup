Feature: Backup & Restore

  Background:
    Given default configuration
    And a working s3
    And a working clickhouse on clickhouse01
    And clickhouse on clickhouse01 has test schema

  Scenario: Create backup
    Given clickhouse01 has test clickhouse data test1
    When we create clickhouse01 clickhouse backup
    Then ch_backup entries of clickhouse01 are in proper condition
      | num | state    | data_count | link_count   | title         |
      | 0   | created  | 4          | 0            | data          |

  Scenario: Create backup with increments
    Given clickhouse01 has test clickhouse data test2
    When we create clickhouse01 clickhouse backup
    Then ch_backup entries of clickhouse01 are in proper condition
      | num | state    | data_count | link_count   | title         |
      | 0   | created  | 4          | 4            | data+links    |
      | 1   | created  | 4          | 0            | shared        |

  Scenario: Restore from backup
    When we restore clickhouse #0 backup to clickhouse02
    Then we got same clickhouse data at clickhouse01 clickhouse02

  @schema-only
  Scenario: Restore schema from backup without data
    When we drop all databases at clickhouse02
    And we restore clickhouse 1 backup schema to clickhouse02
    Then clickhouse02 has same schema as clickhouse01
    But on clickhouse02 tables are empty
# TODO: check deduplication with overdue backups