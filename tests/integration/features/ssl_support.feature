Feature: SSL support

  Background:
    Given configuration
    """
    ch_backup:
      protocol: https
    """
    And a working s3
    And a working clickhouse on clickhouse01
    And a working clickhouse on clickhouse02
    And clickhouse on clickhouse01 has test schema

  Scenario: Create backup
    Given clickhouse01 has test clickhouse data test1
    When we create clickhouse01 clickhouse backup
    Then we got the following backups on clickhouse01
      | num | state    | data_count | link_count   |
      | 0   | created  | 4          | 0            |
    When we restore clickhouse backup #0 to clickhouse02
    Then we got same clickhouse data at clickhouse01 clickhouse02
