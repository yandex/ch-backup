Feature: Backup of single database table

  Background:
    Given default configuration
    And a working s3
    And a working clickhouse on clickhouse01
    And a working clickhouse on clickhouse02
    And clickhouse on clickhouse01 has test schema

  Scenario: Create backup
    Given clickhouse01 has test clickhouse data test1
    When we create clickhouse01 clickhouse backup
    """
    tables:
      - test_db_01.test_table_01
    """
    Then we got the following backups on clickhouse01
      | num | state    | data_count | link_count   |
      | 0   | created  | 1          | 0            |
    When we restore clickhouse backup #0 to clickhouse02
    Then clickhouse02 has the subset of clickhouse01 data
    """
    tables:
      - test_db_01.test_table_01
    """
