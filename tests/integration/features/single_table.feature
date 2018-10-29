Feature: Backup of single database table

  Background:
    Given default configuration
    And a working s3
    And a working clickhouse on clickhouse01
    And clickhouse on clickhouse01 has test schema

  Scenario: Create backup
    Given clickhouse01 has test clickhouse data test1
    When we create clickhouse01 clickhouse backup
    """
    tables:
      - test_db_01.test_table_01
    """
    Then ch_backup entries of clickhouse01 are in proper condition
      | num | state    | data_count | link_count   | title         |
      | 0   | created  | 1          | 0            | data          |

  Scenario: Restore from backup
    When we restore clickhouse #0 backup to clickhouse02
    Then clickhouse02 has the subset of clickhouse01 data
    """
    tables:
      - test_db_01.test_table_01
    """
