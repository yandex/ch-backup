Feature: Backup of single database table

  Background: Insert initial data into clickhouse
    Given default configuration
    And a working clickhouse on clickhouse01
    And clickhouse on clickhouse01 has test schema
    And a working s3 on minio01
    And minio01 s3 has bucket dbaas

  Scenario: Backup done successfully
    Given clickhouse01 has test clickhouse data test1
    When we create clickhouse01 clickhouse backup
    """
    tables:
      - test_db_01.test_table_01
    """
    Then we got 1 ch_backup entries of clickhouse01

  Scenario: Backup restored successfully
    When we restore clickhouse #0 backup to clickhouse02
    Then clickhouse02 has the subset of clickhouse01 data
    """
    tables:
      - test_db_01.test_table_01
    """
