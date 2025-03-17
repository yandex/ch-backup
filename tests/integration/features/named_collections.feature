Feature: Backup and restore functionality of named collections

  Background:
    Given default configuration
    And a working s3
    And a working zookeeper on zookeeper01
    And a working clickhouse on clickhouse01
    And a working clickhouse on clickhouse02
    And we have executed queries on clickhouse01
    """
    CREATE NAMED COLLECTION test_s3_nc AS
      access_key_id = 'AKIAIOSFODNN7EXAMPLE',
      secret_access_key = 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
      format = 'CSV' NOT OVERRIDABLE,
      url = 'https://s3.us-east-1.amazonaws.com/yourbucket/mydata/' OVERRIDABLE;
    CREATE DATABASE test_db;
    CREATE TABLE test_db.table_01 (
      key UInt32
    )
    ENGINE=S3(test_s3_nc);
    """

  @require_version_24.3
  Scenario: from local to local storage
    When we create clickhouse01 clickhouse backup
    Then we got the following backups on clickhouse01
      | num | state   | data_count | link_count |
      | 0   | created | 0          | 0          |
    When we restore clickhouse backup #0 to clickhouse02
    Given a working clickhouse on clickhouse02
    Then clickhouse01 has same named collections as clickhouse02
    And clickhouse01 has same schema as clickhouse02
