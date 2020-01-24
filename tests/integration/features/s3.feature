Feature: Tests specific to S3 storage engine

  Background:
    Given default configuration
    And a working s3
    And a working zookeeper on zookeeper01
    And a working clickhouse on clickhouse01
    And a working clickhouse on clickhouse02

  Scenario: Create backup with chunk_size that equals to size of an uploading file
    Given we have executed queries on clickhouse01
    """
    CREATE DATABASE test_db;
    CREATE TABLE test_db.table_01 (date Date, n UInt64) ENGINE = MergeTree() PARTITION BY toYYYYMM(date) ORDER BY n;
    INSERT INTO test_db.table_01 SELECT '2010-01-01', number FROM system.numbers LIMIT 1000000;
    """
    And ch-backup configuration on clickhouse01
    """
    storage:
        chunk_size: {{ get_file_size('clickhouse01', '/var/lib/clickhouse/data/test_db/table_01/201001_1_1_0/n.bin') }}
    """
    When we create clickhouse01 clickhouse backup
    Then we got the following backups on clickhouse01
      | num | state    | data_count | link_count   |
      | 0   | created  | 1          | 0            |
    When we restore clickhouse backup #0 to clickhouse02
    Then we got same clickhouse data at clickhouse01 clickhouse02
