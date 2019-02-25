Feature: Tests specific to S3 storage engine

  Background:
    Given default configuration
    And a working s3
    And a working clickhouse on clickhouse01

  Scenario: Create backup with chunk_size that equals to size of an uploading file
    Given test data on clickhouse01 that was created as follows
    """
    CREATE DATABASE test_db;
    CREATE TABLE test_db.table_01 (date Date, n UInt64) ENGINE = MergeTree() PARTITION BY toYYYYMM(date) ORDER BY n;
    INSERT INTO test_db.table_01 SELECT '2010-01-01', number FROM system.numbers LIMIT 1000000;
    """
    And ch-backup config on clickhouse01 was merged with following
    """
    storage:
        chunk_size: {{ get_file_size('clickhouse01', '/var/lib/clickhouse/data/test_db/table_01/201001_1_1_0/n.bin') }}
    """
    When we create clickhouse01 clickhouse backup
    Then ch_backup entries of clickhouse01 are in proper condition
      | num | state    | data_count | link_count   |
      | 0   | created  | 1          | 0            |

  Scenario: Restore from backup
    Given a working clickhouse on clickhouse02
    When we restore clickhouse #0 backup to clickhouse02
    Then we got same clickhouse data at clickhouse01 clickhouse02
