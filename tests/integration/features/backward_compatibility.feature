Feature: Backward compatibility support for old backups

  Background:
    Given default configuration
    And a working s3
    And a working zookeeper on zookeeper01
    And a working clickhouse on clickhouse01
    And a working clickhouse on clickhouse02
    And ch-backup configuration on clickhouse01
    """
    encryption:
      enabled: False
    """
    And ch-backup configuration on clickhouse02
    """
    encryption:
      enabled: False
    """
    

  Scenario: Restore with old metadata layout
    When we execute queries on clickhouse01
    """
    CREATE DATABASE test_db_01 UUID '82aa76a0-45cd-42f2-b355-852cc8c9c0af' ENGINE = Atomic;
    CREATE TABLE test_db_01.table_01 UUID '10000000-0000-0000-0000-000000000001' (id UInt32, val String) ENGINE = MergeTree ORDER BY id;
    CREATE TABLE test_db_01.table_02 UUID '10000000-0000-0000-0000-000000000002' (id UInt32, abc String, def UInt64) ENGINE = MergeTree ORDER BY id;
    """
    When we create clickhouse01 clickhouse backup
    """
    name: test_backup
    """
    Then s3 bucket ch-backup contains 4 objects
    """
        bucket: ch-backup
    """
    When we delete object in S3
    """
        bucket: ch-backup
        path: /ch_backup/test_backup/metadata/databases.tar
    """
    And we delete object in S3
    """
        bucket: ch-backup
        path: /ch_backup/test_backup/metadata/test_db_01.tar
    """
    Then s3 bucket ch-backup contains 2 objects
    """
        bucket: ch-backup
    """
    When we put object in S3
    """
        bucket: ch-backup
        path: /ch_backup/test_backup/metadata/test_db_01.sql
        data: "ATTACH DATABASE _ UUID '82aa76a0-45cd-42f2-b355-852cc8c9c0af'\nENGINE = Atomic\n"
    """
    And we put object in S3
    """
        bucket: ch-backup
        path: /ch_backup/test_backup/metadata/test_db_01/table_01.sql
        data: "ATTACH TABLE _ UUID '10000000-0000-0000-0000-000000000001'\n(\n    `id` UInt32,\n    `val` String\n)\nENGINE = MergeTree\nORDER BY id\nSETTINGS index_granularity = 8192\n"
    """
    And we put object in S3
    """
        bucket: ch-backup
        path: /ch_backup/test_backup/metadata/test_db_01/table_02.sql
        data: "ATTACH TABLE _ UUID '10000000-0000-0000-0000-000000000002'\n(\n    `id` UInt32,\n    `abc` String,\n    `def` UInt64\n)\nENGINE = MergeTree\nORDER BY id\nSETTINGS index_granularity = 8192\n"
    """
    When we restore clickhouse backup #0 to clickhouse02
    Then clickhouse02 has same schema as clickhouse01
