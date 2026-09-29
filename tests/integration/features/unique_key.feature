Feature: Backup of tables with UNIQUE KEY

  Background:
    Given default configuration
    And a working s3
    And a working zookeeper on zookeeper01
    And a working clickhouse on clickhouse01
    And a working clickhouse on clickhouse02
    And clickhouse on clickhouse01 supports UNIQUE KEY
    And ClickHouse settings
    """
    allow_experimental_unique_key: 1
    """
    And we have executed queries on clickhouse01
    """
    CREATE DATABASE test_db;

    CREATE TABLE test_db.table_plain (id UInt64)
    ENGINE = MergeTree ORDER BY id;

    INSERT INTO test_db.table_plain SELECT number FROM numbers(100);

    CREATE TABLE test_db.table_unique_key (id UInt64)
    ENGINE = MergeTree ORDER BY id UNIQUE KEY id;

    INSERT INTO test_db.table_unique_key SELECT number FROM numbers(100);
    """

  @require_version_26.5
  Scenario: Create backup of a table with UNIQUE KEY
    When we create clickhouse01 clickhouse backup
    Then we got the following backups on clickhouse01
      | num | state    | data_count | link_count |
      | 0   | created  | 1          | 0          |
    And metadata of clickhouse01 backup #0 for "test_db"."table_unique_key" contains
    """
    data_skipped_reason: unique_key
    parts: {}
    """

  @require_version_26.5
  Scenario: Restore backup of a table with UNIQUE KEY
    Given we have created clickhouse01 clickhouse backup
    When we restore clickhouse backup #0 to clickhouse02
    Then clickhouse02 has same schema as clickhouse01
    When we execute query on clickhouse02
    """
    SELECT count() FROM test_db.table_unique_key
    """
    Then we get response
    """
    0
    """
    When we execute query on clickhouse02
    """
    SELECT count() FROM test_db.table_plain
    """
    Then we get response
    """
    100
    """

  @require_version_26.5
  Scenario: Restore a table with UNIQUE KEY into a replicated database
    Given ClickHouse settings
    """
    allow_experimental_unique_key: 1
    allow_experimental_database_replicated: 1
    """
    And we have executed queries on clickhouse01
    """
    CREATE DATABASE test_replicated_db
    ENGINE = Replicated('/clickhouse/databases/test_replicated_db', '{shard}', '{replica}');

    CREATE TABLE test_replicated_db.table_unique_key (id UInt64)
    ENGINE = MergeTree ORDER BY id UNIQUE KEY id;

    INSERT INTO test_replicated_db.table_unique_key SELECT number FROM numbers(100);
    """
    And we have created clickhouse01 clickhouse backup
    When we restore clickhouse backup #0 to clickhouse02
    """
    restore_tables_in_replicated_database: true
    """
    Then clickhouse02 has same schema as clickhouse01
    When we execute query on clickhouse02
    """
    SELECT count() FROM test_replicated_db.table_unique_key
    """
    Then we get response
    """
    0
    """
