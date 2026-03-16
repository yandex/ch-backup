Feature: Database engines

  Background:
    Given default configuration
    And a working s3
    And a working zookeeper on zookeeper01
    And a working clickhouse on clickhouse01
    And a working clickhouse on clickhouse02
    And clickhouse on clickhouse01 has test schema


  @require_version_22.7
  Scenario: Restore-schema with incorrect default database engine
    Given ClickHouse settings
    """
    allow_deprecated_database_ordinary: 1
    """
    When we execute queries on clickhouse01
    """
    DROP DATABASE IF EXISTS default;
    CREATE DATABASE default ENGINE=Ordinary
    """
    When we create clickhouse01 clickhouse backup
    """
    schema_only: true
    """
    When we restore clickhouse backup #0 to clickhouse02
    """
    schema_only: true
    """
    Then clickhouse02 has same schema as clickhouse01

  @require_version_22.7
  Scenario: Restore-schema with incorrect database engine(atomic to ordinary)
    Given ClickHouse settings
    """
    allow_deprecated_database_ordinary: 1
    """
    When we execute queries on clickhouse01
    """
    CREATE DATABASE test_db ENGINE=Ordinary
    """
    And we create clickhouse01 clickhouse backup
    """
    schema_only: true
    """
    When we execute queries on clickhouse02
    """
    CREATE DATABASE test_db ENGINE=Atomic
    """
    And we restore clickhouse backup #0 to clickhouse02
    """
    schema_only: true
    """
    Then clickhouse02 has same schema as clickhouse01

  @require_version_24.8
  Scenario: Restore tables from Ordinary database should not conflict with detached tables having zero UUID
    Given ClickHouse settings
    """
    allow_deprecated_database_ordinary: 1
    """
    When we execute queries on clickhouse01
    """
    CREATE DATABASE test_ordinary ENGINE=Ordinary;
    CREATE TABLE test_ordinary.table1 (id UInt32, value String) ENGINE=MergeTree() ORDER BY id;
    INSERT INTO test_ordinary.table1 VALUES (1, 'test1');
    """
    When we create clickhouse01 clickhouse backup
    When we execute queries on clickhouse02
    """
    CREATE DATABASE test_ordinary ENGINE=Ordinary;
    CREATE TABLE test_ordinary.table2 (id UInt32, value String) ENGINE=MergeTree() ORDER BY id;
    INSERT INTO test_ordinary.table2 VALUES (999, 'should_stay_detached');
    DETACH TABLE test_ordinary.table2;
    """
    When we restore clickhouse backup #0 to clickhouse02
    When we execute query on clickhouse02
    """
    SELECT count() FROM system.detached_tables WHERE database = 'test_ordinary' AND table = 'table2'
    """
    Then we get response
    """
    1
    """
    When we execute query on clickhouse02
    """
    SELECT value FROM test_ordinary.table1
    """
    Then we get response
    """
    test1
    """
