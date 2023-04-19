Feature: Backup dictionary created by ddl

  Background:
    Given default configuration
    And a working s3
    And a working zookeeper on zookeeper01
    And a working clickhouse on clickhouse01
    And a working clickhouse on clickhouse02

  Scenario: Backup dictionary created by ddl
    Given we have executed queries on clickhouse01
    """
    CREATE DATABASE test_db;

    CREATE TABLE test_db.table_01 (n1 UInt32, n2 UInt32)
    ENGINE MergeTree PARTITION BY tuple() ORDER BY n1;

    INSERT INTO test_db.table_01 SELECT number, rand() FROM system.numbers LIMIT 3;

    CREATE DICTIONARY test_db.test_dictionary (n1 UInt32, n2 UInt32)
    PRIMARY KEY n1 LAYOUT(hashed()) LIFETIME(MIN 1 MAX 10)
    SOURCE(CLICKHOUSE(HOST 'localhost' PORT 9000 DB 'test_db' TABLE 'table_01' USER 'default'))
    """
    When we create clickhouse01 clickhouse backup
    Then we got the following backups on clickhouse01
      | num | state   | data_count | link_count |
      | 0   | created | 1          | 0          |
    When we restore clickhouse backup #0 to clickhouse02
    When we execute queries on clickhouse02
    """
    SYSTEM RELOAD DICTIONARY 'test_db.test_dictionary';
    """
    Then we got same clickhouse data at clickhouse01 clickhouse02

  Scenario: Dependent tables and dicts works
    Given we have executed queries on clickhouse01
    """
    CREATE DATABASE test_db;

    CREATE TABLE test_db.table_01 (n1 UInt32, n2 UInt32)
    ENGINE MergeTree PARTITION BY tuple() ORDER BY n1;

    INSERT INTO test_db.table_01 SELECT number, rand() FROM system.numbers LIMIT 3;

    CREATE DICTIONARY test_db.test_dictionary (n1 UInt32, n2 UInt32)
    PRIMARY KEY n1 LAYOUT(hashed()) LIFETIME(MIN 1 MAX 10)
    SOURCE(CLICKHOUSE(HOST 'localhost' PORT 9000 DB 'test_db' TABLE 'table_01' USER 'default'));

    CREATE TABLE test_db.table_02 (
        n1 UInt32,
        n2 UInt32 DEFAULT dictGetUInt32('test_db.test_dictionary', 'n2', CAST(1, 'UInt64')))
    ENGINE MergeTree PARTITION BY tuple() ORDER BY n1;
    """
    When we create clickhouse01 clickhouse backup
    Then we got the following backups on clickhouse01
      | num | state   | data_count | link_count |
      | 0   | created | 1          | 0          |
    When we restore clickhouse backup #0 to clickhouse02
    When we execute queries on clickhouse02
    """
    SYSTEM RELOAD DICTIONARY 'test_db.test_dictionary';
    """
    Then we got same clickhouse data at clickhouse01 clickhouse02

  @require_version_22.7
  Scenario: Dict with nonexistent dependent table works
    Given ClickHouse settings
    """
    allow_deprecated_database_ordinary: 1
    """
    Given we have executed queries on clickhouse01
    """
    CREATE DATABASE test_db Engine=Ordinary;

    CREATE TABLE test_db.table_01 (n1 UInt32, n2 UInt32)
    ENGINE MergeTree PARTITION BY tuple() ORDER BY n1;

    CREATE DICTIONARY test_db.test_dictionary (n1 UInt32, n2 UInt32)
    PRIMARY KEY n1 LAYOUT(hashed()) LIFETIME(MIN 1 MAX 10)
    SOURCE(CLICKHOUSE(HOST 'localhost' PORT 9000 DB 'test_db' TABLE 'table_01' USER 'default'));

    ATTACH TABLE test_db.dict_table (`n1` UInt32, `n2` UInt32) ENGINE = Dictionary('test_db.test_dictionary');
    """
    When we create clickhouse01 clickhouse backup
    Then we got the following backups on clickhouse01
      | num | state   | data_count | link_count |
      | 0   | created | 0          | 0          |
    When we restore clickhouse backup #0 to clickhouse02
    Then clickhouse02 has same schema as clickhouse01
