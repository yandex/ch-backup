Feature: Backup dictionary created by ddl

  Background:
    Given default configuration
    And a working s3
    And a working clickhouse on clickhouse01
    And a working clickhouse on clickhouse02

  @require_version_19.17
  Scenario: Backup dictionary created by ddl
    Given we have executed queries on clickhouse01
    """
    CREATE DATABASE test_db;
    CREATE TABLE test_db.table_01 (n1 UInt32, n2 UInt32) ENGINE = MergeTree() PARTITION BY tuple() ORDER BY n1;
    INSERT INTO test_db.table_01 SELECT number, rand() FROM system.numbers LIMIT 3;
    CREATE DICTIONARY test_db.test_dictionary (n1 UInt32, n2 UInt32)
    PRIMARY KEY n1 LAYOUT(hashed()) LIFETIME(MIN 1 MAX 10)
    SOURCE(CLICKHOUSE(HOST 'localhost' PORT 9000 TABLE 'test_db.table_01' USER 'default'))
    """
    When we create clickhouse01 clickhouse backup
    Then we got the following backups on clickhouse01
      | num | state    | data_count | link_count   |
      | 0   | created  | 1          | 0            |
    When we restore clickhouse backup #0 to clickhouse02
    When we execute queries on clickhouse02
    """
    SYSTEM RELOAD DICTIONARY 'test_db.test_dictionary';
    """
    Then we got same clickhouse data at clickhouse01 clickhouse02
