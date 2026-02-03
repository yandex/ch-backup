Feature: Non-UTF-8 schema encoding support

  Background:
    Given default configuration
    And a working s3
    And a working zookeeper on zookeeper01
    And a working clickhouse on clickhouse01
    And a working clickhouse on clickhouse02

  Scenario: Backup and restore multiple tables with correct utf-8 encodings
    Given we have executed queries on clickhouse01
    """
    CREATE DATABASE test_db;
    
    CREATE TABLE test_db.table_ascii (
        id Int32,
        name_ascii String COMMENT 'ascii'
    ) ENGINE = MergeTree() ORDER BY id;
    
    CREATE TABLE test_db.table_emoji (
        id Int32,
        `name_😈` String COMMENT '😈'
    ) ENGINE = MergeTree() ORDER BY id;
    
    CREATE TABLE test_db.table_cyrillic (
        id Int32,
        `name_абвгд` String COMMENT 'абвгд'
    ) ENGINE = MergeTree() ORDER BY id;
    
    CREATE TABLE test_db.table_chinese (
        id Int32,
        `name_试` String COMMENT '试'
    ) ENGINE = MergeTree() ORDER BY id;
    
    INSERT INTO test_db.table_ascii VALUES (1, 'test1');
    INSERT INTO test_db.table_emoji VALUES (2, 'test2');
    INSERT INTO test_db.table_cyrillic VALUES (3, 'test3');
    INSERT INTO test_db.table_chinese VALUES (4, 'test4');
    """
    When we create clickhouse01 clickhouse backup
    Then we got the following backups on clickhouse01
      | num | state   | data_count | link_count |
      | 0   | created | 4          | 0          |
    When we restore clickhouse backup #0 to clickhouse02
    Then clickhouse02 has same schema as clickhouse01
    And we got same clickhouse data at clickhouse01 clickhouse02

  Scenario: Table with invalid utf-8 characters
    Given we have created non-UTF-8 test table on clickhouse01
    When we create clickhouse01 clickhouse backup
    Then we got the following backups on clickhouse01
      | num | state   | data_count | link_count |
      | 0   | created | 1          | 0          |
    When we restore clickhouse backup #0 to clickhouse02
    When we execute query on clickhouse02
    """
    EXISTS TABLE test_db.table_rus
    """
    Then we get response
    """
    1
    """

