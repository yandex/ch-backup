Feature: Backup of tables with different engines and configurations

  Background:
    Given default configuration
    And a working s3
    And a working zookeeper on zookeeper01
    And a working clickhouse on clickhouse01
    And a working clickhouse on clickhouse02
    And we have executed queries on clickhouse01
    """
    CREATE DATABASE test_db;
    """

  Scenario: Create backup containing merge tree table with old style configuration
    Given we have executed queries on clickhouse01
    """
    CREATE TABLE test_db.table_01 (date Date, n Int32)
    ENGINE = MergeTree(date, date, 8192);
    INSERT INTO test_db.table_01 SELECT today(), number FROM system.numbers LIMIT 1000;
    """
    When we create clickhouse01 clickhouse backup
    Then we got the following backups on clickhouse01
      | num | state    | data_count | link_count   |
      | 0   | created  | 1          | 0            |
    When we restore clickhouse backup #0 to clickhouse02
    Then clickhouse02 has same schema as clickhouse01
    And we got same clickhouse data at clickhouse01 clickhouse02

  Scenario: Create backup containing merge tree tables with new style configuration
    Given we have executed queries on clickhouse01
    """
    CREATE TABLE test_db.table_01 (date Date, n Int32)
    ENGINE = MergeTree() PARTITION BY date ORDER BY date;
    INSERT INTO test_db.table_01 SELECT today(), number FROM system.numbers LIMIT 1000;

    CREATE TABLE test_db.table_02 (n Int32)
    ENGINE = MergeTree() PARTITION BY n % 10 ORDER BY n;
    INSERT INTO test_db.table_02 SELECT number FROM system.numbers LIMIT 1000;
    """
    When we create clickhouse01 clickhouse backup
    Then we got the following backups on clickhouse01
      | num | state    | data_count | link_count   |
      | 0   | created  | 11         | 0            |
    When we restore clickhouse backup #0 to clickhouse02
    Then clickhouse02 has same schema as clickhouse01
    And we got same clickhouse data at clickhouse01 clickhouse02

  Scenario: Create backup containing merge tree table with implicit structure
    Given we have executed queries on clickhouse01
    """
    CREATE TABLE test_db.table_01
    ENGINE = MergeTree() PARTITION BY n % 10 ORDER BY n
    AS SELECT number "n", toString(number) "s" FROM system.numbers LIMIT 1000;
    """
    When we create clickhouse01 clickhouse backup
    Then we got the following backups on clickhouse01
      | num | state    | data_count | link_count   |
      | 0   | created  | 10         | 0            |
    When we restore clickhouse backup #0 to clickhouse02
    Then clickhouse02 has same schema as clickhouse01
    And we got same clickhouse data at clickhouse01 clickhouse02

  Scenario: Create backup containing tables with log table engine family
    Given we have executed queries on clickhouse01
    """
    CREATE TABLE test_db.table_01 (n Int32, s String) ENGINE = TinyLog;
    INSERT INTO test_db.table_01 SELECT number, toString(number) FROM system.numbers LIMIT 1000;

    CREATE TABLE test_db.table_02 (n Int32, s String) ENGINE = Log;
    INSERT INTO test_db.table_02 SELECT number, toString(number) FROM system.numbers LIMIT 1000;

    CREATE TABLE test_db.table_03 (n Int32, s String) ENGINE = StripeLog;
    INSERT INTO test_db.table_03 SELECT number, toString(number) FROM system.numbers LIMIT 1000;
    """
    When we create clickhouse01 clickhouse backup
    Then we got the following backups on clickhouse01
      | num | state    | data_count | link_count   |
      | 0   | created  | 0          | 0            |
    When we restore clickhouse backup #0 to clickhouse02
    Then clickhouse02 has same schema as clickhouse01
    But on clickhouse02 tables are empty

  Scenario: Create backup containing distributed table
    Given we have executed queries on clickhouse01
    """
    CREATE TABLE test_db.table_01 (n Int32, s String)
    ENGINE = MergeTree() PARTITION BY n % 10 ORDER BY n;
    INSERT INTO test_db.table_01 SELECT number, toString(number) FROM system.numbers LIMIT 1000;

    CREATE TABLE test_db.table_02 AS test_db.table_01
    ENGINE = Distributed('test_shard_localhost', 'test_db', 'table_01', n);
    """
    When we create clickhouse01 clickhouse backup
    Then we got the following backups on clickhouse01
      | num | state    | data_count | link_count   |
      | 0   | created  | 10         | 0            |
    When we restore clickhouse backup #0 to clickhouse02
    Then clickhouse02 has same schema as clickhouse01
    And we got same clickhouse data at clickhouse01 clickhouse02

  Scenario: Create backup containing views
    Given we have executed queries on clickhouse01
    """
    CREATE DATABASE test_db2;

    CREATE TABLE test_db.table_01 (n Int32, s String)
    ENGINE = MergeTree() PARTITION BY n % 10 ORDER BY n;
    INSERT INTO test_db.table_01 SELECT number, toString(number) FROM system.numbers LIMIT 1000;

    CREATE TABLE test_db2.table_02 (n Int32, n2 Int32)
    ENGINE = MergeTree() PARTITION BY n % 10 ORDER BY n;
    INSERT INTO test_db2.table_02 SELECT number, number * number FROM system.numbers LIMIT 1000;

    CREATE VIEW test_db.view_01
    AS SELECT n, n * n AS "n2"
    FROM test_db.table_01;

    CREATE VIEW test_db2.view_02
    AS SELECT n, n2, s
    FROM (
        SELECT n, s
        FROM test_db.table_01
    ) subquery1
    ALL LEFT JOIN (
        SELECT n, n2
        FROM test_db2.table_02
    ) subquery2
    USING n;
    """
    When we create clickhouse01 clickhouse backup
    Then we got the following backups on clickhouse01
      | num | state    | data_count | link_count   |
      | 0   | created  | 20         | 0            |
    When we restore clickhouse backup #0 to clickhouse02
    Then clickhouse02 has same schema as clickhouse01
    And we got same clickhouse data at clickhouse01 clickhouse02

  Scenario: Create backup containing materialized view with implicit backend table
    Given we have executed queries on clickhouse01
    """
    CREATE TABLE test_db.table_01 (n Int32, s String)
    ENGINE = MergeTree() PARTITION BY n % 10 ORDER BY n;

    CREATE MATERIALIZED VIEW test_db.mview_01
    ENGINE = MergeTree() PARTITION BY n % 10 ORDER BY n
    AS SELECT n, n * n AS "n2"
    FROM test_db.table_01;

    INSERT INTO test_db.table_01 SELECT number, toString(number) FROM system.numbers LIMIT 1000;
    """
    When we create clickhouse01 clickhouse backup
    Then we got the following backups on clickhouse01
      | num | state    | data_count | link_count   |
      | 0   | created  | 20         | 0            |
    When we restore clickhouse backup #0 to clickhouse02
    Then clickhouse02 has same schema as clickhouse01
    And we got same clickhouse data at clickhouse01 clickhouse02

  Scenario: Create backup containing materialized view with explicit backend table
    Given we have executed queries on clickhouse01
    """
    CREATE TABLE test_db.table_01 (n Int32, s String)
    ENGINE = MergeTree() PARTITION BY n % 10 ORDER BY n;

    CREATE TABLE test_db.mview_backend_01 (n Int32, n2 Int64)
    ENGINE = MergeTree() PARTITION BY n % 10 ORDER BY n;

    CREATE MATERIALIZED VIEW test_db.mview_01 TO test_db.mview_backend_01
    AS SELECT n, n * n AS "n2"
    FROM test_db.table_01;

    INSERT INTO test_db.table_01 SELECT number, toString(number) FROM system.numbers LIMIT 1000;
    """
    When we create clickhouse01 clickhouse backup
    Then we got the following backups on clickhouse01
      | num | state    | data_count | link_count   |
      | 0   | created  | 20         | 0            |
    When we restore clickhouse backup #0 to clickhouse02
    Then clickhouse02 has same schema as clickhouse01
    And we got same clickhouse data at clickhouse01 clickhouse02

  Scenario: Create backup containing materialized view with implicit backend table and broken view dependencies
    Given we have executed queries on clickhouse01
    """
    CREATE TABLE test_db.table_01 (n Int32, s String)
    ENGINE = MergeTree() PARTITION BY n % 10 ORDER BY n;

    CREATE MATERIALIZED VIEW test_db.mview_01
    ENGINE = MergeTree() PARTITION BY n % 10 ORDER BY n
    AS SELECT n, n * n AS "n2"
    FROM test_db.table_01;

    INSERT INTO test_db.table_01 SELECT number, toString(number) FROM system.numbers LIMIT 1000;

    DROP TABLE test_db.table_01;
    """
    When we create clickhouse01 clickhouse backup
    Then we got the following backups on clickhouse01
      | num | state    | data_count | link_count   |
      | 0   | created  | 10          | 0            |
    When we restore clickhouse backup #0 to clickhouse02
    Then clickhouse02 has same schema as clickhouse01
    And we got same clickhouse data at clickhouse01 clickhouse02

  @require_version_21.1
  Scenario: Create backup containing tables with EmbeddedRocksDB table engine family
    Given we have executed queries on clickhouse01
    """
    CREATE TABLE test_db.table_01 (key String, value UInt32) ENGINE = EmbeddedRocksDB PRIMARY KEY key
    """
    When we create clickhouse01 clickhouse backup
    Then we got the following backups on clickhouse01
      | num | state    | data_count | link_count   |
      | 0   | created  | 0          | 0            |
    When we restore clickhouse backup #0 to clickhouse02
    Then clickhouse02 has same schema as clickhouse01
    But on clickhouse02 tables are empty
