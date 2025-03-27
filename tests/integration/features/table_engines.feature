Feature: Backup of tables with different engines and configurations

  Background:
    Given default configuration
    And a working s3
    And a working zookeeper on zookeeper01
    And a working clickhouse on clickhouse01
    And a working clickhouse on clickhouse02

  @TableEngines
  @require_version_22.7
  Scenario Outline: Create backup containing <name> table
    Given ClickHouse settings
    """
    allow_deprecated_database_ordinary: 1
    """
    Given we execute query on clickhouse01
    """
    CREATE DATABASE test_db ENGINE=Ordinary
    """
    When we put following info in "/var/lib/clickhouse/metadata/test_db/test.sql" at clickhouse01
    """
    ATTACH TABLE test (s String, n Int32) ENGINE=<engine_request>
    """
    When we execute query on clickhouse01
    """
    ATTACH TABLE test_db.test
    """
    When we create clickhouse01 clickhouse backup
    Then we got the following backups on clickhouse01
      | num | state    | data_count | link_count   |
      | 0   | created  | 0          | 0            |
    When we restore clickhouse backup #0 to clickhouse02
    Then clickhouse02 has same schema as clickhouse01
    Examples:
      | name        | engine_request                                                                                              |
      | ODBC        | ODBC('', 'test', 'test')                                                                                    |
      | JDBC        | JDBC('jdbc:test_driver://test:1111/?user=test_user&password=test_password', 'test', 'test')                 |
      | RabbitMQ    | RabbitMQ SETTINGS rabbitmq_host_port='test:1111', rabbitmq_exchange_name='test_name', rabbitmq_format='TSV' |
      | HDFS        | HDFS('hdfs://test:1111/test_storage', 'TSV')                                                                |
      | Kafka       | Kafka('mysql:1111', 'test', 'test_group', 'TSV')                                                            |
      | MySQL       | MySQL('mysql:1111', 'test', 'test_table', 'test_user', 'clickhouse')                                        |
      | PostgreSQL  | PostgreSQL('postgre:1111', 'test', 'test_table', 'test_user', 'clickhouse')                                 |
      | MongoDB     | MongoDB('mongo:1111', 'test', 'test_table', 'test_user', 'clickhouse')                                      |

  @S3
  Scenario: Create backup containing s3 tables
    Given we execute query on clickhouse01
    """
    CREATE DATABASE test_db
    """
    When we execute query on clickhouse01
    """
    CREATE TABLE test_db.s3_test_table (s String, n Int32)
    ENGINE = S3('{{conf['s3']['endpoint']}}/ch-backup/s3_test_table.tsv', '{{conf['s3']['access_key_id']}}', '{{conf['s3']['access_secret_key']}}', 'TSV');
    """
    When we execute queries on clickhouse01
    """
    INSERT INTO test_db.s3_test_table VALUES ('one', 1);
    """
    When we create clickhouse01 clickhouse backup
    Then we got the following backups on clickhouse01
      | num | state    | data_count | link_count   |
      | 0   | created  | 0          | 0            |
    When we restore clickhouse backup #0 to clickhouse02
    Then clickhouse02 has same schema as clickhouse01
    And we got same clickhouse data at clickhouse01 clickhouse02

  @merge_tree
  Scenario: Create backup containing merge tree table with old style configuration
    Given ClickHouse settings
    """
    {% if ch_version_ge('22.7') %}
    allow_deprecated_syntax_for_merge_tree: 1
    {% endif %}
    """
    And we have executed queries on clickhouse01
    """
    CREATE DATABASE test_db;

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

  @merge_tree
  Scenario: Create backup containing merge tree tables with new style configuration
    Given we have executed queries on clickhouse01
    """
    CREATE DATABASE test_db;

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

  @merge_tree
  Scenario: Create backup containing merge tree table with implicit structure
    Given we have executed queries on clickhouse01
    """
    CREATE DATABASE test_db;

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

  @log
  Scenario: Create backup containing tables with log table engine family
    Given we have executed queries on clickhouse01
    """
    CREATE DATABASE test_db;

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

  @distributed
  Scenario: Create backup containing distributed tables
    Given we have executed queries on clickhouse01
    """
    CREATE DATABASE test_db;

    CREATE TABLE test_db.table_01 (n Int32, s String)
    ENGINE = MergeTree() PARTITION BY n % 10 ORDER BY n;
    INSERT INTO test_db.table_01 SELECT number, toString(number) FROM system.numbers LIMIT 1000;

    CREATE TABLE test_db.dtable_with_default_cluster AS test_db.table_01
    ENGINE = Distributed('default', 'test_db', 'table_01', n);

    CREATE TABLE test_db.dtable_with_clickhouse01_cluster AS test_db.table_01
    ENGINE = Distributed('clickhouse01', 'test_db', 'table_01', n);
    """
    When we create clickhouse01 clickhouse backup
    Then we got the following backups on clickhouse01
      | num | state    | data_count | link_count   |
      | 0   | created  | 10         | 0            |
    When we restore clickhouse backup #0 to clickhouse02
    Then clickhouse02 has same schema as clickhouse01
    And we got same clickhouse data at clickhouse01 clickhouse02

  @view
  @require_version_23.3
  Scenario: Create backup containing views
    Given ClickHouse settings
    """
    allow_experimental_live_view: 1
    """
    Given we have executed queries on clickhouse01
    """
    CREATE DATABASE test_db;
    CREATE DATABASE test_db2;

    CREATE TABLE test_db.table_01 (n Int32, s String)
    ENGINE = MergeTree() PARTITION BY n % 10 ORDER BY n;
    INSERT INTO test_db.table_01 SELECT number, toString(number) FROM system.numbers LIMIT 1000;

    CREATE TABLE test_db2.table_02 (n Int32, n2 Int32)
    ENGINE = MergeTree() PARTITION BY n % 10 ORDER BY n;
    INSERT INTO test_db2.table_02 SELECT number, number * number FROM system.numbers LIMIT 1000;

    CREATE VIEW test_db.view_on_single_table
    AS SELECT n, n * n AS "n2"
    FROM test_db.table_01;

    CREATE VIEW test_db2.view_on_multiple_tables
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

    CREATE LIVE VIEW test_db.live_view
    AS SELECT n, n * n AS "n2"
    FROM test_db.table_01;

    CREATE VIEW test_db.parametrized_view
    AS WITH {a:UInt32} AS a, {b:UInt32} AS b
    SELECT a + b
    """
    When we create clickhouse01 clickhouse backup
    Then we got the following backups on clickhouse01
      | num | state    | data_count | link_count   |
      | 0   | created  | 20         | 0            |
    When we restore clickhouse backup #0 to clickhouse02
    Then clickhouse02 has same schema as clickhouse01
    And we got same clickhouse data at clickhouse01 clickhouse02

  @view
  Scenario: Create backup containing materialized view with implicit backend table
    Given we have executed queries on clickhouse01
    """
    CREATE DATABASE `test-db`;
    CREATE TABLE `test-db`.table_01 (n Int32, s String)
    ENGINE = MergeTree() PARTITION BY n % 10 ORDER BY n;

    CREATE MATERIALIZED VIEW `test-db`.mview_01
    ENGINE = MergeTree() PARTITION BY n % 10 ORDER BY n
    AS SELECT n, n * n AS "n2"
    FROM `test-db`.table_01;

    INSERT INTO `test-db`.table_01 SELECT number, toString(number) FROM system.numbers LIMIT 1000;
    """
    When we create clickhouse01 clickhouse backup
    Then we got the following backups on clickhouse01
      | num | state    | data_count | link_count   |
      | 0   | created  | 20         | 0            |
    When we restore clickhouse backup #0 to clickhouse02
    Then clickhouse02 has same schema as clickhouse01
    And we got same clickhouse data at clickhouse01 clickhouse02

  @view
  Scenario: Create backup containing materialized view with explicit backend table
    Given we have executed queries on clickhouse01
    """
    CREATE DATABASE test_db;

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

  @view
  Scenario: Create backup containing materialized views and broken view dependencies
    Given we have executed queries on clickhouse01
    """
    CREATE DATABASE test_db;

    -- Source table that will be selected by materialized views
    CREATE TABLE test_db.table_01 (n Int32, s String)
    ENGINE MergeTree PARTITION BY n % 10 ORDER BY n;

    -- Materialized view with implicit backend table
    CREATE MATERIALIZED VIEW test_db.mview_01
    ENGINE MergeTree PARTITION BY n % 10 ORDER BY n
    AS SELECT n, n * n AS "n2" FROM test_db.table_01;

    -- Materialized view with explicit backend table
    CREATE TABLE test_db.mview_02_backend (n Int32, n2 Int64)
    ENGINE MergeTree PARTITION BY n % 10 ORDER BY n;

    CREATE MATERIALIZED VIEW test_db.mview_02 TO test_db.mview_02_backend
    AS SELECT n, n * n AS "n2" FROM test_db.table_01;

    -- Insert test data to the source table
    INSERT INTO test_db.table_01 SELECT number, toString(number) FROM system.numbers LIMIT 1000;

    -- Drop source table and break dependencies across tables and views
    DROP TABLE test_db.table_01;
    """
    When we create clickhouse01 clickhouse backup
    Then we got the following backups on clickhouse01
      | num | state    | data_count | link_count   |
      | 0   | created  | 20         | 0            |
    When we restore clickhouse backup #0 to clickhouse02
    Then clickhouse02 has same schema as clickhouse01
    And we got same clickhouse data at clickhouse01 clickhouse02

  @require_version_23.12
  @view
  Scenario: Create backup containing refreshable materialized views
    Given ClickHouse settings
    """
    allow_experimental_refreshable_materialized_view: 1
    """
    Given we have executed queries on clickhouse01
    """
    CREATE DATABASE test_db;

    CREATE TABLE test_db.table_01 (n Int32, s String)
    ENGINE = MergeTree() PARTITION BY n % 10 ORDER BY n;

    INSERT INTO test_db.table_01 SELECT number, toString(number) FROM system.numbers LIMIT 1000;

    CREATE TABLE test_db.mview_backend_01 (n Int32, n2 Int64)
    ENGINE = MergeTree() PARTITION BY n % 10 ORDER BY n;

    CREATE MATERIALIZED VIEW test_db.mview_01
    REFRESH EVERY 1 HOUR TO test_db.mview_backend_01
    AS SELECT n, n * n AS "n2"
    FROM test_db.table_01;

    SYSTEM REFRESH VIEW test_db.mview_01;
    """
    When we create clickhouse01 clickhouse backup
    Then we got the following backups on clickhouse01
      | num | state    | data_count | link_count   |
      | 0   | created  | 20         | 0            |
    When we restore clickhouse backup #0 to clickhouse02
    Then clickhouse02 has same schema as clickhouse01
    And we got same clickhouse data at clickhouse01 clickhouse02

  @rocksdb
  Scenario: Create backup containing tables with EmbeddedRocksDB engine
    Given we have executed queries on clickhouse01
    """
    CREATE DATABASE test_db;

    CREATE TABLE test_db.table_01 (key String, value UInt32) ENGINE = EmbeddedRocksDB PRIMARY KEY key
    """
    When we create clickhouse01 clickhouse backup
    Then we got the following backups on clickhouse01
      | num | state    | data_count | link_count   |
      | 0   | created  | 0          | 0            |
    When we restore clickhouse backup #0 to clickhouse02
    Then clickhouse02 has same schema as clickhouse01
    But on clickhouse02 tables are empty

  @rabbitmq
  Scenario: Create backup containing tables with RabbitMQ engine
    Given we have executed queries on clickhouse01
    """
    CREATE DATABASE test_db;

    ATTACH TABLE test_db.rabbitmq_table UUID 'b21b194c-2473-4a7d-b79a-4b2e84c20f72' (`json` String)
    ENGINE = RabbitMQ
    SETTINGS rabbitmq_host_port = 'rabbitmq_host:5672', rabbitmq_exchange_name = 'test_exchange',
             rabbitmq_format = 'TSV', rabbitmq_exchange_type = 'topic', rabbitmq_flush_interval_ms = 5000,
             rabbitmq_persistent = 1;
    """
    When we create clickhouse01 clickhouse backup
    Then we got the following backups on clickhouse01
      | num | state    | data_count | link_count   |
      | 0   | created  | 0          | 0            |
    When we restore clickhouse backup #0 to clickhouse02
    Then clickhouse02 has same schema as clickhouse01

  @merge_tree
  Scenario: Create backup containing merge tree tables with projections
    Given we have executed queries on clickhouse01
    """
    CREATE DATABASE test_db;

    CREATE TABLE test_db.table_01 (date Date, n Int32)
    ENGINE = MergeTree() PARTITION BY date ORDER BY date;
    INSERT INTO test_db.table_01 SELECT today(), number FROM system.numbers LIMIT 1000;

    ALTER TABLE test_db.table_01 ADD PROJECTION test_proj (SELECT n, count() GROUP BY n);
    ALTER TABLE test_db.table_01 MATERIALIZE PROJECTION test_proj;
    """
    When we create clickhouse01 clickhouse backup
    Then we got the following backups on clickhouse01
      | num | state   | data_count | link_count |
      | 0   | created | 1          | 0          |
    When we restore clickhouse backup #0 to clickhouse02
    Then clickhouse02 has same schema as clickhouse01
    And we got same clickhouse data at clickhouse01 clickhouse02

  @storage_proxy
  Scenario: Create backup containing StorageProxy table
    Given we have executed queries on clickhouse01
    """
    CREATE DATABASE test_db;

    CREATE TABLE test_db.table_01 (date Date, n Int32) AS url('http://127.0.0.1:12345/', CSV)
    """
    When we create clickhouse01 clickhouse backup
    Then we got the following backups on clickhouse01
      | num | state   | data_count | link_count |
      | 0   | created | 0          | 0          |
    When we restore clickhouse backup #0 to clickhouse02
    Then clickhouse02 has same schema as clickhouse01
