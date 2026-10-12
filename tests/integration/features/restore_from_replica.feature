Feature: Restore schema from a live replica

  Background:
    Given default configuration
    And a working s3
    And a working zookeeper on zookeeper01
    And a working clickhouse on clickhouse01
    And a working clickhouse on clickhouse02
    And we have enabled shared zookeeper for clickhouse01
    And we have enabled shared zookeeper for clickhouse02

  @require_version_23.8
  Scenario: Add a new replica with the schema of a live replica
    Given ClickHouse settings
    """
    allow_experimental_database_replicated: 1
    """
    And we have executed queries on clickhouse01
    """
    CREATE DATABASE test_db;
    CREATE TABLE test_db.table_01 (n UInt32)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/test_db.table_01_add', '{replica}')
    ORDER BY n;
    CREATE TABLE test_db.table_02 (n UInt32, v String) ENGINE = MergeTree ORDER BY n;
    CREATE MATERIALIZED VIEW test_db.mv_01
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/test_db.mv_01_add', '{replica}')
    ORDER BY n
    AS SELECT n FROM test_db.table_01;
    CREATE VIEW test_db.view_01 AS SELECT n FROM test_db.table_01;
    CREATE DICTIONARY test_db.dict_01 (n UInt32, v String)
    PRIMARY KEY n
    SOURCE(CLICKHOUSE(DB 'test_db' TABLE 'table_02'))
    LIFETIME(0)
    LAYOUT(FLAT());
    INSERT INTO test_db.table_01 SELECT number FROM system.numbers LIMIT 10;
    CREATE DATABASE repl_db ENGINE = Replicated('/databases/repl_db_add', 'shard1', '{replica}');
    CREATE TABLE repl_db.table_01 (n UInt32) ENGINE = ReplicatedMergeTree ORDER BY n;
    INSERT INTO repl_db.table_01 SELECT number FROM system.numbers LIMIT 5;
    """
    When we execute command on clickhouse02
    """
    ch-backup -c /etc/yandex/ch-backup/ch-backup.conf restore-from-replica --source-host clickhouse01
    """
    And we execute queries on clickhouse02
    """
    SYSTEM SYNC REPLICA test_db.table_01;
    SYSTEM SYNC REPLICA repl_db.table_01
    """
    Then clickhouse02 has same schema as clickhouse01
    When we execute query on clickhouse02
    """
    SELECT count() FROM system.tables AS t
    INNER JOIN remote('clickhouse01', system.tables) AS r USING (database, name, uuid)
    WHERE database IN ('test_db', 'repl_db')
    """
    Then we get response
    """
    7
    """
    When we execute query on clickhouse02
    """
    SELECT
        (SELECT count() FROM test_db.table_01),
        (SELECT count() FROM repl_db.table_01)
    FORMAT CSV
    """
    Then we get response
    """
    10,5
    """

  @require_version_23.8
  Scenario: Rebuild a replica with the schema of a live replica
    Given we have executed queries on clickhouse01
    """
    CREATE DATABASE test_db;
    CREATE TABLE test_db.table_01 (n UInt32)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/test_db.table_01_rebuild', '{replica}')
    ORDER BY n;
    INSERT INTO test_db.table_01 SELECT number FROM system.numbers LIMIT 10;
    """
    And we have executed queries on clickhouse02
    """
    CREATE DATABASE test_db;
    CREATE TABLE test_db.table_01 (n UInt32)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/test_db.table_01_rebuild', '{replica}')
    ORDER BY n;
    SYSTEM SYNC REPLICA test_db.table_01;
    """
    ## The disk of clickhouse02 is lost, its replica stays registered in ZooKeeper.
    When we stop clickhouse at clickhouse02
    And we execute command on clickhouse02
    """
    find /var/lib/clickhouse -mindepth 1 -maxdepth 1 ! -name access -exec rm -rf {} +
    """
    And we start clickhouse at clickhouse02
    And we execute command on clickhouse02
    """
    ch-backup -c /etc/yandex/ch-backup/ch-backup.conf restore-from-replica --source-host clickhouse01
    """
    And we execute queries on clickhouse02
    """
    SYSTEM SYNC REPLICA test_db.table_01
    """
    Then clickhouse02 has same schema as clickhouse01
    When we execute query on clickhouse02
    """
    SELECT count() FROM system.tables AS t
    INNER JOIN remote('clickhouse01', system.tables) AS r USING (database, name, uuid)
    WHERE database = 'test_db'
    """
    Then we get response
    """
    1
    """
    When we execute query on clickhouse02
    """
    SELECT count() FROM test_db.table_01
    """
    Then we get response
    """
    10
    """

  @require_version_23.8
  Scenario: Restore from a replica fails without changes on the host
    Given we have executed queries on clickhouse01
    """
    CREATE DATABASE test_db;
    CREATE TABLE test_db.table_01 (n UInt32)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/test_db.table_01_fail', 'clickhouse01')
    ORDER BY n;
    """
    When we try to execute command on clickhouse02
    """
    ch-backup -c /etc/yandex/ch-backup/ch-backup.conf restore-from-replica --source-host clickhouse02
    """
    Then we get response contains
    """
    is this replica itself
    """
    When we try to execute command on clickhouse02
    """
    ch-backup -c /etc/yandex/ch-backup/ch-backup.conf restore-from-replica --source-host clickhouse01 --replica-name clickhouse01
    """
    Then we get response contains
    """
    Replica name clickhouse01 to remove is the one of clickhouse01
    """
    When we try to execute command on clickhouse02
    """
    ch-backup -c /etc/yandex/ch-backup/ch-backup.conf restore-from-replica --source-host clickhouse01
    """
    Then we get response contains
    """
    Create statement of test_db.table_01 on clickhouse01 has replica name 'clickhouse01' without the {replica} macro
    """
    When we execute query on clickhouse01
    """
    SELECT is_readonly, active_replicas FROM system.replicas
    WHERE database = 'test_db' AND table = 'table_01'
    FORMAT CSV
    """
    Then we get response
    """
    0,1
    """
    Given we have executed queries on clickhouse01
    """
    DROP TABLE test_db.table_01 SYNC;
    CREATE TABLE test_db.s3_table (n UInt32)
    ENGINE = S3('http://s3:9000/bucket/data.csv', 'access_key', 'secret_key', 'CSV');
    """
    When we try to execute command on clickhouse02
    """
    ch-backup -c /etc/yandex/ch-backup/ch-backup.conf restore-from-replica --source-host clickhouse01
    """
    Then we get response contains
    """
    Create statement of test_db.s3_table on clickhouse01 has hidden secrets
    """
    When we execute query on clickhouse02
    """
    SELECT count() FROM system.databases WHERE name = 'test_db'
    """
    Then we get response
    """
    0
    """

  @require_version_less_than_23.8
  Scenario: Restore from a replica needs a supported ClickHouse version
    When we try to execute command on clickhouse02
    """
    ch-backup -c /etc/yandex/ch-backup/ch-backup.conf restore-from-replica --source-host clickhouse01
    """
    Then we get response contains
    """
    Restore from a replica needs ClickHouse 23.8 or newer
    """
