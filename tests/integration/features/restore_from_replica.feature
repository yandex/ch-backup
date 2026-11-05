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
    ## S3 is unreachable: the restore must not need it.
    When we execute command on clickhouse02
    """
    ch-backup -c /etc/yandex/ch-backup/ch-backup.conf \
        --config-parameter storage.credentials.endpoint_url '"http://127.0.0.1:1"' \
        restore-from-replica --source-host clickhouse01
    """
    When we synchronize replica test_db.table_01 on clickhouse02
    When we synchronize replica repl_db.table_01 on clickhouse02
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
    """
    When we synchronize replica test_db.table_01 on clickhouse02
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
    When we synchronize replica test_db.table_01 on clickhouse02
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
    is this host itself
    """
    When we try to execute command on clickhouse02
    """
    ch-backup -c /etc/yandex/ch-backup/ch-backup.conf restore-from-replica --source-host clickhouse01 --replica-name clickhouse01
    """
    Then we get response contains
    """
    Replica clickhouse01 to remove from ZooKeeper is the source clickhouse01
    """
    When we try to execute command on clickhouse02
    """
    ch-backup -c /etc/yandex/ch-backup/ch-backup.conf restore-from-replica --source-host clickhouse01
    """
    Then we get response contains
    """
    test_db.table_01 gets replica name 'clickhouse01' instead of the {replica} macro
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
    Secrets of test_db.s3_table on clickhouse01 are hidden
    """
    When we execute query on clickhouse02
    """
    SELECT count() FROM system.databases WHERE name = 'test_db'
    """
    Then we get response
    """
    0
    """

  @require_version_23.8
  Scenario: Restore access entities and functions from a live replica
    Given we replace config file display_secrets.xml in favor of display_secrets.xml on clickhouse01 with restart
    And we have executed queries on clickhouse01
    """
    CREATE DATABASE test_db;
    CREATE TABLE test_db.table_01 (n UInt32)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/test_db.table_01_access', '{replica}')
    ORDER BY n;
    INSERT INTO test_db.table_01 SELECT number FROM system.numbers LIMIT 10;
    CREATE ROLE test_role;
    CREATE SETTINGS PROFILE test_profile SETTINGS max_threads = 2 TO test_role;
    CREATE USER test_user IDENTIFIED WITH sha256_password BY 'test_password'
    SETTINGS PROFILE 'test_profile';
    GRANT test_role TO test_user;
    ALTER USER test_user DEFAULT ROLE test_role;
    GRANT SELECT ON test_db.* TO test_role;
    CREATE QUOTA test_quota FOR INTERVAL 1 hour MAX queries = 100 TO test_user;
    CREATE ROW POLICY test_policy ON test_db.table_01 USING n > 5 TO test_user;
    CREATE USER a_user IDENTIFIED WITH no_password;
    CREATE USER z_user IDENTIFIED WITH no_password;
    ALTER USER a_user GRANTEES z_user;
    ALTER USER z_user GRANTEES a_user;
    CREATE FUNCTION test_udf AS x -> x + 1;
    """
    When we execute command on clickhouse02
    """
    ch-backup -c /etc/yandex/ch-backup/ch-backup.conf restore-from-replica --source-host clickhouse01
    """
    When we synchronize replica test_db.table_01 on clickhouse02
    Then clickhouse02 has same access control objects as clickhouse01
    When we execute command on clickhouse02
    """
    clickhouse-client --user test_user --password test_password \
        --query "SELECT count(), test_udf(1) FROM test_db.table_01 FORMAT CSV"
    """
    Then we get response
    """
    4,2
    """
    ## A second run replaces the access entities and keeps references between them.
    When we execute command on clickhouse02
    """
    ch-backup -c /etc/yandex/ch-backup/ch-backup.conf restore-from-replica --source-host clickhouse01 --access
    """
    Then clickhouse02 has same access control objects as clickhouse01
    When we execute command on clickhouse02
    """
    clickhouse-client --user test_user --password test_password \
        --query "SELECT count(), test_udf(1) FROM test_db.table_01 FORMAT CSV"
    """
    Then we get response
    """
    4,2
    """

  @require_version_25.5
  Scenario: Restore named collections from a live replica
    Given we replace config file display_secrets.xml in favor of display_secrets.xml on clickhouse01 with restart
    And we have executed queries on clickhouse01
    """
    CREATE NAMED COLLECTION test_nc AS host = 'h', port = 3306, password = 'secret';
    """
    When we execute command on clickhouse02
    """
    ch-backup -c /etc/yandex/ch-backup/ch-backup.conf restore-from-replica --source-host clickhouse01 --nc
    """
    Then clickhouse02 has same named collections as clickhouse01

  @require_version_24.11
  Scenario: Restore workload entities from a live replica
    Given we have executed queries on clickhouse01
    """
    CREATE RESOURCE s3_read (READ DISK s3);
    CREATE WORKLOAD all SETTINGS max_bytes_per_second = 2147483648;
    CREATE WORKLOAD user IN all SETTINGS weight = 9;
    CREATE WORKLOAD production IN user SETTINGS weight = 3;
    """
    ## A changed parent with a child can not be dropped, it is replaced in place.
    And we have executed queries on clickhouse02
    """
    CREATE WORKLOAD all;
    CREATE WORKLOAD user IN all;
    """
    When we execute command on clickhouse02
    """
    ch-backup -c /etc/yandex/ch-backup/ch-backup.conf restore-from-replica --source-host clickhouse01
    """
    And we execute command on clickhouse02
    """
    ch-backup -c /etc/yandex/ch-backup/ch-backup.conf restore-from-replica --source-host clickhouse01 --workload
    """
    And we execute query on clickhouse02
    """
    SELECT
        (SELECT count() FROM system.workloads
         INNER JOIN remote('clickhouse01', system.workloads) AS r USING (name, parent, create_query)),
        (SELECT count() FROM system.resources
         INNER JOIN remote('clickhouse01', system.resources) AS r USING (name, create_query))
    FORMAT CSV
    """
    Then we get response
    """
    3,1
    """

  @require_version_23.8
  @require_version_less_than_25.5
  Scenario: Named collections from a replica need a supported ClickHouse version
    Given we have executed queries on clickhouse01
    """
    CREATE DATABASE test_db;
    CREATE TABLE test_db.table_01 (n UInt32) ENGINE = MergeTree ORDER BY n;
    CREATE NAMED COLLECTION test_nc AS host = 'h';
    """
    When we try to execute command on clickhouse02
    """
    ch-backup -c /etc/yandex/ch-backup/ch-backup.conf restore-from-replica --source-host clickhouse01
    """
    Then we get response contains
    """
    Named collections test_nc are missing on this host, and restoring them needs ClickHouse 25.5 or newer
    """
    When we execute command on clickhouse02
    """
    ch-backup -c /etc/yandex/ch-backup/ch-backup.conf restore-from-replica --source-host clickhouse01 --schema --access --udf
    """
    Then clickhouse02 has same schema as clickhouse01

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
