Feature: Backup replicated merge tree table

  Background:
    Given default configuration
    And a working s3
    And a working zookeeper on zookeeper01
    And a working clickhouse on clickhouse01
    And a working clickhouse on clickhouse02

  Scenario: Backup replicated table
    Given we have executed queries on clickhouse01
    """
    CREATE DATABASE test_db;
    CREATE TABLE test_db.table_01 (
        EventDate DateTime,
        CounterID UInt32,
        UserID UInt32
    )
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/shard_01/test_db.table_01', '{replica}')
    PARTITION BY CounterID % 10
    ORDER BY (CounterID, EventDate, intHash32(UserID))
    SAMPLE BY intHash32(UserID);
    INSERT INTO test_db.table_01 SELECT now(), number, rand() FROM system.numbers LIMIT 10
    """
    When we create clickhouse01 clickhouse backup
    Then we got the following backups on clickhouse01
      | num | state   | data_count | link_count |
      | 0   | created | 10         | 0          |
    When we restore clickhouse backup #0 to clickhouse02
    Then we got same clickhouse data at clickhouse01 clickhouse02
    When we execute query on clickhouse02
    """
    SELECT count() FROM test_db.table_01
    """
    Then we get response
    """
    10
    """
    When we execute query on clickhouse02
    """
    INSERT INTO test_db.table_01 SELECT now(), number, rand() FROM system.numbers LIMIT 10
    """
    When we execute query on clickhouse02
    """
    SELECT count() FROM test_db.table_01
    """
    Then we get response
    """
    20
    """

  @require_version_21.10
  Scenario: Backup & Restore databases with Replicated engine
    Given ClickHouse settings
    """
    allow_experimental_database_replicated: 1
    """
    Given we have executed queries on clickhouse01
    """
    CREATE DATABASE test_replicated_db
    ENGINE = Replicated('/clickhouse/databases/test_db', '{shard}', '{replica}');
    """
    When we create clickhouse01 clickhouse backup
    And we restore clickhouse backup #0 to clickhouse02
    Then clickhouse01 has same schema as clickhouse02

  @require_version_21.10
  Scenario: Backup Replicated database with static replica name
    Given ClickHouse settings
    """
    allow_experimental_database_replicated: 1
    """
    Given we have executed queries on clickhouse01
    """
    CREATE DATABASE test_db ENGINE=Replicated('some/path/test_db','test_shard','test_replica');
    """
    When we create clickhouse01 clickhouse backup
    When we restore clickhouse backup #0 to clickhouse02
    """
    override_replica_name: '{replica}'
    """
    And we execute query on clickhouse02
    """
    SELECT name FROM system.zookeeper WHERE path='/some/path/test_db/replicas'
    """
    Then we get response
    """
    test_shard|clickhouse02
    """

  @require_version_21.10
  Scenario: Override replicated database to single-node on restore with cmd flag
    Given ClickHouse settings
    """
    allow_experimental_database_replicated: 1
    """
    Given we have executed queries on clickhouse01
    """
    CREATE DATABASE test_db ENGINE=Replicated('some/path/test_db','test_shard','test_replica');
    """
    When we create clickhouse01 clickhouse backup
    When we restore clickhouse backup #0 to clickhouse02
    """
    force_non_replicated: true
    """
    And we execute query on clickhouse02
    """
    SELECT engine FROM system.databases WHERE name = 'test_db'
    """
    Then we get response
    """
    Atomic
    """

  @require_version_less_than_22.7
  Scenario: Backup ReplicatedMergeTree table with static replica name
    Given we have executed queries on clickhouse01
    """
    CREATE DATABASE test_db;
    CREATE TABLE test_db.table_01 (
        EventDate DateTime,
        CounterID UInt32,
        UserID UInt32
    )
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/shard_01/test_db.table_01', 'static_name')
    PARTITION BY toYYYYMM(EventDate)
    ORDER BY (CounterID, EventDate, intHash32(UserID))
    SAMPLE BY intHash32(UserID);
    INSERT INTO test_db.table_01 SELECT now(), number, rand() FROM system.numbers LIMIT 10
    """
    And we have executed queries on clickhouse01
    """
    CREATE TABLE test_db.table_02 (
        EventDate DateTime,
        CounterID UInt32,
        UserID UInt32
    )
    ENGINE = ReplicatedSummingMergeTree('/clickhouse/tables/shard_01/test_db.table_02', 'static_name')
    PARTITION BY toYYYYMM(EventDate)
    ORDER BY (CounterID, EventDate, intHash32(UserID))
    SAMPLE BY intHash32(UserID);
    INSERT INTO test_db.table_02 SELECT now(), number, rand() FROM system.numbers LIMIT 10
    """
    And we have executed queries on clickhouse01
    """
    CREATE TABLE test_db.legacy_table
    (
        EventDate Date,
        CounterID UInt32,
        UserID UInt32
    )
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/shard_01/test_db.legacy_table', 'static_name',
    EventDate, intHash32(UserID), (CounterID, EventDate, intHash32(UserID)), 8192);
    INSERT INTO test_db.legacy_table SELECT now(), number, rand() FROM system.numbers LIMIT 10
    """
    When we create clickhouse01 clickhouse backup
    Then we got the following backups on clickhouse01
      | num | state   | data_count | link_count |
      | 0   | created | 3          | 0          |
    When we restore clickhouse backup #0 to clickhouse02
    """
    override_replica_name: '{replica}'
    """
    And we execute query on clickhouse02
    """
    SELECT DISTINCT replica_name FROM system.replicas WHERE database = 'test_db'
    """
    Then we get response
    """
    clickhouse02
    """
    And we got same clickhouse data at clickhouse01 clickhouse02

  @require_version_22.7
  Scenario: Backup ReplicatedMergeTree table with static replica name
    Given ClickHouse settings
    """
    allow_deprecated_syntax_for_merge_tree: 1
    """
    And we have executed queries on clickhouse01
    """
    CREATE DATABASE test_db;
    CREATE TABLE test_db.table_01 (
        EventDate DateTime,
        CounterID UInt32,
        UserID UInt32
    )
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/shard_01/test_db.table_01', 'static_name')
    PARTITION BY toYYYYMM(EventDate)
    ORDER BY (CounterID, EventDate, intHash32(UserID))
    SAMPLE BY intHash32(UserID);
    INSERT INTO test_db.table_01 SELECT now(), number, rand() FROM system.numbers LIMIT 10
    """
    And we have executed queries on clickhouse01
    """
    CREATE TABLE test_db.table_02 (
        EventDate DateTime,
        CounterID UInt32,
        UserID UInt32
    )
    ENGINE = ReplicatedSummingMergeTree('/clickhouse/tables/shard_01/test_db.table_02', 'static_name')
    PARTITION BY toYYYYMM(EventDate)
    ORDER BY (CounterID, EventDate, intHash32(UserID))
    SAMPLE BY intHash32(UserID);
    INSERT INTO test_db.table_02 SELECT now(), number, rand() FROM system.numbers LIMIT 10
    """
    And we have executed queries on clickhouse01
    """
    CREATE TABLE test_db.legacy_table
    (
        EventDate Date,
        CounterID UInt32,
        UserID UInt32
    ) ENGINE = ReplicatedMergeTree('/clickhouse/tables/shard_01/test_db.legacy_table', 'static_name',
    EventDate, intHash32(UserID), (CounterID, EventDate, intHash32(UserID)), 8192);
    INSERT INTO test_db.legacy_table SELECT now(), number, rand() FROM system.numbers LIMIT 10
    """
    When we create clickhouse01 clickhouse backup
    Then we got the following backups on clickhouse01
      | num | state   | data_count | link_count |
      | 0   | created | 3          | 0          |
    When we restore clickhouse backup #0 to clickhouse02
    """
    override_replica_name: '{replica}'
    """
    And we execute query on clickhouse02
    """
    SELECT DISTINCT replica_name FROM system.replicas WHERE database = 'test_db'
    """
    Then we get response
    """
    clickhouse02
    """
    And we got same clickhouse data at clickhouse01 clickhouse02

  @require_version_less_than_22.7
  Scenario: Override replicated table to single-node on restore with cmd flag
    Given we have executed queries on clickhouse01
    """
    CREATE DATABASE test_db;
    CREATE TABLE test_db.table_01 (
        EventDate DateTime,
        CounterID UInt32,
        UserID UInt32
    )
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/shard_01/test_db.table_01', '{replica}')
    PARTITION BY toYYYYMM(EventDate)
    ORDER BY (CounterID, EventDate, intHash32(UserID))
    SAMPLE BY intHash32(UserID);
    INSERT INTO test_db.table_01 SELECT now(), number, rand() FROM system.numbers LIMIT 10
    """
    And we have executed queries on clickhouse01
    """
    CREATE TABLE test_db.table_02 (
        EventDate DateTime,
        CounterID UInt32,
        UserID UInt32
    )
    ENGINE = ReplicatedSummingMergeTree('/clickhouse/tables/shard_01/test_db.table_02', '{replica}')
    PARTITION BY toYYYYMM(EventDate)
    ORDER BY (CounterID, EventDate, intHash32(UserID))
    SAMPLE BY intHash32(UserID);
    INSERT INTO test_db.table_02 SELECT now(), number, rand() FROM system.numbers LIMIT 10
    """
    And we have executed queries on clickhouse01
    """
    CREATE TABLE test_db.legacy_table
    (
        EventDate Date,
        CounterID UInt32,
        UserID UInt32
    )
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/shard_01/test_db.legacy_table', '{replica}}',
    EventDate, intHash32(UserID), (CounterID, EventDate, intHash32(UserID)), 8192);
    INSERT INTO test_db.legacy_table SELECT now(), number, rand() FROM system.numbers LIMIT 10
    """
    When we create clickhouse01 clickhouse backup
    Then we got the following backups on clickhouse01
      | num | state   | data_count | link_count |
      | 0   | created | 3          | 0          |
    When we restore clickhouse backup #0 to clickhouse02
    """
    force_non_replicated: true
    """
    And we execute query on clickhouse02
    """
    SELECT DISTINCT engine FROM system.tables WHERE database = 'test_db' ORDER BY engine
    """
    Then we get response
    """
    MergeTree
    SummingMergeTree
    """
    Then we got same clickhouse data at clickhouse01 clickhouse02

  @require_version_22.7
  Scenario: Override replicated table to single-node on restore with cmd flag
    Given ClickHouse settings
    """
    allow_deprecated_syntax_for_merge_tree: 1
    """
    And we have executed queries on clickhouse01
    """
    CREATE DATABASE test_db;
    CREATE TABLE test_db.table_01 (
        EventDate DateTime,
        CounterID UInt32,
        UserID UInt32
    )
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/shard_01/test_db.table_01', '{replica}')
    PARTITION BY toYYYYMM(EventDate)
    ORDER BY (CounterID, EventDate, intHash32(UserID))
    SAMPLE BY intHash32(UserID);
    INSERT INTO test_db.table_01 SELECT now(), number, rand() FROM system.numbers LIMIT 10
    """
    And we have executed queries on clickhouse01
    """
    CREATE TABLE test_db.table_02 (
        EventDate DateTime,
        CounterID UInt32,
        UserID UInt32
    )
    ENGINE = ReplicatedSummingMergeTree('/clickhouse/tables/shard_01/test_db.table_02', '{replica}')
    PARTITION BY toYYYYMM(EventDate)
    ORDER BY (CounterID, EventDate, intHash32(UserID))
    SAMPLE BY intHash32(UserID);
    INSERT INTO test_db.table_02 SELECT now(), number, rand() FROM system.numbers LIMIT 10
    """
    And we have executed queries on clickhouse01
    """
    CREATE TABLE test_db.legacy_table
    (
        EventDate Date,
        CounterID UInt32,
        UserID UInt32
    )
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/shard_01/test_db.legacy_table', '{replica}}',
    EventDate, intHash32(UserID), (CounterID, EventDate, intHash32(UserID)), 8192);
    INSERT INTO test_db.legacy_table SELECT now(), number, rand() FROM system.numbers LIMIT 10
    """
    When we create clickhouse01 clickhouse backup
    Then we got the following backups on clickhouse01
      | num | state   | data_count | link_count |
      | 0   | created | 3          | 0          |
    When we restore clickhouse backup #0 to clickhouse02
    """
    force_non_replicated: true
    """
    And we execute query on clickhouse02
    """
    SELECT DISTINCT engine FROM system.tables WHERE database = 'test_db' ORDER BY engine
    """
    Then we get response
    """
    MergeTree
    SummingMergeTree
    """
    Then we got same clickhouse data at clickhouse01 clickhouse02

  Scenario: Override replicated table to single-node on restore with config
    Given ch-backup configuration on clickhouse02
    """
    backup:
        force_non_replicated: True
    """
    And we have executed queries on clickhouse01
    """
    CREATE DATABASE test_db;
    CREATE TABLE test_db.table_01 (
        EventDate DateTime,
        CounterID UInt32,
        UserID UInt32
    )
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/shard_01/test_db.table_01', '{replica}')
    PARTITION BY toYYYYMM(EventDate)
    ORDER BY (CounterID, EventDate, intHash32(UserID))
    SAMPLE BY intHash32(UserID);
    INSERT INTO test_db.table_01 SELECT now(), number, rand() FROM system.numbers LIMIT 10
    """
    When we create clickhouse01 clickhouse backup
    Then we got the following backups on clickhouse01
      | num | state   | data_count | link_count |
      | 0   | created | 1          | 0          |
    When we restore clickhouse backup #0 to clickhouse02
    And we execute query on clickhouse02
    """
    SELECT DISTINCT engine FROM system.tables WHERE database = 'test_db'
    """
    Then we get response
    """
    MergeTree
    """
    Then we got same clickhouse data at clickhouse01 clickhouse02

  @require_version_less_than_22.7
  Scenario: Override replicated table to single-node incremental
    Given we have executed queries on clickhouse01
    """
    CREATE DATABASE test_db;
    CREATE TABLE test_db.table_01 (
        EventDate DateTime,
        CounterID UInt32,
        UserID UInt32
    )
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/shard_01/test_db.table_01', '{replica}')
    PARTITION BY toYYYYMM(EventDate)
    ORDER BY (CounterID, EventDate, intHash32(UserID))
    SAMPLE BY intHash32(UserID);
    INSERT INTO test_db.table_01 SELECT now(), number, rand() FROM system.numbers LIMIT 10
    """
    And we have executed queries on clickhouse01
    """
    CREATE TABLE test_db.table_02 (
        EventDate DateTime,
        CounterID UInt32,
        UserID UInt32
    )
    ENGINE = ReplicatedSummingMergeTree('/clickhouse/tables/shard_01/test_db.table_02', '{replica}')
    PARTITION BY toYYYYMM(EventDate)
    ORDER BY (CounterID, EventDate, intHash32(UserID))
    SAMPLE BY intHash32(UserID);
    INSERT INTO test_db.table_02 SELECT now(), number, rand() FROM system.numbers LIMIT 10
    """
    And we have executed queries on clickhouse01
    """
    CREATE TABLE test_db.legacy_table
    (
        EventDate Date,
        CounterID UInt32,
        UserID UInt32
    ) ENGINE = ReplicatedMergeTree('/clickhouse/tables/shard_01/test_db.legacy_table', '{replica}}',
    EventDate, intHash32(UserID), (CounterID, EventDate, intHash32(UserID)), 8192);
    INSERT INTO test_db.legacy_table SELECT now(), number, rand() FROM system.numbers LIMIT 10
    """
    When we create clickhouse01 clickhouse backup
    Then we got the following backups on clickhouse01
      | num | state   | data_count | link_count |
      | 0   | created | 3          | 0          |
    When we restore clickhouse backup #0 to clickhouse02
    """
    force_non_replicated: true
    schema_only: true
    """
    And we execute query on clickhouse02
    """
    SELECT DISTINCT engine FROM system.tables WHERE database = 'test_db' ORDER BY engine
    """
    When we restore clickhouse backup #0 to clickhouse02
    """
    force_non_replicated: true
    """
    Then we get response
    """
    MergeTree
    SummingMergeTree
    """
    Then we got same clickhouse data at clickhouse01 clickhouse02

  @require_version_22.7
  Scenario: Override replicated table to single-node incremental
    Given ClickHouse settings
    """
    allow_deprecated_syntax_for_merge_tree: 1
    """
    And we have executed queries on clickhouse01
    """
    CREATE DATABASE test_db;
    CREATE TABLE test_db.table_01 (
        EventDate DateTime,
        CounterID UInt32,
        UserID UInt32
    )
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/shard_01/test_db.table_01', '{replica}')
    PARTITION BY toYYYYMM(EventDate)
    ORDER BY (CounterID, EventDate, intHash32(UserID))
    SAMPLE BY intHash32(UserID);
    INSERT INTO test_db.table_01 SELECT now(), number, rand() FROM system.numbers LIMIT 10
    """
    And we have executed queries on clickhouse01
    """
    CREATE TABLE test_db.table_02 (
        EventDate DateTime,
        CounterID UInt32,
        UserID UInt32
    )
    ENGINE = ReplicatedSummingMergeTree('/clickhouse/tables/shard_01/test_db.table_02', '{replica}')
    PARTITION BY toYYYYMM(EventDate)
    ORDER BY (CounterID, EventDate, intHash32(UserID))
    SAMPLE BY intHash32(UserID);
    INSERT INTO test_db.table_02 SELECT now(), number, rand() FROM system.numbers LIMIT 10
    """
    And we have executed queries on clickhouse01
    """
    CREATE TABLE test_db.legacy_table
    (
        EventDate Date,
        CounterID UInt32,
        UserID UInt32
    ) ENGINE = ReplicatedMergeTree('/clickhouse/tables/shard_01/test_db.legacy_table', '{replica}}',
    EventDate, intHash32(UserID), (CounterID, EventDate, intHash32(UserID)), 8192);
    INSERT INTO test_db.legacy_table SELECT now(), number, rand() FROM system.numbers LIMIT 10
    """
    When we create clickhouse01 clickhouse backup
    Then we got the following backups on clickhouse01
      | num | state   | data_count | link_count |
      | 0   | created | 3          | 0          |
    When we restore clickhouse backup #0 to clickhouse02
    """
    force_non_replicated: true
    schema_only: true
    """
    And we execute query on clickhouse02
    """
    SELECT DISTINCT engine FROM system.tables WHERE database = 'test_db' ORDER BY engine
    """
    When we restore clickhouse backup #0 to clickhouse02
    """
    force_non_replicated: true
    """
    Then we get response
    """
    MergeTree
    SummingMergeTree
    """
    Then we got same clickhouse data at clickhouse01 clickhouse02

  Scenario: Backup ReplicatedMergeTree materialized view with static replica name
    Given we have executed queries on clickhouse01
    """
    CREATE DATABASE test_db;
    CREATE MATERIALIZED VIEW test_db.view_01 (`n` Int32)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/shard_01/test_db.table_01', 'static_name')
    PARTITION BY tuple() ORDER BY n SETTINGS index_granularity = 8192
    AS
    SELECT number AS n FROM system.numbers LIMIT 10
    """
    And we have executed queries on clickhouse01
    """
    CREATE MATERIALIZED VIEW test_db.view_02 (`n` Int32)
    ENGINE = ReplicatedSummingMergeTree('/clickhouse/tables/shard_01/test_db.table_02', 'static_name')
    PARTITION BY tuple() ORDER BY n SETTINGS index_granularity = 8192
    AS
    SELECT number AS n FROM system.numbers LIMIT 10
    """
    When we create clickhouse01 clickhouse backup
    Then we got the following backups on clickhouse01
      | num | state   | data_count | link_count |
      | 0   | created | 0          | 0          |
    When we restore clickhouse backup #0 to clickhouse02
    """
    override_replica_name: '{replica}'
    """
    And we execute query on clickhouse02
    """
    SELECT DISTINCT replica_name FROM system.replicas WHERE database = 'test_db'
    """
    Then we get response
    """
    clickhouse02
    """
    And we got same clickhouse data at clickhouse01 clickhouse02

  Scenario: Override replicated view to single-node on restore
    Given we have executed queries on clickhouse01
    """
    CREATE DATABASE test_db;
    CREATE MATERIALIZED VIEW test_db.view_01 (`n` Int32)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/shard_01/test_db.table_01', 'static_name')
    PARTITION BY tuple() ORDER BY n SETTINGS index_granularity = 8192
    AS
    SELECT number AS n FROM system.numbers LIMIT 10
    """
    And we have executed queries on clickhouse01
    """
    CREATE MATERIALIZED VIEW test_db.view_02 (`n` Int32)
    ENGINE = ReplicatedSummingMergeTree('/clickhouse/tables/shard_01/test_db.table_02', 'static_name')
    PARTITION BY tuple() ORDER BY n SETTINGS index_granularity = 8192
    AS
    SELECT number AS n FROM system.numbers LIMIT 10
    """
    When we create clickhouse01 clickhouse backup
    Then we got the following backups on clickhouse01
      | num | state   | data_count | link_count |
      | 0   | created | 0          | 0          |
    When we restore clickhouse backup #0 to clickhouse02
    """
    force_non_replicated: true
    """
    And we execute query on clickhouse02
    """
    SELECT DISTINCT engine FROM system.tables WHERE database = 'test_db' ORDER BY engine
    """
    Then we get response
    """
    MaterializedView
    MergeTree
    SummingMergeTree
    """
    Then we got same clickhouse data at clickhouse01 clickhouse02

  Scenario: Restore dirty host
    Given we have executed queries on clickhouse01
    """
    CREATE DATABASE test_db;
    CREATE TABLE test_db.table_01 (
        EventDate DateTime,
        CounterID UInt32,
        UserID UInt32
    )
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/test_db.table_01', '{replica}')
    PARTITION BY toYYYYMM(EventDate)
    ORDER BY (CounterID, EventDate, intHash32(UserID))
    SAMPLE BY intHash32(UserID);
    INSERT INTO test_db.table_01 SELECT now(), number, rand() FROM system.numbers LIMIT 10
    """
    When we create clickhouse01 clickhouse backup
    Then we got the following backups on clickhouse01
      | num | state   | data_count | link_count |
      | 0   | created | 1          | 0          |
    When we dirty remove clickhouse data at clickhouse01
    And we restore clickhouse backup #0 to clickhouse01
    """
    override_replica_name: '{replica}'
    clean_zookeeper_mode: 'replica-only'
    replica_name: clickhouse01
    """
    When we restore clickhouse backup #0 to clickhouse02
    Then we got same clickhouse data at clickhouse01 clickhouse02

  Scenario: Restore with default database
    Given we have executed queries on clickhouse01
    """
    CREATE TABLE default.table_01 (
        EventDate DateTime,
        CounterID UInt32,
        UserID UInt32
    )
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/test_db.table_01', '{replica}')
    PARTITION BY toYYYYMM(EventDate)
    ORDER BY (CounterID, EventDate, intHash32(UserID))
    SAMPLE BY intHash32(UserID);
    INSERT INTO default.table_01 SELECT now(), number, rand() FROM system.numbers LIMIT 10
    """
    When we create clickhouse01 clickhouse backup
    Then we got the following backups on clickhouse01
      | num | state   | data_count | link_count |
      | 0   | created | 1          | 0          |
    When we restore clickhouse backup #0 to clickhouse02
    Then we got same clickhouse data at clickhouse01 clickhouse02

  @require_version_20.10
  Scenario: Backup replicated table with implicit parameters
    Given we have executed queries on clickhouse01
    """
    CREATE DATABASE test_db Engine = Atomic;
    CREATE TABLE test_db.table_01 ON CLUSTER 'default' (
        EventDate DateTime,
        CounterID UInt32,
        UserID UInt32
    )
    ENGINE = ReplicatedMergeTree()
    PARTITION BY CounterID % 10
    ORDER BY (CounterID, EventDate, intHash32(UserID))
    SAMPLE BY intHash32(UserID);

    CREATE MATERIALIZED VIEW test_db.view_01 ON CLUSTER 'default' (
        `n` Int32
    )
    ENGINE = ReplicatedMergeTree()
    PARTITION BY tuple() ORDER BY n SETTINGS index_granularity = 8192
    AS SELECT CounterID n FROM test_db.table_01;

    INSERT INTO test_db.table_01 SELECT now(), number, rand() FROM system.numbers LIMIT 10
    """
    When we create clickhouse01 clickhouse backup
    Then we got the following backups on clickhouse01
      | num | state   | data_count | link_count |
      | 0   | created | 11         | 0          |
    When we restore clickhouse backup #0 to clickhouse02
    Then we got same clickhouse data at clickhouse01 clickhouse02

  @require_version_20.10
  Scenario: Override replicated table with implicit parameters to single-node
    Given we have executed queries on clickhouse01
    """
    CREATE DATABASE test_db Engine = Atomic;
    CREATE TABLE test_db.table_01 ON CLUSTER 'default' (
        EventDate DateTime,
        CounterID UInt32,
        UserID UInt32
    )
    ENGINE = ReplicatedMergeTree()
    PARTITION BY CounterID % 10
    ORDER BY (CounterID, EventDate, intHash32(UserID))
    SAMPLE BY intHash32(UserID);

    CREATE MATERIALIZED VIEW test_db.view_01 ON CLUSTER 'default' (
        `n` Int32
    )
    ENGINE = ReplicatedMergeTree()
    PARTITION BY tuple() ORDER BY n SETTINGS index_granularity = 8192
    AS SELECT CounterID n FROM test_db.table_01;

    INSERT INTO test_db.table_01 SELECT now(), number, rand() FROM system.numbers LIMIT 10
    """
    And we have executed queries on clickhouse01
    """
    CREATE TABLE test_db.table_02 ON CLUSTER 'default' (
        EventDate DateTime,
        CounterID UInt32,
        UserID UInt32
    )
    ENGINE = ReplicatedSummingMergeTree()
    PARTITION BY CounterID % 10
    ORDER BY (CounterID, EventDate, intHash32(UserID))
    SAMPLE BY intHash32(UserID);
    INSERT INTO test_db.table_02 SELECT now(), number, rand() FROM system.numbers LIMIT 10
    """
    When we create clickhouse01 clickhouse backup
    Then we got the following backups on clickhouse01
      | num | state   | data_count | link_count |
      | 0   | created | 21         | 0          |
    When we restore clickhouse backup #0 to clickhouse02
    """
    force_non_replicated: true
    """
    And we execute query on clickhouse02
    """
    SELECT DISTINCT engine FROM system.tables WHERE database = 'test_db' ORDER BY engine
    """
    Then we get response
    """
    MaterializedView
    MergeTree
    SummingMergeTree
    """
    Then we got same clickhouse data at clickhouse01 clickhouse02

  @require_version_21.8
  @require_version_less_than_22.7
  Scenario: Backup replicated table with invalid zk path
    Given we have executed queries on clickhouse01
    """
    CREATE DATABASE test_db ENGINE Ordinary;
    ATTACH TABLE test_db.table_01 (
        EventDate DateTime,
        CounterID UInt32,
        UserID UInt32
    )
    ENGINE = ReplicatedMergeTree('invalid_path', '{replica}')
    PARTITION BY CounterID % 10
    ORDER BY (CounterID, EventDate, intHash32(UserID))
    SAMPLE BY intHash32(UserID)
    """
    When we create clickhouse01 clickhouse backup
    Then we got the following backups on clickhouse01
      | num | state   | data_count | link_count |
      | 0   | created | 0          | 0          |
    When we restore clickhouse backup #0 to clickhouse02
    Then clickhouse02 has same schema as clickhouse01
    And we got same clickhouse data at clickhouse01 clickhouse02

  @require_version_22.7
  Scenario: Backup replicated table with invalid zk path
    Given ClickHouse settings
    """
    allow_deprecated_database_ordinary: 1
    """
    And we have executed queries on clickhouse01
    """
    CREATE DATABASE test_db ENGINE Ordinary;
    ATTACH TABLE test_db.table_01 (
        EventDate DateTime,
        CounterID UInt32,
        UserID UInt32
    )
    ENGINE = ReplicatedMergeTree('invalid_path', '{replica}')
    PARTITION BY CounterID % 10
    ORDER BY (CounterID, EventDate, intHash32(UserID))
    SAMPLE BY intHash32(UserID)
    """
    When we create clickhouse01 clickhouse backup
    Then we got the following backups on clickhouse01
      | num | state   | data_count | link_count |
      | 0   | created | 0          | 0          |
    When we restore clickhouse backup #0 to clickhouse02
    Then clickhouse02 has same schema as clickhouse01
    And we got same clickhouse data at clickhouse01 clickhouse02


  Scenario: Host resetup with zookeeper table cleanup
    Given we have enabled shared zookeeper for clickhouse01
    And we have enabled shared zookeeper for clickhouse02
    And we have executed queries on clickhouse01
    """
    DROP DATABASE IF EXISTS test_db SYNC;
    CREATE DATABASE test_db;
    CREATE TABLE test_db.table_01 (
        EventDate DateTime,
        CounterID UInt32,
        UserID UInt32
    )
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/shard01/test_db.table_01', '{replica}')
    PARTITION BY CounterID % 10
    ORDER BY (CounterID, EventDate, intHash32(UserID))
    SAMPLE BY intHash32(UserID);
    INSERT INTO test_db.table_01 SELECT now(), number, rand() FROM system.numbers LIMIT 10
    """
    When we create clickhouse01 clickhouse backup
    Then we got the following backups on clickhouse01
      | num | state   | data_count | link_count |
      | 0   | created | 10         | 0          |

    When we stop clickhouse at clickhouse01
    When we restore clickhouse backup #0 to clickhouse02
    """
    replica_name: clickhouse01
    schema_only: true
    """
    When we start clickhouse at clickhouse01
    Then there are no zk node on zookeeper01
    """
    zookeeper_path: /{{ conf.zk.shared_node }}/clickhouse/tables/shard01/test_db.table_01/replicas/clickhouse01
    """

  Scenario Outline: Host resetup with database table cleanup
    Given we have enabled shared zookeeper for clickhouse01
    And we have enabled shared zookeeper for clickhouse02
    Given ClickHouse settings
    """
      allow_experimental_database_replicated: 1
    """
    And we have executed queries on clickhouse01
    """
    DROP DATABASE IF EXISTS db_repl SYNC;
    CREATE DATABASE db_repl ENGINE = Replicated('<zookeeper_path>', 'shard_01', '{replica}')
    """
    When we create clickhouse01 clickhouse backup
    Then we got the following backups on clickhouse01
      | num | state   | data_count | link_count |
      | 0   | created | 0         | 0          |

    When we stop clickhouse at clickhouse01
    When we restore clickhouse backup #0 to clickhouse02
    """
    replica_name: clickhouse01
    schema_only: true
    """
    When we start clickhouse at clickhouse01
    Then database replica db_repl on clickhouse01 does not exists
    
    @require_version_24.8
    Examples:
    | zookeeper_path          |
    |/databases/{uuid}/db_repl|
    
    @require_version_22.8
    Examples:
    | zookeeper_path          |
    |/databases/replicated/db_repl|

  Scenario Outline: Add new host with replicated
    Given we have enabled shared zookeeper for clickhouse01
    And we have enabled shared zookeeper for clickhouse02
    Given ClickHouse settings
    """
    allow_experimental_database_replicated: 1
    """
    And we have executed queries on clickhouse01
    """
    DROP DATABASE IF EXISTS db_repl ON CLUSTER 'default' SYNC;
    CREATE DATABASE db_repl ENGINE = Replicated('<zookeeper_path>', 'shard_01', '{replica}');

    DROP TABLE IF EXISTS table_01 ON CLUSTER 'default' SYNC;
    CREATE TABLE table_01 (
        EventDate DateTime,
        CounterID UInt32,
        UserID UInt32
    )
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/shard01/test_db.table_01', '{replica}')
    PARTITION BY CounterID % 10
    ORDER BY (CounterID, EventDate, intHash32(UserID))
    SAMPLE BY intHash32(UserID);
    INSERT INTO table_01 SELECT now(), number, rand() FROM system.numbers LIMIT 10;
    """
    When we create clickhouse01 clickhouse backup

    When we restore clickhouse backup #0 to clickhouse02
    """
    schema_only: true
    """
    Then we got same clickhouse data at clickhouse01 clickhouse02
    
    @require_version_24.8
    Examples:
    | zookeeper_path          |
    |/databases/{uuid}/db_repl|
    
    @require_version_22.8
    Examples:
    | zookeeper_path          |
    |/databases/replicated/db_repl|

  ## Note: Sometimes we can have active orphaned table in the zookeeper.
  ## Here we are imitating such situation by creating objects with static replica name.
  @require_version_23.3
  Scenario: Host resetup with active orphaned objects in zookeeper.
    Given we have enabled shared zookeeper for clickhouse01
    And we have enabled shared zookeeper for clickhouse02
    And ClickHouse settings
    """
      allow_experimental_database_replicated: 1
    """
    And we have executed queries on clickhouse01
    """
    DROP DATABASE IF EXISTS test_db SYNC;
    CREATE DATABASE test_db;
    CREATE TABLE test_db.table_01 (
        EventDate DateTime,
        CounterID UInt32,
        UserID UInt32
    )
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/shard01/test_db.table_01', 'replica')
    PARTITION BY CounterID % 10
    ORDER BY (CounterID, EventDate, intHash32(UserID))
    SAMPLE BY intHash32(UserID);
    INSERT INTO test_db.table_01 SELECT now(), number, rand() FROM system.numbers LIMIT 10;

    DROP DATABASE IF EXISTS db_repl SYNC;
    CREATE DATABASE db_repl ENGINE = Replicated('/databases/replicated/db_repl', 'shard_01', 'replica');
    """
    When we create clickhouse01 clickhouse backup
    Then we got the following backups on clickhouse01
      | num | state   | data_count | link_count |
      | 0   | created | 10         | 0          |

    When we restore clickhouse backup #0 to clickhouse02
    """
    replica_name: replica
    schema_only: true
    """

  Scenario Outline: Clean metadata modes
    Given we have executed queries on clickhouse01
    """
    CREATE DATABASE test_db;
    CREATE TABLE test_db.table_01 (id UInt32)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/shard1/test_db.table_01', 'r1')
    ORDER BY id;

    INSERT INTO test_db.table_01 SELECT number FROM system.numbers LIMIT 10;

    CREATE TABLE test_db.table_02 (id UInt32)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/shard1/test_db.table_01', 'r2')
    ORDER BY id;
    """
    When we create clickhouse01 clickhouse backup
    """
    tables:
      - test_db.table_01
    """
    Then we got the following backups on clickhouse01
      | num | state   | data_count | link_count |
      | 0   | created | 1          | 0          |
    When we dirty remove clickhouse data at clickhouse01
    And we restore clickhouse backup #0 to clickhouse01
    """
    override_replica_name: 'r1'
    clean_zookeeper_mode: '<clean_zookeeper_mode>'
    replica_name: 'r1'
    """
    And we execute ZK list query on zookeeper01
    """
    /clickhouse01/clickhouse/tables/shard1/test_db.table_01/replicas
    """
    Then we get ZK list with len <len>
    When we restore clickhouse backup #0 to clickhouse02
    Then we got same clickhouse data at clickhouse01 clickhouse02

    Examples:
      | clean_zookeeper_mode | len |
      | replica-only         |  2  |
      | all-replicas         |  1  |

  @require_version_23.3
  Scenario Outline: Clean metadata modes for replicated database
    Given ClickHouse settings
    """
      allow_experimental_database_replicated: 1
    """
    And we have executed queries on clickhouse01
    """
    CREATE DATABASE test_db1 ENGINE Replicated('/clickhouse/databases/testdb', '{shard}', 'r1');
    CREATE DATABASE test_db2 ENGINE Replicated('/clickhouse/databases/testdb', '{shard}', 'r2');
    """
    When we create clickhouse01 clickhouse backup
    """
    databases:
      - test_db1
    """
    Then we got the following backups on clickhouse01
      | num | state   | data_count | link_count |
      | 0   | created | 0          | 0          |
    When we dirty remove clickhouse data at clickhouse01
    And we restore clickhouse backup #0 to clickhouse01
    """
    override_replica_name: 'r1'
    clean_zookeeper_mode: '<clean_zookeeper_mode>'
    replica_name: 'r1'
    """
    And we execute ZK list query on zookeeper01
    """
    /clickhouse01/clickhouse/databases/testdb/replicas
    """
    Then we get ZK list with len <len>
    When we restore clickhouse backup #0 to clickhouse02
    Then we got same clickhouse data at clickhouse01 clickhouse02

    Examples:
      | clean_zookeeper_mode | len |
      | replica-only         |  2  |
      | all-replicas         |  1  |
