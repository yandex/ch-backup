Feature: Backup replicated merge tree table

  Background:
    Given default configuration
    And a working s3
    And a working clickhouse on clickhouse01
    And a working clickhouse on clickhouse02
    And a working zookeeper on zookeeper01

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
    PARTITION BY toYYYYMM(EventDate)
    ORDER BY (CounterID, EventDate, intHash32(UserID))
    SAMPLE BY intHash32(UserID);
    INSERT INTO test_db.table_01 SELECT now(), number, rand() FROM system.numbers LIMIT 10
    """
    When we create clickhouse01 clickhouse backup
    Then we got the following backups on clickhouse01
      | num | state    | data_count | link_count   |
      | 0   | created  | 1          | 0            |
    When we restore clickhouse backup #0 to clickhouse02
    Then we got same clickhouse data at clickhouse01 clickhouse02

  Scenario: Backup ReplicatedMergeTree table with static name
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
    When we create clickhouse01 clickhouse backup
    Then we got the following backups on clickhouse01
      | num | state    | data_count | link_count   |
      | 0   | created  | 2          | 0            |
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
