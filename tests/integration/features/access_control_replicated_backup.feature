Feature: Backup and restore functionality of replicated access control entities

  Background:
    Given default configuration
    And a working s3
    And a working zookeeper on zookeeper01
    And a working clickhouse on clickhouse01
    And a working clickhouse on clickhouse02
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
    INSERT INTO test_db.table_01 SELECT now(), number, rand() FROM system.numbers LIMIT 1000
    """

  @require_version_22.3
  Scenario: check new files after backup from replicated storage
    Given we have executed command on clickhouse01
    """
    find /var/lib/clickhouse/access -name "*.sql" | wc -l
    """
    Then we get response
    """
    0
    """
    Given we have dirty enabled replicated access on clickhouse01 with restart
    And a working clickhouse on clickhouse01
    And we have executed queries on clickhouse01
    """
    CREATE USER test_user IDENTIFIED WITH plaintext_password BY 'password';
    CREATE ROLE test_role;
    CREATE ROW POLICY filter ON test_db.table_01 FOR SELECT USING CounterID < 5 TO test_role;
    CREATE QUOTA test_quota FOR INTERVAL 1 DAY MAX QUERIES 10 TO test_role;
    CREATE SETTINGS PROFILE memory_profile SETTINGS max_memory_usage = 100000001 MIN 90000000 MAX 110000000 TO test_role;
    """
    When we create clickhouse01 clickhouse backup
    Then we got the following backups on clickhouse01
      | num | state   | data_count | link_count |
      | 0   | created | 1          | 0          |


  @require_version_22.3
  Scenario: check ZK data after restore to replicated storage
    # 1. create backup from local
    Given we have executed queries on clickhouse01
    """
    CREATE USER test_user IDENTIFIED WITH plaintext_password BY 'password';
    CREATE ROLE test_role;
    CREATE ROW POLICY filter ON test_db.table_01 FOR SELECT USING CounterID < 5 TO test_role;
    CREATE QUOTA test_quota FOR INTERVAL 1 DAY MAX QUERIES 10 TO test_role;
    CREATE SETTINGS PROFILE memory_profile SETTINGS max_memory_usage = 100000001 MIN 90000000 MAX 110000000 TO test_role;
    """
    When we create clickhouse01 clickhouse backup
    Then we got the following backups on clickhouse01
      | num | state   | data_count | link_count |
      | 0   | created | 1          | 0          |
    # 2. pre-check that we don't have any access entities in ZK yet
    # check UUID
    When we execute ZK list query on zookeeper01
    """
    /clickhouse02/clickhouse/access/uuid
    """
    Then we get ZK list with len 0
    # check U
    When we execute ZK list query on zookeeper01
    """
    /clickhouse02/clickhouse/access/U
    """
    Then we get ZK list with len 0
    # check R
    When we execute ZK list query on zookeeper01
    """
    /clickhouse02/clickhouse/access/R
    """
    Then we get ZK list with len 0
    # check P
    When we execute ZK list query on zookeeper01
    """
    /clickhouse02/clickhouse/access/P
    """
    Then we get ZK list with len 0
    # check S
    When we execute ZK list query on zookeeper01
    """
    /clickhouse02/clickhouse/access/S
    """
    Then we get ZK list with len 0
    # check Q
    When we execute ZK list query on zookeeper01
    """
    /clickhouse02/clickhouse/access/Q
    """
    Then we get ZK list with len 0
    # 3. restore to replicated and check ZK again
    Given we have dirty enabled replicated access on clickhouse02 with restart
    And a working clickhouse on clickhouse02
    When we restore clickhouse access control metadata backup #0 to clickhouse02 with restart
    Given a working clickhouse on clickhouse02
    When we restore clickhouse backup #0 to clickhouse02
    And we execute ZK list query on zookeeper01
    """
    /clickhouse02/clickhouse/access/uuid
    """
    Then we get ZK list with len 5
    # check U
    When we execute ZK list query on zookeeper01
    """
    /clickhouse02/clickhouse/access/U
    """
    Then we get ZK list with len 1
    # check R
    When we execute ZK list query on zookeeper01
    """
    /clickhouse02/clickhouse/access/R
    """
    Then we get ZK list with len 1
    # check P
    When we execute ZK list query on zookeeper01
    """
    /clickhouse02/clickhouse/access/P
    """
    Then we get ZK list with len 1
    # check S
    When we execute ZK list query on zookeeper01
    """
    /clickhouse02/clickhouse/access/S
    """
    Then we get ZK list with len 1
    # check Q
    When we execute ZK list query on zookeeper01
    """
    /clickhouse02/clickhouse/access/Q
    """
    Then we get ZK list with len 1
