Feature: Backup users, roles, etc. created by SQL

  Background:
    Given default configuration
    And a working s3
    And a working zookeeper on zookeeper01
    And a working clickhouse on clickhouse01
    And a working clickhouse on clickhouse02

  Scenario: Backup user, role with grants and row_policy
    Given we have executed queries on clickhouse01
    """
    CREATE USER test_user IDENTIFIED WITH plaintext_password BY 'password';
    CREATE ROLE test_role;
    GRANT SELECT ON test_db.* TO test_role;
    CREATE ROW POLICY filter ON test_db.table_01 FOR SELECT USING CounterID < 5 TO test_role;
    CREATE ROW POLICY filter2 ON test_db.table_01 FOR SELECT USING CounterID > 2 TO test_role, test_user;
    CREATE QUOTA test_quota FOR INTERVAL 1 DAY MAX QUERIES 10 TO test_role;
    CREATE SETTINGS PROFILE memory_profile SETTINGS max_memory_usage = 100000001 MIN 90000000 MAX 110000000 TO test_role;
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
    INSERT INTO test_db.table_01 SELECT now(), number, rand() FROM system.numbers LIMIT 1000
    """
    When we create clickhouse01 clickhouse backup
    """
    backup_access_control: True
    """
    Then we got the following backups on clickhouse01
      | num | state   | data_count | link_count |
      | 0   | created | 1          | 0          |
    When we restore clickhouse access control metadata backup #0 to clickhouse02 with restart
    Given a working clickhouse on clickhouse02
    When we restore clickhouse backup #0 to clickhouse02
    Then clickhouse01 has same access control objects as clickhouse02
    Then we got same clickhouse data at clickhouse01 clickhouse02
    """
    user: test_user
    password: password
    """
