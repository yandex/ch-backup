Feature: Backup and restore Replicated Database with synchronization

  Background:
    Given default configuration
    And a working s3
    And a working zookeeper on zookeeper01
    And a working clickhouse on clickhouse01
    And a working clickhouse on clickhouse02

  Scenario: Replicated Database synchronization after full restore
    Given we have enabled shared zookeeper for clickhouse01
    And we have enabled shared zookeeper for clickhouse02
    And ClickHouse settings
    """
    allow_experimental_database_replicated: 1
    """
    And we have executed queries on clickhouse01
    """
    CREATE DATABASE test_replicated_db
    ENGINE = Replicated('/clickhouse/databases/test_db', '{shard}', '{replica}');
    """
    # Create tables to test synchronization with many objects
    When we create 10 tables on clickhouse01 with schema
    """
    CREATE TABLE test_replicated_db.test_table_{table_number} (id UInt32, name String) ENGINE = MergeTree() ORDER BY id PARTITION BY id
    """
    # Insert some test data into the first table
    Given we have executed queries on clickhouse01
    """
    INSERT INTO test_replicated_db.test_table_0 VALUES (1, 'test1'), (2, 'test2');
    """
    When we create clickhouse01 clickhouse backup
    Then we got the following backups on clickhouse01
      | num | state   | data_count | link_count |
      | 0   | created | 2          | 0          |
    When we restore clickhouse backup #0 to clickhouse02
    Then we got same clickhouse data at clickhouse01 clickhouse02
    When we execute query on clickhouse02
    """
    SELECT name FROM system.databases WHERE name = 'test_replicated_db'
    """
    Then we get response
    """
    test_replicated_db
    """
    # Verify all 100 tables were synchronized
    When we execute query on clickhouse02
    """
    SELECT count() FROM system.tables WHERE database = 'test_replicated_db'
    """
    Then we get response
    """
    10
    """
    # Verify data in the first table
    When we execute query on clickhouse02
    """
    SELECT count() FROM test_replicated_db.test_table_0
    """
    Then we get response
    """
    2
    """

  @require_version_22.8
  Scenario: Replicated Database synchronization after schema-only restore
    Given we have enabled shared zookeeper for clickhouse01
    And we have enabled shared zookeeper for clickhouse02
    And ClickHouse settings
    """
    allow_experimental_database_replicated: 1
    """
    Given we have executed queries on clickhouse01
    """
    CREATE DATABASE test_replicated_db
    ENGINE = Replicated('/clickhouse/databases/test_db', '{shard}', '{replica}');
    """
    # Create tables to test synchronization with many objects
    When we create 10 tables on clickhouse01 with schema
    """
    CREATE TABLE test_replicated_db.test_table_{table_number} (id UInt32, name String) ENGINE = MergeTree() ORDER BY id PARTITION BY id
    """
    # Insert some test data into the first table
    Given we have executed queries on clickhouse01
    """
    INSERT INTO test_replicated_db.test_table_0 VALUES (1, 'test1'), (2, 'test2');
    """
    When we create clickhouse01 clickhouse backup
    Then we got the following backups on clickhouse01
      | num | state   | data_count | link_count |
      | 0   | created | 2          | 0          |
    When we restore clickhouse backup #0 to clickhouse02
    """
    schema_only: true
    """
    Then clickhouse02 has same schema as clickhouse01
    When we execute query on clickhouse02
    """
    SELECT name FROM system.databases WHERE name = 'test_replicated_db'
    """
    Then we get response
    """
    test_replicated_db
    """
    # Verify all 100 tables were synchronized (schema-only)
    When we execute query on clickhouse02
    """
    SELECT count() FROM system.tables WHERE database = 'test_replicated_db'
    """
    Then we get response
    """
    10
    """
    When we execute query on clickhouse02
    """
    SELECT count() FROM test_replicated_db.test_table_0
    """
    Then we get response
    """
    0
    """

  @require_version_22.8
  Scenario Outline: Restore with --restore-tables-in-replicated-database flag
    Given we have enabled shared zookeeper for clickhouse01
    And we have enabled shared zookeeper for clickhouse02
    Given ClickHouse settings
    """
    allow_experimental_database_replicated: 1
    """
    Given we have executed queries on clickhouse01
    """
    CREATE DATABASE test_replicated_db
    ENGINE = Replicated('/clickhouse/databases/test_db', '{shard}', '{replica}');
    
    CREATE TABLE test_replicated_db.test_table (
        id UInt32,
        name String
    ) ENGINE = MergeTree()
    ORDER BY id PARTITION BY id;
    
    INSERT INTO test_replicated_db.test_table VALUES (1, 'test1'), (2, 'test2');
    """
    When we create clickhouse01 clickhouse backup
    Then we got the following backups on clickhouse01
      | num | state   | data_count | link_count |
      | 0   | created | 2          | 0          |
    # Drop database on clickhouse01 after backup to simulate clean restore scenario
    When we execute queries on clickhouse01
    """
    DROP DATABASE test_replicated_db SYNC;
    """
    When we restore clickhouse backup #0 to clickhouse02
    """
    restore_tables_in_replicated_database: <flag_value>
    schema_only: true
    """
    # Database should always be restored
    And we execute query on clickhouse02
    """
    SELECT name FROM system.databases WHERE name = 'test_replicated_db'
    """
    Then we get response
    """
    test_replicated_db
    """
    # Table existence depends on flag value
    When we execute query on clickhouse02
    """
    SELECT count() FROM system.tables WHERE database = 'test_replicated_db' AND name = 'test_table'
    """
    Then we get response
    """
    <table_count>
    """

    Examples:
      | flag_value | table_count |
      | true       | 1           |
      | false      | 0           |

  @require_version_22.8
  Scenario Outline: Mixed databases with --restore-tables-in-replicated-database flag
    Given we have enabled shared zookeeper for clickhouse01
    And we have enabled shared zookeeper for clickhouse02
    Given ClickHouse settings
    """
    allow_experimental_database_replicated: 1
    """
    And we have executed queries on clickhouse01
    """
    CREATE DATABASE test_atomic_db ENGINE = Atomic;
    CREATE DATABASE test_replicated_db
    ENGINE = Replicated('/clickhouse/databases/test_db', '{shard}', '{replica}');
    
    CREATE TABLE test_atomic_db.test_table (
        id UInt32,
        name String
    ) ENGINE = MergeTree()
    ORDER BY id;
    
    CREATE TABLE test_replicated_db.test_table (
        id UInt32,
        name String
    ) ENGINE = MergeTree()
    ORDER BY id;   
    """
    When we create clickhouse01 clickhouse backup
    Then we got the following backups on clickhouse01
      | num | state   | data_count | link_count |
      | 0   | created | 0          | 0          |
    # Drop databases on clickhouse01 after backup to simulate clean restore scenario
    When we execute queries on clickhouse01
    """
    DROP DATABASE test_atomic_db SYNC;
    DROP DATABASE test_replicated_db SYNC;
    """
    When we restore clickhouse backup #0 to clickhouse02
    """
    restore_tables_in_replicated_database: <flag_value>
    schema_only: true
    """
    # Both databases should always be restored
    And we execute query on clickhouse02
    """
    SELECT name FROM system.databases WHERE name LIKE 'test_%' ORDER BY name
    """
    Then we get response
    """
    test_atomic_db
    test_replicated_db
    """    
    # Replicated database table existence depends on flag value
    When we execute query on clickhouse02
    """
    SELECT count() FROM system.tables WHERE database = 'test_replicated_db' AND name = 'test_table'
    """
    Then we get response
    """
    <replicated_table_count>
    """

    Examples:
      | flag_value | replicated_table_count |
      | true       | 1                      |
      | false      | 0                      |
