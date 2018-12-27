Feature: Backups with custom names

  Background:
    Given default configuration
    And a working s3
    And a working clickhouse on clickhouse01

  Scenario: Create backups with explicit names
    When we execute queries on clickhouse01
    """
    CREATE DATABASE test_db1;
    CREATE TABLE test_db1.test_table (partition_id Int32, n Int32) ENGINE = MergeTree() PARTITION BY partition_id ORDER BY (partition_id, n);
    INSERT INTO test_db1.test_table SELECT 1, number FROM system.numbers LIMIT 100;
    INSERT INTO test_db1.test_table SELECT 2, number FROM system.numbers LIMIT 100;
    """
    And we create clickhouse01 clickhouse backup
    """
    name: db1_backup_1
    databases: ["test_db1"]
    """
    And we execute queries on clickhouse01
    """
    CREATE DATABASE test_db2;
    CREATE TABLE test_db2.test_table (partition_id Int32, n Int32) ENGINE = MergeTree() PARTITION BY partition_id ORDER BY (partition_id, n);
    INSERT INTO test_db2.test_table SELECT 1, number FROM system.numbers LIMIT 100;
    INSERT INTO test_db2.test_table SELECT 2, number FROM system.numbers LIMIT 100;
    """
    And we create clickhouse01 clickhouse backup
    """
    name: db2_backup_1
    databases: ["test_db2"]
    """
    And we execute queries on clickhouse01
    """
    ALTER TABLE test_db1.test_table DROP PARTITION 1;
    INSERT INTO test_db1.test_table SELECT 3, number FROM system.numbers LIMIT 100;
    """
    And we create clickhouse01 clickhouse backup
    """
    name: db1_backup_2
    databases: ["test_db1"]
    """
    Then ch_backup entries of clickhouse01 are in proper condition
      | num | name         | state             | data_count | link_count   |
      | 0   | db1_backup_2 | created           | 1          | 1            |
      | 1   | db2_backup_1 | created           | 2          | 0            |
      | 2   | db1_backup_1 | created           | 2          | 0            |

  Scenario: Delete backup
    When we delete clickhouse01 clickhouse backup #2
    Then ch_backup entries of clickhouse01 are in proper condition
      | num | name         | state             | data_count | link_count   |
      | 0   | db1_backup_2 | created           | 1          | 1            |
      | 1   | db2_backup_1 | created           | 2          | 0            |
      | 2   | db1_backup_1 | partially_deleted | 1          | 0            |

  Scenario: Purge backup
    Given ch-backup config on clickhouse01 was merged with following
    """
    backup:
        retain_time:
            days: 0
        retain_count: 1
    """
    When we purge clickhouse01 clickhouse backups
    Then ch_backup entries of clickhouse01 are in proper condition
      | num | name         | state             | data_count | link_count   |
      | 0   | db1_backup_2 | created           | 1          | 1            |
      | 1   | db1_backup_1 | partially_deleted | 1          | 0            |

  Scenario: Restore from backup
    Given a working clickhouse on clickhouse02
    When we restore clickhouse #0 backup to clickhouse02
    And we execute queries on clickhouse01
    """
    DROP DATABASE test_db2;
    """
    Then we got same clickhouse data at clickhouse01 clickhouse02

  Scenario: Create backup with name containing {uuid} macro
    When we create clickhouse01 clickhouse backup
    """
    name: '{uuid}'
    """
    Then ch_backup entries of clickhouse01 are in proper condition
      | num | state             | data_count | link_count   |
      | 0   | created           | 1          | 1            |
      | 1   | created           | 1          | 1            |
      | 2   | partially_deleted | 1          | 0            |
