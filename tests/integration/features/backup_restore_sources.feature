Feature: Backup & Restore sources

  Background:
    Given default configuration
    And a working s3
    And a working zookeeper on zookeeper01
    And a working clickhouse on clickhouse01
    And clickhouse on clickhouse01 has test schema
    And clickhouse01 has test clickhouse data test1
    And we have executed queries on clickhouse01
    """
    CREATE USER test_user IDENTIFIED WITH plaintext_password BY 'password';
    CREATE ROLE test_role;
    """

  Scenario: Test backup sources
    # 1) full backup
    When we create clickhouse01 clickhouse backup
    Then we got the following backups on clickhouse01
      | num | state   | data_count | acl_count | udf_count | schema_only |
      | 0   | created | 4          | 2         | 0         | False       |
    # 2) only access
    When we create clickhouse01 clickhouse backup
    """
    access: True
    """
    Then we got the following backups on clickhouse01
      | num | state   | data_count | acl_count | udf_count | schema_only |
      | 0   | created | 0          | 2         | 0         | False       |
      | 1   | created | 4          | 2         | 0         | False       |
    # 3) old schema-only
    When we create clickhouse01 clickhouse backup
    """
    schema_only: True
    """
    Then we got the following backups on clickhouse01
      | num | state   | data_count | acl_count | udf_count | schema_only |
      | 0   | created | 0          | 2         | 0         | True        |
      | 1   | created | 0          | 2         | 0         | False       |
      | 2   | created | 4          | 2         | 0         | False       |
    # 4) new schema only
    When we create clickhouse01 clickhouse backup
    """
    schema: True
    """
    Then we got the following backups on clickhouse01
      | num | state   | data_count | acl_count | udf_count | schema_only |
      | 0   | created | 0          | 0         | 0         | True        |
      | 1   | created | 0          | 2         | 0         | True        |
      | 2   | created | 0          | 2         | 0         | False       |
      | 3   | created | 4          | 2         | 0         | False       |
    # 5) access & data
    When we create clickhouse01 clickhouse backup
    """
    access: True
    data: True
    """
    Then we got the following backups on clickhouse01
      | num | state   | data_count | link_count | acl_count | udf_count | schema_only |
      | 0   | created | 0          | 4          | 2         | 0         | False       |
      | 1   | created | 0          | 0          | 0         | 0         | True        |
      | 2   | created | 0          | 0          | 2         | 0         | True        |
      | 3   | created | 0          | 0          | 2         | 0         | False       |
      | 4   | created | 4          | 0          | 2         | 0         | False       |

  @require_version_21.11
  Scenario: Test backup sources for UDF
    When we execute queries on clickhouse01
    """
    CREATE FUNCTION test_func AS (a, b) -> a + b;
    """
    # 1) full backup
    And we create clickhouse01 clickhouse backup
    Then we got the following backups on clickhouse01
      | num | state   | data_count | acl_count | udf_count | schema_only |
      | 0   | created | 4          | 2         | 1         | False       |
    # 2) only udf
    When we create clickhouse01 clickhouse backup
    """
    udf: True
    """
    Then we got the following backups on clickhouse01
      | num | state   | data_count | acl_count | udf_count | schema_only |
      | 0   | created | 0          | 0         | 1         | False       |
      | 1   | created | 4          | 2         | 1         | False       |
    # 3) udf & access
    When we create clickhouse01 clickhouse backup
    """
    udf: True
    access: True
    """
    Then we got the following backups on clickhouse01
      | num | state   | data_count | acl_count | udf_count | schema_only |
      | 0   | created | 0          | 2         | 1         | False       |
      | 1   | created | 0          | 0         | 1         | False       |
      | 2   | created | 4          | 2         | 1         | False       |

  Scenario: Test restore sources
    When we create clickhouse01 clickhouse backup
    Then we got the following backups on clickhouse01
      | num | state   | data_count | acl_count | udf_count | schema_only |
      | 0   | created | 4          | 2         | 0         | False       |
    # 1) full restore, but access will be missed
    When we restore clickhouse backup #0 to clickhouse02
    And we restart clickhouse on clickhouse02
    Given a working clickhouse on clickhouse02
    Then we got same clickhouse data at clickhouse01 clickhouse02
    When we execute query on clickhouse02
    """
    SELECT count() FROM system.roles WHERE name = 'test_role'
    """
    Then we get response
    """
    0
    """
    When we execute query on clickhouse02
    """
    SELECT count() FROM system.users WHERE name = 'test_user'
    """
    Then we get response
    """
    0
    """
    # 2) restore only access entities (current analog of restore-access-control command)
    When we drop all data at clickhouse02
    And we restore clickhouse backup #0 to clickhouse02
    """
    access: True
    """
    And we restart clickhouse on clickhouse02
    Given a working clickhouse on clickhouse02
    Then clickhouse01 has same access control objects as clickhouse02
    But on clickhouse02 tables are empty

  @require_version_21.11
  Scenario: Test restore sources for UDF
    When we execute queries on clickhouse01
    """
    CREATE FUNCTION test_func AS (a, b) -> a + b;
    """
    And we create clickhouse01 clickhouse backup
    Then we got the following backups on clickhouse01
      | num | state   | data_count | acl_count | udf_count | schema_only |
      | 0   | created | 4          | 2         | 1         | False       |
    # 1) full restore, but access will be missed
    When we restore clickhouse backup #0 to clickhouse02
    And we restart clickhouse on clickhouse02
    Given a working clickhouse on clickhouse02
    Then we got same clickhouse data at clickhouse01 clickhouse02
    When we execute query on clickhouse02
    """
    SELECT count() FROM system.roles WHERE name = 'test_role'
    """
    Then we get response
    """
    0
    """
    When we execute query on clickhouse02
    """
    SELECT count() FROM system.users WHERE name = 'test_user'
    """
    Then we get response
    """
    0
    """
    When we execute query on clickhouse02
    """
    SELECT count() FROM system.functions WHERE name = 'test_func'
    """
    Then we get response
    """
    1
    """
    # 2) restore only UDF
    When we drop all data at clickhouse02
    And we restore clickhouse backup #0 to clickhouse02
    """
    udf: True
    """
    And we restart clickhouse on clickhouse02
    Given a working clickhouse on clickhouse02
    Then on clickhouse02 tables are empty
    When we execute query on clickhouse02
    """
    SELECT count() FROM system.functions WHERE name = 'test_func'
    """
    Then we get response
    """
    1
    """
    When we execute query on clickhouse02
    """
    SELECT count() FROM system.roles WHERE name = 'test_role'
    """
    Then we get response
    """
    0
    """
    When we execute query on clickhouse02
    """
    SELECT count() FROM system.users WHERE name = 'test_user'
    """
    Then we get response
    """
    0
    """
    # 3) restore access & UDF
    When we drop all data at clickhouse02
    And we restore clickhouse backup #0 to clickhouse02
    """
    access: True
    udf: True
    """
    And we restart clickhouse on clickhouse02
    Given a working clickhouse on clickhouse02
    Then on clickhouse02 tables are empty
    When we execute query on clickhouse02
    """
    SELECT count() FROM system.functions WHERE name = 'test_func'
    """
    Then we get response
    """
    1
    """
    When we execute query on clickhouse02
    """
    SELECT count() FROM system.roles WHERE name = 'test_role'
    """
    Then we get response
    """
    1
    """
    When we execute query on clickhouse02
    """
    SELECT count() FROM system.users WHERE name = 'test_user'
    """
    Then we get response
    """
    1
    """

Scenario: Restore with regular sync of restore context
    When we drop all databases at clickhouse01
    And we drop all databases at clickhouse02
    Given ch-backup configuration on clickhouse02
    """
      backup:
        restore_context_sync_on_disk_operation_threshold: 1
    """
    Given we have executed queries on clickhouse01
    """
    CREATE DATABASE test_db;
    CREATE TABLE test_db.table_01 (n Int32) ENGINE = MergeTree() PARTITION BY n%100 ORDER BY n;
    INSERT INTO test_db.table_01 SELECT number FROM system.numbers LIMIT 10000000;
    """
    When we create clickhouse01 clickhouse backup
    And we restore clickhouse backup #0 to clickhouse02
    Then we got same clickhouse data at clickhouse01 clickhouse02

