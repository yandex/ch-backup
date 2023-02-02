Feature: User defined functions support

  Background:
    Given default configuration
    And a working s3
    And a working zookeeper on zookeeper01
    And a working clickhouse on clickhouse01
    And a working clickhouse on clickhouse02

  @require_version_21.11
  Scenario: Check UDF restore
    Given we have executed queries on clickhouse01
    """
    CREATE FUNCTION test_func AS (a, b) -> a + b;
    """
    When we create clickhouse01 clickhouse backup
    When we restore clickhouse backup #0 to clickhouse02
    When we execute query on clickhouse02
    """
    SELECT name FROM system.functions WHERE origin == 'SQLUserDefined' LIMIT 1;
    """
    Then we get response
    """
    test_func
    """

  @require_version_21.11
  Scenario: Check udf restore with same udf name
    Given we have executed queries on clickhouse01
    """
    CREATE FUNCTION test_func AS (a, b) -> a + b;
    """
    Given we have executed queries on clickhouse02
    """
    CREATE FUNCTION test_func AS (a, b) -> a - b;
    """
    When we create clickhouse01 clickhouse backup
    When we restore clickhouse backup #0 to clickhouse02
    When we execute query on clickhouse02
    """
    SELECT create_query FROM system.functions WHERE origin == 'SQLUserDefined' LIMIT 1;
    """
    Then we get response
    """
    CREATE FUNCTION test_func AS (a, b) -> (a + b)
    """

  @require_version_21.11
  Scenario: Check udf restore-schema
    Given we have executed queries on clickhouse01
    """
    CREATE FUNCTION test_func AS (a, b) -> a + b;
    """
    When we create clickhouse01 clickhouse backup
    """
    backup_access_control: true
    schema_only: true
    """
    When we restore clickhouse backup #0 to clickhouse02
    """
    schema_only: true
    """
    When we execute query on clickhouse02
    """
    SELECT name FROM system.functions WHERE origin == 'SQLUserDefined' LIMIT 1;
    """
    Then we get response
    """
    test_func
    """

  @require_version_21.11
  Scenario: Check udf restore-schema with same udf name
    Given we have executed queries on clickhouse01
    """
    CREATE FUNCTION test_func AS (a, b) -> a + b;
    """
    Given we have executed queries on clickhouse02
    """
    CREATE FUNCTION test_func AS (a, b) -> a - b;
    """
    When we create clickhouse01 clickhouse backup
    """
    backup_access_control: true
    schema_only: true
    """
    When we restore clickhouse backup #0 to clickhouse02
    """
    schema_only: true
    """
    When we execute query on clickhouse02
    """
    SELECT create_query FROM system.functions WHERE origin == 'SQLUserDefined' LIMIT 1;
    """
    Then we get response
    """
    CREATE FUNCTION test_func AS (a, b) -> (a + b)
    """