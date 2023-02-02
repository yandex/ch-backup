Feature: Database engines

  Background:
    Given default configuration
    And a working s3
    And a working zookeeper on zookeeper01
    And a working clickhouse on clickhouse01
    And a working clickhouse on clickhouse02
    And clickhouse on clickhouse01 has test schema

  @require_version_22.7
  Scenario: Restore-schema with incorrect database engine
    Given ClickHouse settings
    """
    allow_deprecated_database_ordinary: 1
    """
    When we execute queries on clickhouse01
    """
    DROP DATABASE IF EXIST default;
    CREATE DATABASE default ENGINE=Ordinary
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
    SELECT engine FROM system.databases WHERE database='default'
    """
    Then we get response
    """
    Ordinary
    """