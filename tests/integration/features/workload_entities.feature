Feature: Workload entities (WORKLOADs and RESOURCEs) support

  Background:
    Given default configuration
    And a working s3
    And a working zookeeper on zookeeper01
    And a working clickhouse on clickhouse01
    And a working clickhouse on clickhouse02

  @require_version_24.11
  Scenario: Check RESOURCE restore
    Given we have executed queries on clickhouse01
    """
    CREATE RESOURCE test_resource (WRITE DISK disk_s3, READ DISK disk_s3);
    """
    When we create clickhouse01 clickhouse backup
    When we restore clickhouse backup #0 to clickhouse02
    When we execute query on clickhouse02
    """
    SELECT name FROM system.resources WHERE name = 'test_resource' LIMIT 1;
    """
    Then we get response
    """
    test_resource
    """

  @require_version_24.11
  Scenario: Check WORKLOAD restore
    Given we have executed queries on clickhouse01
    """
    CREATE RESOURCE test_io_write (WRITE DISK disk_s3);
    CREATE RESOURCE test_io_read (READ DISK disk_s3);
    CREATE WORKLOAD test_workload SETTINGS max_requests = 100;
    """
    When we create clickhouse01 clickhouse backup
    When we restore clickhouse backup #0 to clickhouse02
    When we execute query on clickhouse02
    """
    SELECT name FROM system.workloads WHERE name = 'test_workload' LIMIT 1;
    """
    Then we get response
    """
    test_workload
    """

  @require_version_24.11
  Scenario: Check workload entity restore with same name
    Given we have executed queries on clickhouse01
    """
    CREATE RESOURCE test_resource (WRITE DISK disk_s3);
    """
    Given we have executed queries on clickhouse02
    """
    CREATE RESOURCE test_resource (READ DISK disk_s3);
    """
    When we create clickhouse01 clickhouse backup
    When we restore clickhouse backup #0 to clickhouse02
    When we execute query on clickhouse02
    """
    SELECT create_query FROM system.resources WHERE name = 'test_resource' LIMIT 1;
    """
    Then we get response contains
    """
    WRITE DISK disk_s3
    """

  @require_version_24.11
  Scenario: Check workload entities restore-schema
    Given we have executed queries on clickhouse01
    """
    CREATE RESOURCE test_resource (WRITE DISK disk_s3);
    CREATE WORKLOAD test_workload SETTINGS max_requests = 100;
    """
    When we create clickhouse01 clickhouse backup
    """
    schema_only: true
    """
    When we restore clickhouse backup #0 to clickhouse02
    """
    schema_only: true
    """
    When we execute query on clickhouse02
    """
    SELECT count() FROM (
        SELECT name FROM system.resources WHERE name = 'test_resource'
        UNION ALL
        SELECT name FROM system.workloads WHERE name = 'test_workload'
    );
    """
    Then we get response
    """
    2
    """

  @require_version_24.11
  Scenario: Check workload entities restore-schema with same name
    Given we have executed queries on clickhouse01
    """
    CREATE RESOURCE test_resource (WRITE DISK disk_s3);
    """
    Given we have executed queries on clickhouse02
    """
    CREATE RESOURCE test_resource (READ DISK disk_s3);
    """
    When we create clickhouse01 clickhouse backup
    """
    schema_only: true
    """
    When we restore clickhouse backup #0 to clickhouse02
    """
    schema_only: true
    """
    When we execute query on clickhouse02
    """
    SELECT create_query FROM system.resources WHERE name = 'test_resource' LIMIT 1;
    """
    Then we get response contains
    """
    WRITE DISK disk_s3
    """
