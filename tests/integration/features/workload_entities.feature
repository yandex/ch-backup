Feature: Workload entities (WORKLOADs and RESOURCEs) support

  Background:
    Given default configuration
    And a working s3
    And a working zookeeper on zookeeper01
    And a working clickhouse on clickhouse01
    And a working clickhouse on clickhouse02

  @require_version_24.11
  Scenario Outline: Check WORKLOAD and RESOURCE restore with hierarchy: <name>
    Given we replace config file no_storage.xml in favor of <backup_storage_config_file> on clickhouse01 with restart
    And we have executed queries on clickhouse01
    """
    CREATE RESOURCE s3_read (READ DISK s3);
    CREATE RESOURCE s3_write (WRITE DISK s3);
    CREATE WORKLOAD all SETTINGS max_bytes_per_second = 2147483648;
    CREATE WORKLOAD user IN all SETTINGS weight = 9;
    CREATE WORKLOAD system IN all;
    CREATE WORKLOAD development IN user;
    CREATE WORKLOAD production IN user SETTINGS weight = 3;
    """
    When we create clickhouse01 clickhouse backup
    When we restore clickhouse backup #0 to clickhouse02
    When we execute query on clickhouse02
    """
    SELECT count() FROM system.workloads WHERE name in ('all','user','system', 'development', 'production');
    """
    Then we get response
    """
    5
    """
    When we execute query on clickhouse02
    """
    SELECT count() FROM system.resources WHERE name in ('s3_read','s3_write');
    """
    Then we get response
    """
    2
    """
    Examples:
        | name | backup_storage_config_file |
        | from local storage | workload_entity_storage/no_storage.xml |
        | from zookeeper storage | workload_entity_storage/zookeeper.xml |

  @require_version_24.11
  Scenario: Restore when clickhouse02 already has conflicting resources
    Given we have executed queries on clickhouse01
    """
    CREATE RESOURCE s3_read (READ DISK s3);
    CREATE RESOURCE s3_write (WRITE DISK s3);
    """
    When we create clickhouse01 clickhouse backup
    And we execute queries on clickhouse02
    """
    CREATE RESOURCE s3_read (READ DISK s3);
    CREATE RESOURCE s3_write (READ DISK s3);
    """
    When we restore clickhouse backup #0 to clickhouse02
    When we execute query on clickhouse02
    """
    SELECT create_query FROM system.resources WHERE name = 's3_read';
    """
    Then we get response
    """
    CREATE RESOURCE s3_read (READ DISK s3)
    """
    When we execute query on clickhouse02
    """
    SELECT create_query FROM system.resources WHERE name = 's3_write';
    """
    Then we get response
    """
    CREATE RESOURCE s3_write (WRITE DISK s3)
    """

  @require_version_25.10
  Scenario: Skip configuration-defined workload entities during backup
    Given we replace config file no_storage.xml in favor of workload_entity_storage/resources_and_workloads.xml on clickhouse01 with restart
    When we execute query on clickhouse01
    """
    SELECT count() FROM system.workloads WHERE name IN ('xml_all', 'xml_production');
    """
    Then we get response
    """
    2
    """
    When we execute query on clickhouse01
    """
    SELECT count() FROM system.resources WHERE name = 'xml_s3_read';
    """
    Then we get response
    """
    1
    """
    When we create clickhouse01 clickhouse backup
    Then clickhouse01 backup #0 contains no workload entities
    When we create clickhouse01 clickhouse backup
    """
    schema_only: true
    """
    Then clickhouse01 backup #1 contains no workload entities
