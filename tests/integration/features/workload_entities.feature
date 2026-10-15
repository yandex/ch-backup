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
    CREATE WORKLOAD all SETTINGS max_bytes_per_second = 1024 FOR s3_write;
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
    When we execute query on clickhouse02
    """
    SELECT create_query = formatQuerySingleLine('CREATE WORKLOAD all SETTINGS max_bytes_per_second = 1024 FOR s3_write')
    FROM system.workloads WHERE name = 'all';
    """
    Then we get response
    """
    1
    """

  @require_version_24.11
  Scenario Outline: Restore existing workload hierarchy in <storage> storage
    Given ch-backup configuration on clickhouse01
    """
    encryption:
      type: noop
    """
    And ch-backup configuration on clickhouse02
    """
    encryption:
      type: noop
    """
    And we replace config file no_storage.xml in favor of <config> on clickhouse02 with restart
    And we have executed queries on clickhouse01
    """
    CREATE RESOURCE s3_read (READ DISK s3);
    CREATE WORKLOAD all SETTINGS max_bytes_per_second = 2048 FOR s3_read;
    CREATE WORKLOAD user IN all SETTINGS weight = 3;
    """
    And we have executed queries on clickhouse02
    """
    CREATE RESOURCE s3_read (READ DISK s3);
    CREATE WORKLOAD all SETTINGS max_bytes_per_second = 1024 FOR s3_read;
    CREATE WORKLOAD user IN all SETTINGS weight = 1;
    CREATE WORKLOAD extra IN user;
    """
    When we create clickhouse01 clickhouse backup
    And we restore clickhouse backup #0 to clickhouse02
    Given file "workload_entities/s3_read.sql" in clickhouse01 backup #0 data set to
    """
    create resource `s3_read`
    ( READ DISK s3 );
    """
    And file "workload_entities/all.sql" in clickhouse01 backup #0 data set to
    """
    create workload all
    settings max_bytes_per_second = 2048 for `s3_read`;
    """
    And ch-backup configuration on clickhouse02
    """
    clickhouse:
      settings:
        log_queries: 1
        log_comment: workload_repeat_restore
    """
    # Repeat to verify restoring the same backup is idempotent.
    When we restore clickhouse backup #0 to clickhouse02
    And we execute query on clickhouse02
    """
    SYSTEM FLUSH LOGS;
    """
    And we execute query on clickhouse02
    """
    SELECT count() > 0 AND countIf(match(query, '(?i)^\\s*(CREATE|DROP)\\s+(OR\\s+REPLACE\\s+)?(WORKLOAD|RESOURCE)\\b')) = 0
    FROM system.query_log
    WHERE type = 'QueryFinish' AND log_comment = 'workload_repeat_restore';
    """
    Then we get response
    """
    1
    """
    When we execute query on clickhouse02
    """
    SELECT create_query FROM system.workloads ORDER BY name;
    """
    Then we get response
    """
    CREATE WORKLOAD `all` SETTINGS max_bytes_per_second = 2048 FOR s3_read
    CREATE WORKLOAD extra IN user
    CREATE WORKLOAD user IN `all` SETTINGS weight = 3
    """
    Examples:
        | storage   | config |
        | local     | workload_entity_storage/no_storage.xml |
        | zookeeper | workload_entity_storage/zookeeper.xml |

  @require_version_25.10
  Scenario: Restore a SQL workload with a configuration-defined parent
    Given we replace config file no_storage.xml in favor of workload_entity_storage/resources_and_workloads.xml on clickhouse01 with restart
    And we replace config file no_storage.xml in favor of workload_entity_storage/resources_and_workloads.xml on clickhouse02 with restart
    And we have executed queries on clickhouse01
    """
    CREATE WORKLOAD child IN xml_all SETTINGS weight = 2;
    """
    When we create clickhouse01 clickhouse backup
    And we restore clickhouse backup #0 to clickhouse02
    And we execute query on clickhouse02
    """
    SELECT create_query FROM system.workloads ORDER BY name;
    """
    Then we get response
    """
    CREATE WORKLOAD child IN xml_all SETTINGS weight = 2
    CREATE WORKLOAD xml_all
    CREATE WORKLOAD xml_production IN xml_all SETTINGS weight = 3
    """

  @require_version_25.10
  Scenario: Restore skips identical configuration-defined entities
    Given we replace config file no_storage.xml in favor of workload_entity_storage/resources_and_workloads.xml on clickhouse02 with restart
    And we have executed queries on clickhouse01
    """
    CREATE RESOURCE xml_s3_read (READ DISK s3);
    CREATE WORKLOAD xml_all;
    CREATE WORKLOAD xml_production IN xml_all SETTINGS weight = 3;
    """
    When we create clickhouse01 clickhouse backup
    And we restore clickhouse backup #0 to clickhouse02
    And we execute query on clickhouse02
    """
    SELECT create_query FROM system.workloads ORDER BY name;
    """
    Then we get response
    """
    CREATE WORKLOAD xml_all
    CREATE WORKLOAD xml_production IN xml_all SETTINGS weight = 3
    """

  @require_version_25.10
  Scenario: Restore reports a configuration conflict without removing dependents
    Given we replace config file no_storage.xml in favor of workload_entity_storage/resources_and_workloads.xml on clickhouse02 with restart
    And we have executed queries on clickhouse01
    """
    CREATE WORKLOAD xml_all;
    CREATE WORKLOAD xml_production IN xml_all SETTINGS weight = 4;
    """
    And we have executed queries on clickhouse02
    """
    CREATE WORKLOAD extra IN xml_production;
    """
    When we create clickhouse01 clickhouse backup
    And we try to execute command on clickhouse02
    """
    ch-backup -c /etc/yandex/ch-backup/ch-backup.conf restore LAST
    """
    Then command exits with code 1
    And we get response contains
    """
    Cannot restore workload entity 'xml_production':
    """
    When we execute query on clickhouse02
    """
    SELECT create_query FROM system.workloads ORDER BY name;
    """
    Then we get response
    """
    CREATE WORKLOAD extra IN xml_production
    CREATE WORKLOAD xml_all
    CREATE WORKLOAD xml_production IN xml_all SETTINGS weight = 3
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
