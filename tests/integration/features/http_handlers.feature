Feature: Backup and restore SQL-defined HTTP handlers

  Background:
    Given default configuration
    And a working s3
    And a working zookeeper on zookeeper01
    And a working clickhouse on clickhouse01
    And a working clickhouse on clickhouse02

  @require_version_26.8
  Scenario Outline: Restore handlers across storage types: <source> to <target>
    Given we replace config file no_storage.xml in favor of http_handlers_storage/<source>.xml on clickhouse01 with restart
    And we replace config file no_storage.xml in favor of http_handlers_storage/<target>.xml on clickhouse02 with restart
    And we have executed queries on clickhouse01
    """
    CREATE HANDLER `handler/with space` URL '/backup-handler' AS SELECT 'original';
    CREATE HANDLER literal_marker URL '/backup-marker' AS SELECT '[HIDDEN]';
    CREATE HANDLER secret_handler URL '/backup-secret' AS
      SELECT * FROM s3('https://example.invalid/data.csv', 'backup-key', 'backup-secret', 'CSV', 'value UInt64');
    """
    When we create clickhouse01 clickhouse backup
    And we execute queries on clickhouse02
    """
    CREATE HANDLER `handler/with space` URL '/backup-handler' AS SELECT 'changed';
    """
    And we restore clickhouse backup #0 to clickhouse02
    Then clickhouse01 has same HTTP handlers as clickhouse02
    When we restore clickhouse backup #0 to clickhouse02
    Then clickhouse01 has same HTTP handlers as clickhouse02

    Examples:
      | source    | target    |
      | local     | local     |
      | local     | zookeeper |
      | zookeeper | local     |
      | zookeeper | zookeeper |

  @require_version_26.8
  Scenario: Select only HTTP handlers for backup and restore
    Given we have executed queries on clickhouse01
    """
    CREATE DATABASE handler_test;
    CREATE HANDLER test_handler URL '/backup-only-handler' AS SELECT 42;
    """
    When we create clickhouse01 clickhouse backup
    """
    http_handlers: True
    """
    And we restore clickhouse backup #0 to clickhouse02
    """
    http_handlers: True
    """
    Then clickhouse01 has same HTTP handlers as clickhouse02
    When we execute query on clickhouse02
    """
    SELECT count() FROM system.databases WHERE name = 'handler_test'
    """
    Then we get response
    """
    0
    """
