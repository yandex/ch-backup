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
    Given ch-backup configuration on clickhouse02
    """
    clickhouse:
      settings:
        log_queries: 1
    """
    When we execute queries on clickhouse02
    """
    SYSTEM FLUSH LOGS;
    TRUNCATE TABLE system.query_log;
    """
    When we restore clickhouse backup #0 to clickhouse02
    Then clickhouse01 has same HTTP handlers as clickhouse02
    When we execute queries on clickhouse02
    """
    SYSTEM FLUSH LOGS;
    """
    When we execute query on clickhouse02
    """
    SELECT count() FROM system.query_log
    WHERE type = 'QueryFinish'
      AND (query LIKE 'CREATE HANDLER%' OR query LIKE 'DROP HANDLER%')
    """
    Then we get response
    """
    0
    """

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

  @require_version_26.8
  Scenario: Reject masked credentials during handler export
    Given we have executed queries on clickhouse01
    """
    CREATE HANDLER secret_handler URL '/backup-secret' AS
      SELECT * FROM s3('https://example.invalid/data.csv', 'backup-key', 'backup-secret', 'CSV', 'value UInt64');
    """
    When we try to execute command on clickhouse01
    """
    ch-backup --config /etc/yandex/ch-backup/ch-backup.conf backup --http-handlers --name masked
    """
    Then command exits with code 1
    And we get response contains
    """
    hidden secrets
    """

  @require_version_26.8
  Scenario: Reject literal markers even when secrets are visible
    Given we replace config file no_storage.xml in favor of http_handlers_storage/local.xml on clickhouse01 with restart
    And we have executed queries on clickhouse01
    """
    CREATE HANDLER literal_marker URL '/backup-marker' AS SELECT '[HIDDEN]';
    """
    When we try to execute command on clickhouse01
    """
    ch-backup --config /etc/yandex/ch-backup/ch-backup.conf backup --http-handlers --name literal
    """
    Then command exits with code 1
    And we get response contains
    """
    Literal [HIDDEN] markers are also unsupported
    """
