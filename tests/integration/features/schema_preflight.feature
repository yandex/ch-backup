@require_version_22.7
Feature: Validate restore schemas before deleting existing data

  Background:
    Given default configuration
    And a working s3
    And a working zookeeper on zookeeper01
    And a working clickhouse on clickhouse01
    And a working clickhouse on clickhouse02

  Scenario Outline: Invalid table schema preserves an existing database during preflight
    Given ClickHouse settings
    """
    allow_deprecated_database_ordinary: 1
    """
    And we have executed queries on clickhouse01
    """
    CREATE DATABASE preflight_db ENGINE = Atomic;
    CREATE TABLE preflight_db.t (id UInt64) ENGINE = <engine>;
    """
    And we have created clickhouse01 clickhouse backup
    """
    name: preflight_backup
    """
    And the schema in clickhouse01 backup "preflight_backup" is changed with
    """
    database: preflight_db
    table: t
    old: UInt64
    new: MissingPreflightType
    """
    And we have executed queries on clickhouse02
    """
    CREATE DATABASE preflight_db ENGINE = Ordinary;
    CREATE TABLE preflight_db.t (id UInt64) ENGINE = MergeTree ORDER BY id;
    INSERT INTO preflight_db.t VALUES (99);
    """
    When we try to execute command on clickhouse02
    """
    ch-backup -c /etc/yandex/ch-backup/ch-backup.conf restore <args> preflight_backup
    """
    Then command exits with code 1
    And we get response contains
    """
    Schema preflight failed
    """
    When we execute query on clickhouse02
    """
    SELECT engine, (SELECT id FROM preflight_db.t), (SELECT count() FROM system.databases WHERE startsWith(name, '_ch_backup_preflight_')) FROM system.databases WHERE name = 'preflight_db'
    """
    Then we get response
    """
    Ordinary	99	0
    """
    Examples:
      | engine                | args                              |
      | MergeTree ORDER BY id |                                   |
      | MergeTree ORDER BY id | --keep-going --skip-cloud-storage  |
      | Memory                | --keep-going                      |
      | Memory                | --schema-only                     |

  Scenario: Preflight isolates UUIDs and dependencies of local schemas
    Given we have executed queries on clickhouse01
    """
    CREATE DATABASE preflight_db;
    CREATE DATABASE preflight_other;
    CREATE TABLE preflight_db.src (id UInt64) ENGINE = MergeTree ORDER BY id;
    CREATE TABLE preflight_db.target (id UInt64) ENGINE = MergeTree ORDER BY id;
    CREATE TABLE preflight_db.replicated (id UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/preflight_replicated', '{replica}') ORDER BY id;
    CREATE TABLE preflight_db.log (id UInt64) ENGINE = Log;
    CREATE TABLE preflight_db.memory (id UInt64) ENGINE = Memory;
    CREATE MATERIALIZED VIEW preflight_db.mv TO preflight_db.target AS SELECT id FROM preflight_db.src;
    CREATE MATERIALIZED VIEW preflight_db.own ENGINE = MergeTree ORDER BY id AS SELECT id FROM preflight_db.src;
    CREATE VIEW preflight_other.v AS SELECT id FROM preflight_db.src;
    CREATE VIEW preflight_db.v2 AS SELECT id FROM preflight_other.v;
    CREATE DICTIONARY preflight_db.d (id UInt64) PRIMARY KEY id SOURCE(CLICKHOUSE(HOST 'localhost' PORT 9000 USER 'default' DB 'preflight_db' TABLE 'src')) LAYOUT(HASHED()) LIFETIME(0);
    INSERT INTO preflight_db.src VALUES (42);
    INSERT INTO preflight_db.replicated VALUES (43);
    """
    And we have created clickhouse01 clickhouse backup
    """
    name: preflight_views
    """
    When we restore clickhouse backup #0 to clickhouse02
    And we execute query on clickhouse02
    """
    SELECT (SELECT id FROM preflight_db.v2), (SELECT id FROM preflight_db.target), (SELECT id FROM preflight_db.own), (SELECT id FROM preflight_db.replicated), (SELECT count() FROM system.databases WHERE startsWith(name, '_ch_backup_preflight_'))
    """
    Then we get response
    """
    42	42	42	43	0
    """
    Given the schema in clickhouse01 backup "preflight_views" is changed with
    """
    database: preflight_db
    table: memory
    old: UInt64
    new: MissingPreflightType
    """
    When we try to execute command on clickhouse02
    """
    ch-backup -c /etc/yandex/ch-backup/ch-backup.conf restore --keep-going preflight_views
    """
    Then command exits with code 1
    And we get response contains
    """
    Schema preflight failed: preflight_db.memory:
    """
    When we execute query on clickhouse02
    """
    SELECT (SELECT id FROM preflight_db.v2), (SELECT id FROM preflight_db.target), (SELECT id FROM preflight_db.own), (SELECT id FROM preflight_db.replicated), (SELECT count() FROM system.databases WHERE startsWith(name, '_ch_backup_preflight_'))
    """
    Then we get response
    """
    42	42	42	43	0
    """
    When we execute queries on clickhouse02
    """
    INSERT INTO preflight_db.src VALUES (44);
    """
    And we execute query on clickhouse02
    """
    SELECT (SELECT count() FROM preflight_db.target), (SELECT count() FROM preflight_db.own)
    """
    Then we get response
    """
    2	2
    """
