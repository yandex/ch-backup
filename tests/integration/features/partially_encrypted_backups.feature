Feature: Support partially encrypted backups

  Background:
    Given default configuration
    And a working s3
    And a working zookeeper on zookeeper01
    And a working clickhouse on clickhouse01
    And a working clickhouse on clickhouse02

  Scenario: Restore backup with disabled encryption
    Given ch-backup configuration on clickhouse01
    """
    encryption:
      type: noop
      is_enabled: False
    """
    Given ch-backup configuration on clickhouse02
    """
    encryption:
      type: noop
      is_enabled: False
    """
    Given we have executed queries on clickhouse01
    """
    CREATE DATABASE test_db;

    CREATE TABLE test_db.table_01 ON CLUSTER 'default' (date Date, n Int32)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/test_db/table', '{replica}') PARTITION BY date ORDER BY date;
    INSERT INTO test_db.table_01 SELECT today(), number FROM system.numbers LIMIT 10;
    """
    When we create clickhouse01 clickhouse backup
    When we restore clickhouse backup #0 to clickhouse02
    Then clickhouse02 has same schema as clickhouse01
    And we got same clickhouse data at clickhouse01 clickhouse02
    And metadata of clickhouse01 backup #0 contains
    """
    is_encryption_enabled: False
    """

  Scenario: Restore backup after encryption disabled with no metadata
    Given ch-backup configuration on clickhouse01
    """
    encryption:
      type: noop
    """
    Given ch-backup configuration on clickhouse02
    """
    encryption:
      type: noop
      is_enabled: True
    """
    Given we have executed queries on clickhouse01
    """
    CREATE DATABASE test_db;

    CREATE TABLE test_db.table_01 ON CLUSTER 'default' (date Date, n Int32)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/test_db/table', '{replica}') PARTITION BY date ORDER BY date;
    INSERT INTO test_db.table_01 SELECT today(), number FROM system.numbers LIMIT 10;
    """
    When we create clickhouse01 clickhouse backup
    When metadata paths of clickhouse01 backup #0 was deleted
    """
    - databases.test_db.engine
    - databases.test_db.metadata_path
    """
    Given file "metadata/test_db/table_01.sql" in clickhouse01 backup #0 data set to
    """
    CREATE TABLE test_db.table_01 ON CLUSTER 'default' (date Date, n Int32)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/test_db/table', '{replica}') PARTITION BY date ORDER BY date
    """
    When we restore clickhouse backup #0 to clickhouse02
    Then clickhouse02 has same schema as clickhouse01
    And we got same clickhouse data at clickhouse01 clickhouse02
    And metadata of clickhouse01 backup #0 contains
    """
    is_encryption_enabled: True
    """
    When we update ch-backup configuration on clickhouse01
    """
    encryption:
      type: noop
      is_enabled: False
    """
    When we update ch-backup configuration on clickhouse02
    """
    encryption:
      type: noop
      is_enabled: False
    """
    When we restore clickhouse backup #0 to clickhouse02
    Then clickhouse02 has same schema as clickhouse01
    And we got not same clickhouse data at clickhouse01 clickhouse02
    And metadata of clickhouse01 backup #0 contains
    """
    is_encryption_enabled: True
    """

  Scenario: Restore backup after encryption disabled with valid metadata
    Given ch-backup configuration on clickhouse01
    """
    encryption:
      type: noop
    """
    Given ch-backup configuration on clickhouse02
    """
    encryption:
      type: noop
      is_enabled: True
    """
    Given we have executed queries on clickhouse01
    """
    CREATE DATABASE test_db;

    CREATE TABLE test_db.table_01 ON CLUSTER 'default' (date Date, n Int32)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/test_db/table', '{replica}') PARTITION BY date ORDER BY date;
    INSERT INTO test_db.table_01 SELECT today(), number FROM system.numbers LIMIT 10;
    """
    When we create clickhouse01 clickhouse backup
    When we update ch-backup configuration on clickhouse01
    """
    encryption:
      type: noop
      is_enabled: False
    """
    When we update ch-backup configuration on clickhouse02
    """
    encryption:
      type: noop
      is_enabled: False
    """
    When we restore clickhouse backup #0 to clickhouse02
    Then clickhouse02 has same schema as clickhouse01
    And we got same clickhouse data at clickhouse01 clickhouse02
    And metadata of clickhouse01 backup #0 contains
    """
    is_encryption_enabled: True
    """
