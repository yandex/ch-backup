Feature: Backup & Clean & Restore

  Background:
    Given default configuration
    And a working s3
    And a working zookeeper on zookeeper01
    And a working clickhouse on clickhouse01
    And clickhouse on clickhouse01 has test schema

  Scenario Outline: Download backup metadata
    When we execute queries on clickhouse01
    """
    CREATE DATABASE IF NOT EXISTS test_db;
    CREATE TABLE test_db.table_01 (
        CounterID UInt32,
        UserID UInt32
    )
    ENGINE = MergeTree()
    ORDER BY UserID
    SETTINGS storage_policy = 's3';
    INSERT INTO test_db.table_01 VALUES(0, 42), (1, 777);
    """
    And we create clickhouse01 clickhouse backup
    """
    name: test_backup
    """
    And we execute command on clickhouse01
    """
    ls /var/lib/clickhouse/disks/s3/shadow/
    """
    Then we get response
    """
    test_backup
    """
    When we execute command on clickhouse01
    """
    rm -r /var/lib/clickhouse/disks/s3/shadow/test_backup
    """
    And we execute command on clickhouse01
    """
    ch-backup -c /etc/yandex/ch-backup/ch-backup.conf get-cloud-storage-metadata --disk s3 <name>
    """
    And we execute command on clickhouse01
    """
    ls /var/lib/clickhouse/disks/s3/shadow/
    """
    Then we get response
    """
    test_backup
    """
    When we execute command on clickhouse01
    """
    ch-backup -c /etc/yandex/ch-backup/ch-backup.conf get-cloud-storage-metadata --disk s3 <name>
    """
    Then we get response contains
    """
    is already present
    """
    Examples:
      | name              |
      | test_backup       |
      | LAST              |

  Scenario: Download backup metadata to local path
    When we execute queries on clickhouse01
    """
    CREATE DATABASE IF NOT EXISTS test_db;
    CREATE TABLE test_db.table_01 (
        CounterID UInt32,
        UserID UInt32
    )
    ENGINE = MergeTree()
    ORDER BY UserID
    SETTINGS storage_policy = 's3';
    INSERT INTO test_db.table_01 VALUES(0, 42), (1, 777);
    """
    And we create clickhouse01 clickhouse backup
    """
    name: test_backup
    """
    And we execute command on clickhouse01
    """
    ls /var/lib/clickhouse/disks/s3/shadow/
    """
    Then we get response
    """
    test_backup
    """
    When we execute command on clickhouse01
    """
    rm -r /var/lib/clickhouse/disks/s3/shadow/test_backup
    """
    And we execute command on clickhouse01
    """
    ch-backup -c /etc/yandex/ch-backup/ch-backup.conf get-cloud-storage-metadata --local-path /tmp/cloud-storage-metadata-pipe --disk s3 LAST
    """
    And we execute command on clickhouse01
    """
    ls /tmp/cloud-storage-metadata-pipe
    """
    Then we get response
    """
    /tmp/cloud-storage-metadata-pipe
    """

  @require_version_22.6
  @require_version_less_than_23.4
  Scenario: All backup data is deleted including data of removed tables
    Given ch-backup configuration on clickhouse01
    """
    backup:
        deduplicate_parts: True
    """
    And clickhouse01 has test clickhouse data test_deleted_table
    When we execute queries on clickhouse01
    """
    CREATE DATABASE IF NOT EXISTS test_db;
    CREATE TABLE test_db.deleted_table (
        CounterID UInt32,
        UserID UInt32
    )
    ENGINE = MergeTree()
    ORDER BY UserID
    SETTINGS storage_policy = 's3';
    INSERT INTO test_db.deleted_table VALUES(0, 42), (1, 777);
    """
    And we create clickhouse01 clickhouse backup
    """
    name: 12345678T123456
    """
    Then we got the following backups on clickhouse01
      | num | state   | data_count | link_count | title   |
      | 0   | created | 5          | 0          | deleted |
    And s3 contains 11 objects
    And s3 bucket cloud-storage-01 contains 8 objects
    And we got the following s3 backup directories on clickhouse01
    """
    ["12345678T123456"]
    """
    When we execute query on clickhouse01
    """
    DROP TABLE test_db.deleted_table;
    """
    And we delete clickhouse01 clickhouse backup #0
    Then we got the following backups on clickhouse01
      | num | state | data_count | link_count | title |
    And s3 contains 0 objects
    And s3 bucket cloud-storage-01 contains 0 objects
    And we got the following s3 backup directories on clickhouse01
    """
    []
    """

  @require_version_23.4
  @require_version_less_than_23.7
  Scenario: All backup data is deleted including data of removed tables
    Given ch-backup configuration on clickhouse01
    """
    backup:
        deduplicate_parts: True
    """
    And clickhouse01 has test clickhouse data test_deleted_table
    When we execute queries on clickhouse01
    """
    CREATE DATABASE IF NOT EXISTS test_db;
    CREATE TABLE test_db.deleted_table (
        CounterID UInt32,
        UserID UInt32
    )
    ENGINE = MergeTree()
    ORDER BY UserID
    SETTINGS storage_policy = 's3';
    INSERT INTO test_db.deleted_table VALUES(0, 42), (1, 777);
    """
    And we create clickhouse01 clickhouse backup
    """
    name: 12345678T123456
    """
    Then we got the following backups on clickhouse01
      | num | state   | data_count | link_count | title   |
      | 0   | created | 5          | 0          | deleted |
    And s3 contains 11 objects
    And s3 bucket cloud-storage-01 contains 9 objects
    And we got the following s3 backup directories on clickhouse01
    """
    ["12345678T123456"]
    """
    When we execute query on clickhouse01
    """
    DROP TABLE test_db.deleted_table;
    """
    And we delete clickhouse01 clickhouse backup #0
    Then we got the following backups on clickhouse01
      | num | state | data_count | link_count | title |
    And s3 contains 0 objects
    And s3 bucket cloud-storage-01 contains 0 objects
    And we got the following s3 backup directories on clickhouse01
    """
    []
    """

  Scenario Outline: All backup data is deleted including data of removed tables
    Given ch-backup configuration on clickhouse01
    """
    backup:
        deduplicate_parts: True
    """
    And clickhouse01 has test clickhouse data test_deleted_table
    When we execute queries on clickhouse01
    """
    CREATE DATABASE IF NOT EXISTS test_db;
    CREATE TABLE test_db.deleted_table (
        CounterID UInt32,
        UserID UInt32
    )
    ENGINE = MergeTree()
    ORDER BY UserID
    SETTINGS storage_policy = 's3';
    INSERT INTO test_db.deleted_table VALUES(0, 42), (1, 777);
    """
    And we create clickhouse01 clickhouse backup
    """
    name: 12345678T123456
    """
    Then we got the following backups on clickhouse01
      | num | state   | data_count | link_count | title   |
      | 0   | created | 5          | 0          | deleted |
    And s3 contains 11 objects
    And s3 bucket cloud-storage-01 contains <object_count> objects
    And we got the following s3 backup directories on clickhouse01
    """
    ["12345678T123456"]
    """
    When we execute query on clickhouse01
    """
    DROP TABLE test_db.deleted_table;
    """
    And we delete clickhouse01 clickhouse backup #0
    Then we got the following backups on clickhouse01
      | num | state | data_count | link_count | title |
    And s3 contains 0 objects
    And s3 bucket cloud-storage-01 contains 0 objects
    And we got the following s3 backup directories on clickhouse01
    """
    []
    """
    @require_version_23.7
    @require_version_less_than_25.8
    Examples:
    | object_count |
    | 10           |

    @require_version_25.8
    @require_version_less_than_26.8
    Examples:
    | object_count |
    | 11           |

    @require_version_26.8
    Examples:
    | object_count |
    | 12           |

  Scenario: Restore succeeds when target node has no storage_configuration section
    # Regression test for: ClickHouseTemporaryDisks.__enter__ raised KeyError when
    # storage_configuration was absent from the ClickHouse config.
    # The backup here uses only the default disk (no cloud_storage disks),
    # so __enter__ must complete without error even on a node with no
    # storage_configuration section.
    Given a working clickhouse on clickhouse02
    When we execute queries on clickhouse01
    """
    CREATE DATABASE IF NOT EXISTS test_db;
    CREATE TABLE test_db.table_no_s3 (
        CounterID UInt32,
        UserID    UInt32
    )
    ENGINE = MergeTree()
    ORDER BY UserID;
    INSERT INTO test_db.table_no_s3 VALUES (0, 1), (1, 2);
    """
    And we create clickhouse01 clickhouse backup
    Then we got the following backups on clickhouse01
      | num | state   | data_count | link_count |
      | 0   | created | 1          | 0          |
    When we replace config file storage_configuration.xml in favor of no_storage_configuration.xml on clickhouse02 with restart
    And we restore clickhouse backup #0 to clickhouse02
    Then we got same clickhouse data at clickhouse01 clickhouse02

  @require_version_22.8
  Scenario: Restore with cloud_storage disks fails with a clear error when disk is absent from target config
    # Regression test for: _create_temporary_disk raised an opaque KeyError instead of
    # ClickHouseDisksException when the disk referenced in backup cloud_storage metadata
    # was missing from the target node's storage_configuration.
    # We use storage_configuration_without_s3.xml on clickhouse02: it keeps the s3/s3_cold
    # storage policies (so the table schema can be restored) but removes the actual 's3'
    # disk entry, which triggers ClickHouseDisksException in __enter__.
    Given a working clickhouse on clickhouse02
    When we execute queries on clickhouse01
    """
    CREATE DATABASE IF NOT EXISTS test_db;
    CREATE TABLE test_db.table_s3 (
        CounterID UInt32,
        UserID    UInt32
    )
    ENGINE = MergeTree()
    ORDER BY UserID
    SETTINGS storage_policy = 's3';
    INSERT INTO test_db.table_s3 VALUES (0, 1), (1, 2);
    """
    And we create clickhouse01 clickhouse backup
    Then we got the following backups on clickhouse01
      | num | state   | data_count | link_count |
      | 0   | created | 1          | 0          |
    When we replace config file storage_configuration.xml in favor of storage_configuration_without_s3.xml on clickhouse02 with restart
    And we try to execute command on clickhouse02
    """
    ch-backup -c /etc/yandex/ch-backup/ch-backup.conf restore LAST \
        --cloud-storage-source-bucket cloud-storage-01 \
        --cloud-storage-source-path data
    """
    Then we get response contains
    """
    missing from ClickHouse storage_configuration
    """

  Scenario Outline: Restore warm parts and dependent objects without the source policy
    Given a working clickhouse on clickhouse02
    # Future timestamps keep TTL inactive. Move only one source partition to S3,
    # leaving both rows local in the materialized view's own table.
    When we execute queries on clickhouse01
    """
    CREATE DATABASE storage_restore;
    CREATE TABLE storage_restore.t (d DateTime, p UInt32, id UInt64)
    ENGINE = <engine>
    PARTITION BY p ORDER BY id
    TTL d + INTERVAL 1 DAY TO VOLUME 'external', d + INTERVAL 100 DAY DELETE
    SETTINGS storage_policy = 's3_cold';
    CREATE MATERIALIZED VIEW storage_restore.mv ENGINE = MergeTree ORDER BY id
    TTL d + INTERVAL 1 DAY TO VOLUME 'external', d + INTERVAL 100 DAY DELETE
    SETTINGS storage_policy = 's3_cold' AS SELECT d, id FROM storage_restore.t;
    INSERT INTO storage_restore.t VALUES ('2099-01-01', 0, 10);
    INSERT INTO storage_restore.t VALUES ('2099-01-01', 1, 20);
    ALTER TABLE storage_restore.t MOVE PARTITION 1 TO VOLUME 'external';
    """
    And we create clickhouse01 clickhouse backup
    And we replace config file storage_configuration.xml in favor of no_storage_configuration.xml on clickhouse02 with restart
    And we restore clickhouse backup #0 to clickhouse02
    """
    skip_cloud_storage: true
    keep_going: <keep_going>
    """
    And we execute query on clickhouse02
    """
    SELECT id FROM storage_restore.t ORDER BY id
    """
    Then we get response
    """
    10
    """
    When we execute query on clickhouse02
    """
    SELECT count() FROM storage_restore.mv
    """
    Then we get response
    """
    2
    """
    When we execute query on clickhouse02
    """
    SELECT storage_policy, position(create_table_query, 'TO VOLUME') = 0, position(create_table_query, 'toIntervalDay(100)') > 0 FROM system.tables WHERE database = 'storage_restore' AND name = 't'
    """
    Then we get response
    """
    default	1	1
    """
    When we restore clickhouse backup #0 to clickhouse02
    """
    skip_cloud_storage: true
    """
    And we execute query on clickhouse02
    """
    SELECT count() FROM storage_restore.t
    """
    Then we get response
    """
    1
    """

    Examples:
      | engine                                                     | keep_going |
      | MergeTree()                                                | false      |
      | ReplicatedMergeTree('/storage_restore/t', '{replica}')       | true       |

  Scenario: Remap local active and detached parts after replacing a missing policy
    Given a working clickhouse on clickhouse02
    # Both source disks disappear on the target. Active and detached parts must
    # land on its default disk while retaining their respective states.
    When we execute queries on clickhouse01
    """
    CREATE DATABASE storage_restore;
    CREATE TABLE storage_restore.t (p UInt32, id UInt64) ENGINE = MergeTree
    PARTITION BY p ORDER BY id SETTINGS storage_policy = 'multiple_disks';
    INSERT INTO storage_restore.t VALUES (0, 10);
    INSERT INTO storage_restore.t VALUES (1, 20);
    ALTER TABLE storage_restore.t MOVE PARTITION 0 TO DISK 'hdd1';
    ALTER TABLE storage_restore.t MOVE PARTITION 1 TO DISK 'hdd2';
    ALTER TABLE storage_restore.t DETACH PARTITION 1;
    """
    And we create clickhouse01 clickhouse backup
    """
    detached_parts: true
    detached_parts_disks:
      - hdd2
    """
    And we replace config file storage_configuration.xml in favor of no_storage_configuration.xml on clickhouse02 with restart
    And we restore clickhouse backup #0 to clickhouse02
    """
    skip_cloud_storage: true
    restore_detached_parts: true
    """
    And we execute query on clickhouse02
    """
    SELECT id FROM storage_restore.t
    """
    Then we get response
    """
    10
    """
    When we execute query on clickhouse02
    """
    SELECT disk, count() FROM system.detached_parts WHERE database = 'storage_restore' AND table = 't' GROUP BY disk
    """
    Then we get response
    """
    default	1
    """

  Scenario Outline: Restore named disk tables using the target default policy
    Given a working clickhouse on clickhouse02
    When we execute queries on clickhouse01
    """
    CREATE DATABASE storage_restore;
    CREATE TABLE storage_restore.t (d DateTime, p UInt32, id UInt64)
    ENGINE = MergeTree PARTITION BY p ORDER BY id
    TTL d + INTERVAL 1 DAY TO DISK 'hdd2', d + INTERVAL 100 DAY DELETE
    SETTINGS disk = 'hdd2', index_granularity = 4096;
    CREATE MATERIALIZED VIEW storage_restore.mv ENGINE = MergeTree ORDER BY id
    SETTINGS disk = 'hdd2' AS SELECT d, id FROM storage_restore.t;
    CREATE VIEW storage_restore.v AS SELECT id FROM storage_restore.t;
    CREATE TABLE storage_restore.log (id UInt64) ENGINE = Log SETTINGS disk = 'default';
    CREATE TABLE storage_restore.remote (id UInt64) ENGINE = MergeTree ORDER BY id SETTINGS disk = 's3';
    INSERT INTO storage_restore.remote VALUES (30);
    INSERT INTO storage_restore.t VALUES ('2099-01-01', 0, 10), ('2099-01-01', 1, 20);
    ALTER TABLE storage_restore.t DETACH PARTITION 1;
    """
    And we create clickhouse01 clickhouse backup
    """
    detached_parts: true
    detached_parts_disks:
      - hdd2
    """
    And we replace config file storage_configuration.xml in favor of <config> on clickhouse02 with restart
    And we restore clickhouse backup #0 to clickhouse02
    """
    skip_cloud_storage: true
    restore_detached_parts: true
    keep_going: <keep_going>
    """
    And we execute query on clickhouse02
    """
    SELECT (SELECT id FROM storage_restore.v), (SELECT count() FROM storage_restore.mv), (SELECT count() FROM storage_restore.remote)
    """
    Then we get response
    """
    10	2	0
    """
    When we execute query on clickhouse02
    """
    SELECT storage_policy, position(create_table_query, 'disk =') = 0, position(create_table_query, 'TO DISK') = 0, position(create_table_query, 'toIntervalDay(100)') > 0, position(create_table_query, 'index_granularity = 4096') > 0 FROM system.tables WHERE database = 'storage_restore' AND name = 't'
    """
    Then we get response
    """
    <policy>	1	1	1	1
    """
    When we execute query on clickhouse02
    """
    SELECT disk_name, count() FROM system.parts WHERE database = 'storage_restore' AND table = 't' AND active GROUP BY disk_name
    """
    Then we get response
    """
    <disk>	1
    """
    When we execute query on clickhouse02
    """
    SELECT disk, count() FROM system.detached_parts WHERE database = 'storage_restore' AND table = 't' GROUP BY disk
    """
    Then we get response
    """
    <disk>	1
    """
    When we execute query on clickhouse02
    """
    SELECT position(create_table_query, 'disk =') > 0 FROM system.tables WHERE database = 'storage_restore' AND name = 'log'
    """
    Then we get response
    """
    1
    """

    Examples:
      | config                        | policy  | disk    | keep_going |
      | no_storage_configuration.xml  | default | default | false      |
      | local_storage_defaults.xml    | local   | hdd1    | true       |

  Scenario: Preserve an existing named cloud disk table when storage adaptation would replace it
    Given a working clickhouse on clickhouse02
    When we execute queries on clickhouse01
    """
    CREATE DATABASE storage_restore;
    CREATE TABLE storage_restore.t (id UInt64) ENGINE = MergeTree ORDER BY id SETTINGS disk = 's3';
    INSERT INTO storage_restore.t VALUES (10);
    """
    And we create clickhouse01 clickhouse backup
    And we execute queries on clickhouse02
    """
    CREATE DATABASE storage_restore;
    CREATE TABLE storage_restore.t (id UInt64) ENGINE = MergeTree ORDER BY id SETTINGS disk = 's3';
    INSERT INTO storage_restore.t VALUES (99);
    """
    And we try to execute command on clickhouse02
    """
    ch-backup -c /etc/yandex/ch-backup/ch-backup.conf restore --skip-cloud-storage --keep-going LAST
    """
    Then command exits with code 1
    And we get response contains
    """
    Cannot remove disk setting for existing table storage_restore.t
    """
    When we execute query on clickhouse02
    """
    SELECT id FROM storage_restore.t
    """
    Then we get response
    """
    99
    """

  @require_version_24.4
  Scenario: Preserve materialized view security while removing a named disk
    Given a working clickhouse on clickhouse02
    When we execute queries on clickhouse01
    """
    CREATE DATABASE storage_restore;
    CREATE TABLE storage_restore.t (id UInt64) ENGINE = MergeTree ORDER BY id;
    CREATE MATERIALIZED VIEW storage_restore.mv ENGINE = MergeTree ORDER BY id
    SETTINGS index_granularity = 4096, disk = 'hdd2'
    DEFINER = default SQL SECURITY DEFINER AS SELECT id FROM storage_restore.t;
    INSERT INTO storage_restore.t VALUES (42);
    """
    And we create clickhouse01 clickhouse backup
    And we replace config file storage_configuration.xml in favor of no_storage_configuration.xml on clickhouse02 with restart
    And we restore clickhouse backup #0 to clickhouse02
    """
    skip_cloud_storage: true
    """
    And we execute query on clickhouse02
    """
    SELECT id FROM storage_restore.mv
    """
    Then we get response
    """
    42
    """
    When we execute query on clickhouse02
    """
    SELECT position(create_table_query, 'SQL SECURITY DEFINER') > 0, position(create_table_query, 'disk =') = 0 FROM system.tables WHERE database = 'storage_restore' AND name = 'mv'
    """
    Then we get response
    """
    1	1
    """

  @require_version_24.9
  Scenario: Reject incompatible Keeper metadata before replacing an Atomic database with Replicated
    Given a working clickhouse on clickhouse02
    And we have enabled shared zookeeper for clickhouse01
    And we have enabled shared zookeeper for clickhouse02
    And ClickHouse settings
    """
    allow_experimental_database_replicated: 1
    """
    When we execute queries on clickhouse01
    """
    CREATE DATABASE storage_restore ENGINE = Replicated('/storage_restore_db', '{shard}', '{replica}');
    CREATE TABLE storage_restore.t (d DateTime, id UInt64)
    ENGINE = ReplicatedMergeTree ORDER BY id
    TTL d + INTERVAL 1 DAY TO VOLUME 'external'
    SETTINGS storage_policy = 's3_cold';
    INSERT INTO storage_restore.t VALUES ('2099-01-01', 10);
    """
    And we create clickhouse01 clickhouse backup
    And we replace config file storage_configuration.xml in favor of no_storage_configuration.xml on clickhouse02 with restart
    And we execute queries on clickhouse02
    """
    CREATE DATABASE storage_restore ENGINE = Atomic;
    CREATE TABLE storage_restore.t (id UInt64) ENGINE = MergeTree ORDER BY id;
    INSERT INTO storage_restore.t VALUES (99);
    """
    And we try to execute command on clickhouse02
    """
    ch-backup -c /etc/yandex/ch-backup/ch-backup.conf restore --skip-cloud-storage --keep-going --restore-tables-in-replicated-database --clean-zookeeper-mode all-replicas LAST
    """
    Then command exits with code 1
    And we get response contains
    """
    contains TTL MOVE destinations unavailable in target policy
    """
    When we execute query on clickhouse02
    """
    SELECT engine, (SELECT id FROM storage_restore.t) FROM system.databases WHERE name = 'storage_restore'
    """
    Then we get response
    """
    Atomic	99
    """

  Scenario: Keep a named disk when restoring without skip cloud storage
    Given a working clickhouse on clickhouse02
    When we execute queries on clickhouse01
    """
    CREATE DATABASE storage_restore;
    CREATE TABLE storage_restore.t (id UInt64) ENGINE = MergeTree ORDER BY id SETTINGS disk = 'hdd2';
    INSERT INTO storage_restore.t VALUES (42);
    """
    And we create clickhouse01 clickhouse backup
    And we restore clickhouse backup #0 to clickhouse02
    And we execute query on clickhouse02
    """
    SELECT disk_name, (SELECT id FROM storage_restore.t) FROM system.parts WHERE database = 'storage_restore' AND table = 't' AND active
    """
    Then we get response
    """
    hdd2	42
    """

  Scenario: Restore adapted schemas into fresh Keeper state and join a second replica using backup parts
    Given a working clickhouse on clickhouse02
    When we execute queries on clickhouse01
    """
    CREATE DATABASE storage_restore;
    CREATE TABLE storage_restore.t (d DateTime, id UInt64)
    ENGINE = ReplicatedMergeTree('/storage_restore/t', '{replica}') ORDER BY id
    TTL d + INTERVAL 1 DAY TO VOLUME 'external', d + INTERVAL 100 DAY DELETE
    SETTINGS storage_policy = 's3_cold';
    INSERT INTO storage_restore.t VALUES ('2099-01-01', 10);
    """
    And we create clickhouse01 clickhouse backup
    # Restore both replicas into a fresh shared namespace. The source Keeper
    # namespace retains its old metadata and must not be reset automatically.
    And we dirty remove clickhouse data at clickhouse01
    Given we have enabled shared zookeeper for clickhouse01
    And we have enabled shared zookeeper for clickhouse02
    When we replace config file storage_configuration.xml in favor of no_storage_configuration.xml on clickhouse01 with restart
    And we replace config file storage_configuration.xml in favor of no_storage_configuration.xml on clickhouse02 with restart
    When we restore clickhouse backup #0 to clickhouse01
    """
    skip_cloud_storage: true
    """
    # Restore ordinary data once per shard. The second replica preloads parts
    # from the backup and lets replica recovery register them.
    And we restore clickhouse backup #0 to clickhouse02
    """
    skip_cloud_storage: true
    schema_only: true
    parts_from_backup: 0
    """
    And we execute query on clickhouse02
    """
    SYSTEM SYNC REPLICA storage_restore.t
    """
    And we execute query on clickhouse02
    """
    SELECT count() FROM storage_restore.t
    """
    Then we get response
    """
    1
    """
    When we execute query on clickhouse02
    """
    SYSTEM FLUSH LOGS
    """
    # NewPart without DownloadPart proves recovery used the preloaded backup part
    # instead of fetching it over the replication protocol.
    And we execute query on clickhouse02
    """
    SELECT countIf(event_type = 'NewPart'), countIf(event_type = 'DownloadPart') FROM system.part_log WHERE database = 'storage_restore' AND table = 't'
    """
    Then we get response
    """
    1	0
    """
    When we restore clickhouse backup #0 to clickhouse01
    """
    skip_cloud_storage: true
    """
    And we restart clickhouse on clickhouse02
    And we execute query on clickhouse02
    """
    SELECT is_readonly, (SELECT count() FROM storage_restore.t) FROM system.replicas WHERE database = 'storage_restore' AND table = 't'
    """
    Then we get response
    """
    0	1
    """


  Scenario Outline: Reject incompatible Keeper metadata without changing any replica or local table
    Given a working clickhouse on clickhouse02
    And we have enabled shared zookeeper for clickhouse01
    And we have enabled shared zookeeper for clickhouse02
    When we execute queries on clickhouse01
    """
    CREATE DATABASE storage_restore;
    CREATE TABLE storage_restore.t (d DateTime, id UInt64)
    ENGINE = ReplicatedMergeTree('/storage_restore/t', '{replica}') ORDER BY id
    TTL d + INTERVAL 1 DAY TO VOLUME 'external'
    SETTINGS storage_policy = 's3_cold';
    INSERT INTO storage_restore.t VALUES ('2099-01-01', 10);
    """
    And we create clickhouse01 clickhouse backup
    And we replace config file storage_configuration.xml in favor of no_storage_configuration.xml on clickhouse02 with restart
    And we execute queries on clickhouse02
    """
    CREATE DATABASE storage_restore;
    CREATE TABLE storage_restore.t (id UInt64) ENGINE = MergeTree ORDER BY id;
    INSERT INTO storage_restore.t VALUES (99);
    """
    And we try to execute command on clickhouse02
    """
    ch-backup -c /etc/yandex/ch-backup/ch-backup.conf restore --skip-cloud-storage --keep-going --clean-zookeeper-mode <cleanup_mode> LAST
    """
    Then command exits with code 1
    And we get response contains
    """
    Keeper metadata at /shared/storage_restore/t contains TTL MOVE destinations
    """
    When we execute query on clickhouse02
    """
    SELECT id FROM storage_restore.t
    """
    Then we get response
    """
    99
    """
    When we execute query on clickhouse01
    """
    SELECT is_readonly, total_replicas, active_replicas, (SELECT count() FROM storage_restore.t) FROM system.replicas WHERE database = 'storage_restore' AND table = 't'
    """
    Then we get response
    """
    0	1	1	1
    """
    When we execute query on clickhouse01
    """
    SELECT position(value, 'TO VOLUME') > 0 FROM system.zookeeper WHERE path = '/storage_restore/t' AND name = 'metadata'
    """
    Then we get response
    """
    1
    """

    Examples:
      | cleanup_mode |
      | disabled     |
      | replica-only |
      | all-replicas |

  Scenario Outline: Restore schemas with SQL keywords as column names
    Given a working clickhouse on clickhouse02
    When we execute queries on clickhouse01
    """
    CREATE DATABASE storage_restore;
    CREATE TABLE storage_restore.ttl (`ttl` DateTime, id UInt64)
    ENGINE = MergeTree ORDER BY ttl
    TTL ttl + INTERVAL 1 DAY TO VOLUME 'external', ttl + INTERVAL 100 DAY DELETE
    SETTINGS storage_policy = 's3_cold';
    INSERT INTO storage_restore.ttl VALUES ('2099-01-01', 42);
    CREATE TABLE storage_restore.comment (`comment` DateTime, id UInt64)
    ENGINE = MergeTree ORDER BY comment
    TTL comment + INTERVAL 1 DAY TO VOLUME 'external', comment + INTERVAL 100 DAY DELETE
    SETTINGS storage_policy = 's3_cold';
    INSERT INTO storage_restore.comment VALUES ('2099-01-01', 42);
    CREATE TABLE storage_restore.empty (`empty` DateTime, id UInt64)
    ENGINE = MergeTree ORDER BY empty
    TTL empty + INTERVAL 1 DAY TO VOLUME 'external', empty + INTERVAL 100 DAY DELETE
    SETTINGS storage_policy = 's3_cold';
    INSERT INTO storage_restore.empty VALUES ('2099-01-01', 42);
    CREATE TABLE storage_restore.settings (`settings` DateTime, id UInt64)
    ENGINE = MergeTree ORDER BY settings
    TTL settings + INTERVAL 1 DAY TO VOLUME 'external', settings + INTERVAL 100 DAY DELETE
    SETTINGS storage_policy = 's3_cold';
    INSERT INTO storage_restore.settings VALUES ('2099-01-01', 42);
    CREATE TABLE storage_restore.case_ttl (`case` DateTime, id UInt64)
    ENGINE = MergeTree ORDER BY case
    TTL case + INTERVAL 1 DAY TO VOLUME 'external', case + INTERVAL 100 DAY DELETE
    SETTINGS storage_policy = 's3_cold';
    INSERT INTO storage_restore.case_ttl VALUES ('2099-01-01', 42);
    CREATE TABLE storage_restore.case_policy (`CASE` DateTime, id UInt64)
    ENGINE = MergeTree ORDER BY CASE
    SETTINGS storage_policy = 's3_cold';
    INSERT INTO storage_restore.case_policy VALUES ('2099-01-01', 42);
    CREATE TABLE storage_restore.interval_ttl (`interval` DateTime, id UInt64)
    ENGINE = MergeTree ORDER BY interval
    TTL interval + INTERVAL 1 DAY TO VOLUME 'external', interval + INTERVAL 100 DAY DELETE
    SETTINGS storage_policy = 's3_cold';
    INSERT INTO storage_restore.interval_ttl VALUES ('2099-01-01', 42);
    CREATE TABLE storage_restore.interval_policy (`INTERVAL` DateTime, id UInt64)
    ENGINE = MergeTree ORDER BY INTERVAL
    SETTINGS storage_policy = 's3_cold';
    INSERT INTO storage_restore.interval_policy VALUES ('2099-01-01', 42);
    CREATE TABLE storage_restore.interval_disk (`interval` DateTime, id UInt64)
    ENGINE = MergeTree ORDER BY interval
    SETTINGS disk = 'hdd2';
    INSERT INTO storage_restore.interval_disk VALUES ('2099-01-01', 42);
    CREATE TABLE storage_restore.when_ttl (`when` DateTime, id UInt64)
    ENGINE = MergeTree ORDER BY when
    TTL when + INTERVAL 1 DAY TO VOLUME 'external', when + INTERVAL 100 DAY DELETE
    SETTINGS storage_policy = 's3_cold';
    INSERT INTO storage_restore.when_ttl VALUES ('2099-01-01', 42);
    CREATE TABLE storage_restore.then_policy (`then` DateTime, id UInt64)
    ENGINE = MergeTree ORDER BY then
    SETTINGS storage_policy = 's3_cold';
    INSERT INTO storage_restore.then_policy VALUES ('2099-01-01', 42);
    CREATE TABLE storage_restore.else_disk (`else` DateTime, id UInt64)
    ENGINE = MergeTree ORDER BY else
    SETTINGS disk = 'hdd2';
    INSERT INTO storage_restore.else_disk VALUES ('2099-01-01', 42);
    CREATE TABLE storage_restore.not_expression (d DateTime, empty UInt8, id UInt64)
    ENGINE = MergeTree ORDER BY NOT empty
    TTL d + INTERVAL 1 DAY TO VOLUME 'external', d + INTERVAL 100 DAY DELETE
    SETTINGS storage_policy = 's3_cold';
    INSERT INTO storage_restore.not_expression VALUES ('2099-01-01', 0, 42);
    """
    And we create clickhouse01 clickhouse backup
    And we replace config file storage_configuration.xml in favor of no_storage_configuration.xml on clickhouse02 with restart
    And we restore clickhouse backup #0 to clickhouse02
    """
    skip_cloud_storage: true
    keep_going: <keep_going>
    """
    And we execute query on clickhouse02
    """
    SELECT _table, id FROM merge('storage_restore', '.*') ORDER BY _table
    """
    Then we get response
    """
    case_policy	42
    case_ttl	42
    comment	42
    else_disk	42
    empty	42
    interval_disk	42
    interval_policy	42
    interval_ttl	42
    not_expression	42
    settings	42
    then_policy	42
    ttl	42
    when_ttl	42
    """
    When we execute query on clickhouse02
    """
    SELECT count(), countIf(storage_policy = 'default'), countIf(position(create_table_query, 'disk =') = 0), countIf(position(create_table_query, 'TO VOLUME') = 0), countIf(position(create_table_query, 'toIntervalDay(100)') > 0) FROM system.tables WHERE database = 'storage_restore'
    """
    Then we get response
    """
    13	13	13	13	8
    """
    When we execute query on clickhouse02
    """
    SELECT disk_name, count() FROM system.parts WHERE database = 'storage_restore' AND active GROUP BY disk_name
    """
    Then we get response
    """
    default	13
    """

    Examples:
      | keep_going |
      | false      |
      | true       |

  Scenario: Restore an unchanged replicated schema without direct Keeper configuration
    Given a working clickhouse on clickhouse02
    When we execute queries on clickhouse01
    """
    CREATE DATABASE storage_restore;
    CREATE TABLE storage_restore.t (id UInt64)
    ENGINE = ReplicatedMergeTree('/storage_restore/t', '{replica}') ORDER BY id;
    INSERT INTO storage_restore.t VALUES (42);
    """
    And we create clickhouse01 clickhouse backup
    Given ch-backup configuration on clickhouse02
    """
    zookeeper:
      hosts: []
    lock:
      zk_flock: false
    """
    When we restore clickhouse backup #0 to clickhouse02
    """
    skip_cloud_storage: true
    clean_zookeeper_mode: disabled
    """
    And we execute query on clickhouse02
    """
    SELECT is_readonly, (SELECT id FROM storage_restore.t) FROM system.replicas WHERE database = 'storage_restore' AND table = 't'
    """
    Then we get response
    """
    0	42
    """
