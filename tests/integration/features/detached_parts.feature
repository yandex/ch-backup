Feature: Detached parts backup and restore

  Background:
    Given default configuration
    And a working s3
    And a working zookeeper on zookeeper01
    And a working clickhouse on clickhouse01
    And a working clickhouse on clickhouse02

  Scenario: Filter and restore user-detached parts without attaching them
    Given we have executed queries on clickhouse01
    """
    CREATE DATABASE detached_db;
    CREATE TABLE detached_db.selected (id UInt32)
      ENGINE = MergeTree PARTITION BY id ORDER BY id;
    CREATE TABLE detached_db.not_selected (id UInt32)
      ENGINE = MergeTree PARTITION BY id ORDER BY id;
    INSERT INTO detached_db.selected VALUES (1), (2);
    INSERT INTO detached_db.not_selected VALUES (1);
    ALTER TABLE detached_db.selected DETACH PARTITION 1;
    ALTER TABLE detached_db.not_selected DETACH PARTITION 1;
    """
    When we create clickhouse01 clickhouse backup
    """
    tables:
      - detached_db.selected
    detached_parts: true
    detached_parts_disks:
      - default
    detached_parts_reasons:
      - ""
    """
    Then we got the following backups on clickhouse01
      | num | state   | data_count | link_count |
      | 0   | created | 2          | 0          |
    And detached archives of "detached_db"."selected" on disk "default" in clickhouse01 backup #0 match source parts

    When we drop all databases at clickhouse02
    And we restore clickhouse backup #0 to clickhouse02
    When we execute query on clickhouse02
    """
    SELECT count()
    FROM system.detached_parts
    WHERE database = 'detached_db' AND table = 'selected'
    FORMAT TSV
    """
    Then we get response
    """
    0
    """

    When we restore clickhouse backup #0 to clickhouse02
    """
    restore_detached_parts: true
    """
    When we execute query on clickhouse02
    """
    SELECT table, reason, count()
    FROM system.detached_parts
    WHERE database = 'detached_db'
    GROUP BY table, reason
    FORMAT TSV
    """
    Then we get response
    """
    selected		1
    """
    When we execute query on clickhouse02
    """
    SELECT groupArray(id) FROM detached_db.selected FORMAT TSV
    """
    Then we get response
    """
    [2]
    """

    # A conflicting directory must be preserved. Repeated restore uses the
    # same _tryN convention as ClickHouse instead of overwriting it.
    When we execute command on clickhouse02
    """
    find /var/lib/clickhouse -path '*/detached/*/checksums.txt' -exec sh -c 'printf conflict >> "$1"' _ {} \;
    """
    And we restore clickhouse backup #0 to clickhouse02
    """
    restore_detached_parts: true
    """
    When we execute query on clickhouse02
    """
    SELECT count(), countIf(endsWith(name, '_try1'))
    FROM system.detached_parts
    WHERE database = 'detached_db' AND table = 'selected'
    FORMAT TSV
    """
    Then we get response
    """
    2	1
    """

  Scenario: Detached parts are not backed up by default
    Given we have executed queries on clickhouse01
    """
    CREATE DATABASE detached_db;
    CREATE TABLE detached_db.opt_in (id UInt32)
      ENGINE = MergeTree PARTITION BY id ORDER BY id;
    INSERT INTO detached_db.opt_in VALUES (1), (2);
    ALTER TABLE detached_db.opt_in DETACH PARTITION 1;
    """
    When we create clickhouse01 clickhouse backup
    """
    tables:
      - detached_db.opt_in
    """
    Then we got the following backups on clickhouse01
      | num | state   | data_count | link_count |
      | 0   | created | 1          | 0          |
    When we drop all databases at clickhouse02
    And we restore clickhouse backup #0 to clickhouse02
    """
    restore_detached_parts: true
    """
    When we execute query on clickhouse02
    """
    SELECT
      (SELECT count() FROM detached_db.opt_in),
      (SELECT count() FROM system.detached_parts
       WHERE database = 'detached_db' AND table = 'opt_in')
    FORMAT TSV
    """
    Then we get response
    """
    1	0
    """

  @require_version_20.8
  Scenario: Filter and restore detached parts from multiple local disks
    Given we have executed queries on clickhouse01
    """
    CREATE DATABASE detached_db;
    CREATE TABLE detached_db.multidisk (id UInt32)
      ENGINE = MergeTree PARTITION BY id ORDER BY id
      SETTINGS storage_policy = 'multiple_disks';
    INSERT INTO detached_db.multidisk VALUES (1), (2), (3), (4);
    ALTER TABLE detached_db.multidisk MOVE PARTITION 1 TO DISK 'hdd1';
    ALTER TABLE detached_db.multidisk MOVE PARTITION 2 TO DISK 'hdd2';
    ALTER TABLE detached_db.multidisk DETACH PARTITION 1;
    ALTER TABLE detached_db.multidisk DETACH PARTITION 2;
    ALTER TABLE detached_db.multidisk DETACH PARTITION 4;
    """
    When we create clickhouse01 clickhouse backup
    """
    tables:
      - detached_db.multidisk
    detached_parts: true
    detached_parts_disks:
      - default
      - hdd1
    """
    Then we got the following backups on clickhouse01
      | num | state   | data_count | link_count |
      | 0   | created | 3          | 0          |
    When we drop all databases at clickhouse02
    And we restore clickhouse backup #0 to clickhouse02
    """
    restore_detached_parts: true
    """
    When we execute query on clickhouse02
    """
    SELECT disk, count()
    FROM system.detached_parts
    WHERE database = 'detached_db' AND table = 'multidisk'
    GROUP BY disk
    ORDER BY disk
    FORMAT TSV
    """
    Then we get response
    """
    default	1
    hdd1	1
    """
    When we execute query on clickhouse02
    """
    SELECT groupArray(id) FROM detached_db.multidisk FORMAT TSV
    """
    Then we get response
    """
    [3]
    """

  Scenario: Schema-only restore leaves detached parts out
    Given we have executed queries on clickhouse01
    """
    CREATE DATABASE detached_db;
    CREATE TABLE detached_db.schema_only (id UInt32)
      ENGINE = MergeTree PARTITION BY id ORDER BY id;
    INSERT INTO detached_db.schema_only VALUES (1), (2);
    ALTER TABLE detached_db.schema_only DETACH PARTITION 1;
    """
    When we create clickhouse01 clickhouse backup
    """
    tables:
      - detached_db.schema_only
    detached_parts: true
    detached_parts_disks:
      - default
    """
    When we drop all databases at clickhouse02
    And we restore clickhouse backup #0 to clickhouse02
    """
    schema_only: true
    restore_detached_parts: true
    """
    When we execute query on clickhouse02
    """
    SELECT count()
    FROM system.detached_parts
    WHERE database = 'detached_db' AND table = 'schema_only'
    FORMAT TSV
    """
    Then we get response
    """
    0
    """
    When we execute query on clickhouse02
    """
    SELECT count() FROM detached_db.schema_only FORMAT TSV
    """
    Then we get response
    """
    0
    """

  @require_version_22.8
  Scenario: Partial restore includes detached parts only for selected tables
    Given we have executed queries on clickhouse01
    """
    CREATE DATABASE detached_db;
    CREATE TABLE detached_db.local_only (id UInt32)
      ENGINE = MergeTree PARTITION BY id ORDER BY id;
    CREATE TABLE detached_db.excluded_object_storage (id UInt32)
      ENGINE = MergeTree PARTITION BY id ORDER BY id
      SETTINGS storage_policy = 's3';
    INSERT INTO detached_db.local_only VALUES (1);
    INSERT INTO detached_db.excluded_object_storage VALUES (2);
    ALTER TABLE detached_db.local_only DETACH PARTITION 1;
    ALTER TABLE detached_db.excluded_object_storage DETACH PARTITION 2;
    """
    When we create clickhouse01 clickhouse backup
    """
    databases:
      - detached_db
    detached_parts: true
    detached_parts_disks:
      - default
      - s3
    """
    When we drop all databases at clickhouse02
    And we restore clickhouse backup #0 to clickhouse02
    """
    table_included_patterns: detached_db.local_only
    restore_detached_parts: true
    """
    When we execute query on clickhouse02
    """
    SELECT name
    FROM system.tables
    WHERE database = 'detached_db'
    ORDER BY name
    FORMAT TSV
    """
    Then we get response
    """
    local_only
    """
    When we execute query on clickhouse02
    """
    SELECT table, count()
    FROM system.detached_parts
    WHERE database = 'detached_db'
    GROUP BY table
    FORMAT TSV
    """
    Then we get response
    """
    local_only	1
    """

  @require_version_22.8
  Scenario: Restore detached part from an S3 disk without attaching it
    Given we have executed queries on clickhouse01
    """
    CREATE DATABASE detached_db;
    CREATE TABLE detached_db.object_storage (id UInt32)
      ENGINE = MergeTree PARTITION BY id ORDER BY id
      SETTINGS storage_policy = 's3';
    INSERT INTO detached_db.object_storage VALUES (1), (2);
    ALTER TABLE detached_db.object_storage DETACH PARTITION 1;
    """
    When we create clickhouse01 clickhouse backup
    """
    tables:
      - detached_db.object_storage
    detached_parts: true
    detached_parts_disks:
      - s3
    """
    Then we got the following backups on clickhouse01
      | num | state   | data_count | link_count |
      | 0   | created | 2          | 0          |
    Given we use the same object storage bucket for clickhouse02 as on clickhouse01
    When we drop all databases at clickhouse02
    And we restore clickhouse backup #0 to clickhouse02
    """
    cloud_storage_source_bucket: cloud-storage-01
    cloud_storage_source_path: data
    restore_detached_parts: true
    """
    When we execute query on clickhouse02
    """
    SELECT count()
    FROM system.detached_parts
    WHERE database = 'detached_db' AND table = 'object_storage'
    FORMAT TSV
    """
    Then we get response
    """
    1
    """
    When we execute query on clickhouse02
    """
    SELECT groupArray(id) FROM detached_db.object_storage FORMAT TSV
    """
    Then we get response
    """
    [2]
    """
    When we execute queries on clickhouse02
    """
    ALTER TABLE detached_db.object_storage ATTACH PARTITION 1;
    """
    When we execute query on clickhouse02
    """
    SELECT groupArray(id)
    FROM (SELECT id FROM detached_db.object_storage ORDER BY id)
    FORMAT TSV;
    """
    Then we get response
    """
    [1, 2]
    """

  @require_version_22.8
  Scenario: Full cloud backup keeps detached S3 parts metadata-only
    Given we have executed queries on clickhouse01
    """
    CREATE DATABASE detached_db;
    CREATE TABLE detached_db.object_storage_full (id UInt32)
      ENGINE = MergeTree PARTITION BY id ORDER BY id
      SETTINGS storage_policy = 's3';
    INSERT INTO detached_db.object_storage_full VALUES (1), (2);
    ALTER TABLE detached_db.object_storage_full DETACH PARTITION 1;
    """
    When we create clickhouse01 clickhouse backup
    """
    tables:
      - detached_db.object_storage_full
    detached_parts: true
    detached_parts_disks:
      - s3
    copy_cloud_storage_data: true
    """
    Then we got the following backups on clickhouse01
      | num | state   | data_count | link_count |
      | 0   | created | 2          | 0          |
    Given we use the same object storage bucket for clickhouse02 as on clickhouse01
    When we drop all databases at clickhouse02
    And we restore clickhouse backup #0 to clickhouse02
    """
    cloud_storage_source_bucket: cloud-storage-01
    cloud_storage_source_path: data
    restore_detached_parts: true
    """
    When we execute query on clickhouse02
    """
    SELECT
      (SELECT count() FROM detached_db.object_storage_full),
      (SELECT count() FROM system.detached_parts
       WHERE database = 'detached_db' AND table = 'object_storage_full')
    FORMAT TSV
    """
    Then we get response
    """
    1	1
    """

  @require_version_22.8
  Scenario: Skip active and detached S3 parts with skip-cloud-storage
    Given we have executed queries on clickhouse01
    """
    CREATE DATABASE detached_db;
    CREATE TABLE detached_db.skip_object_storage (id UInt32)
      ENGINE = MergeTree PARTITION BY id ORDER BY id
      SETTINGS storage_policy = 's3';
    INSERT INTO detached_db.skip_object_storage VALUES (1), (2);
    ALTER TABLE detached_db.skip_object_storage DETACH PARTITION 1;
    """
    When we create clickhouse01 clickhouse backup
    """
    tables:
      - detached_db.skip_object_storage
    detached_parts: true
    detached_parts_disks:
      - s3
    """
    When we drop all databases at clickhouse02
    And we restore clickhouse backup #0 to clickhouse02
    """
    skip_cloud_storage: true
    restore_detached_parts: true
    """
    When we execute query on clickhouse02
    """
    SELECT
      (SELECT count() FROM detached_db.skip_object_storage),
      (SELECT count() FROM system.detached_parts
       WHERE database = 'detached_db' AND table = 'skip_object_storage')
    FORMAT TSV
    """
    Then we get response
    """
    0	0
    """

  @require_version_22.8
  Scenario: Copy a backup containing detached S3 metadata
    Given we have executed queries on clickhouse01
    """
    CREATE DATABASE detached_db;
    CREATE TABLE detached_db.object_storage_copy (id UInt32)
      ENGINE = MergeTree PARTITION BY id ORDER BY id
      SETTINGS storage_policy = 's3';
    INSERT INTO detached_db.object_storage_copy VALUES (1), (2);
    ALTER TABLE detached_db.object_storage_copy DETACH PARTITION 1;
    """
    And we have created clickhouse01 clickhouse backup
    """
    tables:
      - detached_db.object_storage_copy
    detached_parts: true
    detached_parts_disks:
      - s3
    """
    And ch-backup configuration on clickhouse02
    """
    storage:
        credentials:
            bucket: ch-backup-copy
    """
    When we execute command on clickhouse01
    """
    ch-backup -c /etc/yandex/ch-backup/ch-backup.conf copy LAST \
        --target-config-parameter storage.credentials.bucket '"ch-backup-copy"' \
        --target-cloud-storage-bucket cloud-storage-02 \
        --target-cloud-storage-path copy
    """
    Then we get response contains
    """
    : copied
    """
    And we got the following backups on clickhouse02
      | num | state   | data_count | link_count |
      | 0   | created | 2          | 0          |
