Feature: Detached parts backup

  Background:
    Given default configuration
    And a working s3
    And a working zookeeper on zookeeper01
    And a working clickhouse on clickhouse01
    And a working clickhouse on clickhouse02

  Scenario: Filter detached parts by table, disk, and empty reason
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

    # Restore support is intentionally not part of this change. Metadata is
    # ignored and no detached directory is populated on the target node.
    When we drop all databases at clickhouse02
    And we restore clickhouse backup #0 to clickhouse02
    When we execute query on clickhouse02
    """
    SELECT
      (SELECT groupArray(id) FROM detached_db.selected),
      (SELECT count() FROM system.detached_parts
       WHERE database = 'detached_db' AND table = 'selected')
    FORMAT TSV
    """
    Then we get response
    """
    [2]	0
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

  @require_version_20.8
  Scenario: Disk filter accepts a list of local disks
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

  @require_version_22.8
  Scenario: Back up a detached part from an S3 disk
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
