Feature: Copy backups to another storage

  Background:
    Given default configuration
    And a working s3
    And a working zookeeper on zookeeper01
    And a working clickhouse on clickhouse01
    And a working clickhouse on clickhouse02

  Scenario Outline: Copy backup to another bucket without encryption keys
    Given clickhouse on clickhouse01 has test schema
    And clickhouse01 has test clickhouse data test1
    And we have created clickhouse01 clickhouse backup
    And ch-backup configuration on clickhouse02
    """
    storage:
        credentials:
            bucket: ch-backup-copy
    """
    When we execute command on clickhouse01
    """
    ch-backup -c /etc/yandex/ch-backup/ch-backup.conf --config-parameter encryption.key null copy LAST <options> \
        --target-config-parameter storage.credentials.bucket '"ch-backup-copy"'
    """
    Then we get response contains
    """
    : copied
    """
    And we got the following backups on clickhouse02
      | num | state   | link_count |
      | 0   | created | 0          |
    When we restore clickhouse backup #0 to clickhouse02
    Then we got same clickhouse data at clickhouse01 clickhouse02
    When we execute command on clickhouse01
    """
    ch-backup -c /etc/yandex/ch-backup/ch-backup.conf --config-parameter encryption.key null copy LAST <options> \
        --target-config-parameter storage.credentials.bucket '"ch-backup-copy"'
    """
    Then we get response contains
    """
    skipped, already present in target storage
    """

    Examples:
      | options               |
      | --server-side-copy    |
      | --no-server-side-copy |

  Scenario: Copy backup to another path in the same bucket
    Given clickhouse on clickhouse01 has test schema
    And clickhouse01 has test clickhouse data test1
    And we have created clickhouse01 clickhouse backup
    And ch-backup configuration on clickhouse02
    """
    backup:
        path_root: ch_backup_copy/
    """
    When we execute command on clickhouse01
    """
    ch-backup -c /etc/yandex/ch-backup/ch-backup.conf copy LAST \
        --target-config-parameter backup.path_root '"ch_backup_copy/"'
    """
    Then we got the following backups on clickhouse02
      | num | state   |
      | 0   | created |
    When we restore clickhouse backup #0 to clickhouse02
    Then we got same clickhouse data at clickhouse01 clickhouse02

  Scenario Outline: Copy <selection> skips backups without data
    Given clickhouse on clickhouse01 has test schema
    And clickhouse01 has test clickhouse data test1
    And we have created clickhouse01 clickhouse backup
    """
    name: with-data
    """
    And we have created clickhouse01 clickhouse backup
    """
    name: schema-only
    schema_only: true
    """
    And ch-backup configuration on clickhouse02
    """
    backup:
        path_root: ch_backup_copy/
    """
    When we execute command on clickhouse01
    """
    ch-backup -c /etc/yandex/ch-backup/ch-backup.conf copy <selection> \
        --target-config-parameter backup.path_root '"ch_backup_copy/"'
    """
    Then we got the following backups on clickhouse02
      | num | name      | state   |
      | 0   | with-data | created |
    When we restore clickhouse backup #0 to clickhouse02
    Then we got same clickhouse data at clickhouse01 clickhouse02

    Examples:
      | selection |
      | LAST      |
      | --all     |

  Scenario: Copy only the latest deduplicated backup
    Given we have executed queries on clickhouse01
    """
    CREATE DATABASE test_db1;

    CREATE TABLE test_db1.test_table1 (partition_id Int32, n Int32)
    ENGINE = MergeTree() PARTITION BY partition_id ORDER BY (partition_id, n);

    INSERT INTO test_db1.test_table1 SELECT number % 2, number FROM system.numbers LIMIT 100;
    """
    And we have created clickhouse01 clickhouse backup
    And we have executed queries on clickhouse01
    """
    CREATE TABLE test_db1.test_table2 (partition_id Int32, n Int32)
    ENGINE = MergeTree() PARTITION BY partition_id ORDER BY (partition_id, n);

    INSERT INTO test_db1.test_table2 SELECT number % 3, number FROM system.numbers LIMIT 100;
    """
    And ch-backup configuration on clickhouse02
    """
    storage:
        credentials:
            bucket: ch-backup-copy
    """
    When we create clickhouse01 clickhouse backup
    Then we got the following backups on clickhouse01
      | num | state   | data_count | link_count |
      | 0   | created | 3          | 2          |
      | 1   | created | 2          | 0          |
    When we execute command on clickhouse01
    """
    ch-backup -c /etc/yandex/ch-backup/ch-backup.conf copy LAST \
        --target-config-parameter storage.credentials.bucket '"ch-backup-copy"'
    """
    Then we got the following backups on clickhouse02
      | num | state   | data_count | link_count |
      | 0   | created | 5          | 0          |
    When we restore clickhouse backup #0 to clickhouse02
    Then we got same clickhouse data at clickhouse01 clickhouse02

  Scenario: Copy all deduplicated backups keeps deduplication
    Given we have executed queries on clickhouse01
    """
    CREATE DATABASE test_db1;

    CREATE TABLE test_db1.test_table1 (partition_id Int32, n Int32)
    ENGINE = MergeTree() PARTITION BY partition_id ORDER BY (partition_id, n);

    INSERT INTO test_db1.test_table1 SELECT number % 2, number FROM system.numbers LIMIT 100;
    """
    And we have created clickhouse01 clickhouse backup
    And we have executed queries on clickhouse01
    """
    CREATE TABLE test_db1.test_table2 (partition_id Int32, n Int32)
    ENGINE = MergeTree() PARTITION BY partition_id ORDER BY (partition_id, n);

    INSERT INTO test_db1.test_table2 SELECT number % 3, number FROM system.numbers LIMIT 100;
    """
    And ch-backup configuration on clickhouse02
    """
    storage:
        credentials:
            bucket: ch-backup-copy
    """
    When we create clickhouse01 clickhouse backup
    And we execute command on clickhouse01
    """
    ch-backup -c /etc/yandex/ch-backup/ch-backup.conf copy --all \
        --target-config-parameter storage.credentials.bucket '"ch-backup-copy"'
    """
    Then we got the following backups on clickhouse02
      | num | state   | data_count | link_count |
      | 0   | created | 3          | 2          |
      | 1   | created | 2          | 0          |
    When we restore clickhouse backup #0 to clickhouse02
    Then we got same clickhouse data at clickhouse01 clickhouse02

  @require_version_22.8
  Scenario Outline: Copy target window survives source renewal and owner removal
    Given we have executed queries on clickhouse01
    """
    CREATE DATABASE test_db1;
    CREATE TABLE test_db1.test_table1 (n Int32) ENGINE = MergeTree ORDER BY n;
    INSERT INTO test_db1.test_table1 SELECT number FROM numbers(100);
    """
    And we have created clickhouse01 clickhouse backup
    """
    name: a_owner
    """
    And ch-backup configuration on clickhouse02
    """
    storage:
        credentials:
            bucket: ch-backup-copy
    backup:
        retain_count: 1
    """
    When we execute command on clickhouse01
    """
    ch-backup -c /etc/yandex/ch-backup/ch-backup.conf copy a_owner <copy_mode> \
        --target-config-parameter storage.credentials.bucket '"ch-backup-copy"' \
        --target-config-parameter copy.deduplication_age_limit.days 90
    """
    And metadata of clickhouse01 backup "a_owner" was adjusted with
    """
    meta:
        start_time: {{ backup.start_time | decrease_on('8 days') }}
        end_time: {{ backup.end_time | decrease_on('8 days') }}
    """
    And metadata of clickhouse02 backup "a_owner" was adjusted with
    """
    meta:
        start_time: {{ backup.start_time | decrease_on('8 days') }}
        end_time: {{ backup.end_time | decrease_on('8 days') }}
    """
    And we create clickhouse01 clickhouse backup
    """
    name: b_renewed
    """
    Then we got the following backups on clickhouse01
      | num | state   | data_count | link_count |
      | 0   | created | 1          | 0          |
      | 1   | created | 1          | 0          |
    When we execute command on clickhouse01
    """
    ch-backup -c /etc/yandex/ch-backup/ch-backup.conf --config-parameter encryption.key null copy b_renewed <copy_mode> \
        --target-config-parameter storage.credentials.bucket '"ch-backup-copy"' \
        --target-config-parameter copy.deduplication_age_limit.days 90
    """
    Then we got the following backups on clickhouse02
      | num | state   | data_count | link_count |
      | 0   | created | 0          | 1          |
      | 1   | created | 1          | 0          |
    And metadata of clickhouse02 backup "b_renewed" contains
    """
    real_bytes: 0
    """
    And s3 bucket ch-backup-copy contains no objects with prefix "ch_backup/b_renewed/data/"
    And s3 bucket ch-backup-copy contains objects with prefix "ch_backup/a_owner/data/"
    When we restore clickhouse backup "b_renewed" to clickhouse02
    Then we got same clickhouse data at clickhouse01 clickhouse02
    When we execute command on clickhouse02
    """
    ch-backup -c /etc/yandex/ch-backup/ch-backup.conf <remove_owner>
    """
    Then we got the following backups on clickhouse02
      | num | state             | data_count | link_count |
      | 0   | created           | 0          | 1          |
      | 1   | partially_deleted | 1          | 0          |
    # Start a new restore rather than resume the previously completed one.
    Given ch-backup configuration on clickhouse02
    """
    backup:
        restore_context_path: /tmp/copy-restore-after-delete.json
    """
    When we drop all databases at clickhouse02
    And we restore clickhouse backup "b_renewed" to clickhouse02
    Then we got same clickhouse data at clickhouse01 clickhouse02
    When we delete clickhouse02 clickhouse backup "b_renewed"
    And we execute command on clickhouse02
    """
    ch-backup -c /etc/yandex/ch-backup/ch-backup.conf --config-parameter backup.retain_count 0 purge
    """
    Then s3 bucket ch-backup-copy contains 0 objects

    Examples:
      | copy_mode             | remove_owner   |
      | --server-side-copy    | delete a_owner |
      | --no-server-side-copy | purge          |

  Scenario: Copy keeps links to parts of a copy that fails to be published
    Given we have executed queries on clickhouse01
    """
    CREATE DATABASE test_db1;
    CREATE TABLE test_db1.test_table1 (n Int32) ENGINE = MergeTree ORDER BY n;
    INSERT INTO test_db1.test_table1 SELECT number FROM numbers(100);
    """
    And we have created clickhouse01 clickhouse backup
    """
    name: a_owner
    """
    And we have created clickhouse01 clickhouse backup
    """
    name: b_linked
    """
    And we have created clickhouse01 clickhouse backup
    """
    name: c_linked
    """
    And ch-backup configuration on clickhouse02
    """
    storage:
        credentials:
            bucket: ch-backup-copy
    """
    When we try to execute ch-backup on clickhouse01 with publication of "a_owner" failing
    """
    -c /etc/yandex/ch-backup/ch-backup.conf copy --all --keep-going
        --target-config-parameter storage.credentials.bucket '"ch-backup-copy"'
        --report-path /tmp/copy-report.json
    """
    Then command exits with code 1
    And we got the following backups on clickhouse02
      | num | state   | data_count | link_count |
      | 0   | created | 0          | 1          |
      | 1   | created | 0          | 1          |
      | 2   | failed  | 1          | 0          |
    And all parts in clickhouse02 backup "c_linked" link to "a_owner"
    When we restore clickhouse backup #0 to clickhouse02
    Then we got same clickhouse data at clickhouse01 clickhouse02

  Scenario: Copy fails if target storage contains another backup with the same name
    Given clickhouse on clickhouse01 has test schema
    And clickhouse01 has test clickhouse data test1
    And we have created clickhouse01 clickhouse backup
    """
    name: test_backup
    """
    And ch-backup configuration on clickhouse02
    """
    storage:
        credentials:
            bucket: ch-backup-copy
    """
    And we have created clickhouse02 clickhouse backup
    """
    name: test_backup
    """
    When we try to execute command on clickhouse01
    """
    ch-backup -c /etc/yandex/ch-backup/ch-backup.conf copy test_backup \
        --target-config-parameter storage.credentials.bucket '"ch-backup-copy"'
    """
    Then we get response contains
    """
    already exists in target storage and it is not a copy of the source backup
    """

  Scenario: Copy keeps going after a conflicting backup and saves a failure report
    Given clickhouse on clickhouse01 has test schema
    And clickhouse01 has test clickhouse data test1
    And we have created clickhouse01 clickhouse backup
    """
    name: first
    """
    And we have created clickhouse01 clickhouse backup
    """
    name: conflicting
    """
    And we have created clickhouse01 clickhouse backup
    """
    name: last
    """
    And ch-backup configuration on clickhouse02
    """
    storage:
        credentials:
            bucket: ch-backup-copy
    """
    And we have created clickhouse02 clickhouse backup
    """
    name: conflicting
    """
    When we try to execute command on clickhouse01
    """
    ch-backup -c /etc/yandex/ch-backup/ch-backup.conf copy --all --keep-going \
        --target-config-parameter storage.credentials.bucket '"ch-backup-copy"' \
        --report-path /tmp/copy-report.json
    """
    Then command exits with code 1
    And we get response contains
    """
    first: copied
    """
    And we get response contains
    """
    conflicting: failed
    """
    And we get response contains
    """
    last: copied
    """
    And failed copy report "/tmp/copy-report.json" on clickhouse01 has results
    """
    first: {status: copied}
    conflicting: {status: failed}
    last: {status: copied}
    """

  Scenario: Copy keeps going after corrupt source metadata
    Given clickhouse on clickhouse01 has test schema
    And clickhouse01 has test clickhouse data test1
    And we have created clickhouse01 clickhouse backup
    """
    name: first
    """
    And we have created clickhouse01 clickhouse backup
    """
    name: broken
    """
    And we have created clickhouse01 clickhouse backup
    """
    name: last
    """
    When we put object in S3
    """
    bucket: ch-backup
    path: ch_backup/broken/backup_light_struct.json
    data: invalid metadata
    """
    And we try to execute command on clickhouse01
    """
    ch-backup -c /etc/yandex/ch-backup/ch-backup.conf copy --all --keep-going \
        --target-config-parameter storage.credentials.bucket '"ch-backup-copy"' \
        --report-path /tmp/copy-report.json
    """
    Then command exits with code 1
    And failed copy report "/tmp/copy-report.json" on clickhouse01 has results
    """
    first: {status: copied}
    broken:
      status: failed
      objects_total: null
      error_stage: selection
    last: {status: copied}
    """

  @require_version_22.8
  Scenario: Copy keeps going after object failures and leaves the incomplete backup failed
    Given we have executed queries on clickhouse01
    """
    CREATE DATABASE test_db;
    CREATE TABLE test_db.remote_data (n UInt32) ENGINE = MergeTree ORDER BY n
        SETTINGS storage_policy = 's3';
    CREATE TABLE test_db.local_data (n UInt32) ENGINE = MergeTree ORDER BY n;
    INSERT INTO test_db.remote_data VALUES (1), (2);
    INSERT INTO test_db.local_data VALUES (3), (4);
    """
    And we have created clickhouse01 clickhouse backup
    """
    name: a_cloud
    tables:
      - test_db.remote_data
    """
    And we have created clickhouse01 clickhouse backup
    """
    name: b_local
    tables:
      - test_db.local_data
    """
    And ch-backup configuration on clickhouse02
    """
    storage:
        credentials:
            bucket: ch-backup-copy
    """
    When we delete all objects in s3 bucket cloud-storage-01
    And we try to execute command on clickhouse01
    """
    ch-backup -c /etc/yandex/ch-backup/ch-backup.conf copy --all --keep-going \
        --no-server-side-copy \
        --target-config-parameter storage.credentials.bucket '"ch-backup-copy"' \
        --target-cloud-storage-bucket cloud-storage-02 \
        --report-path /tmp/copy-report.json
    """
    Then command exits with code 1
    And we got the following backups on clickhouse02
      | num | state   | data_count | link_count |
      | 0   | created | 1          | 0          |
      | 1   | failed  | 1          | 0          |
    And failed copy report "/tmp/copy-report.json" on clickhouse01 has results
    """
    a_cloud:
      status: failed
      partial_copy: true
      error_stage: copy
      error_sources: true
    b_local: {status: copied}
    """

  Scenario: Copy isolates corrupt target metadata
    Given clickhouse on clickhouse01 has test schema
    And clickhouse01 has test clickhouse data test1
    And we have created clickhouse01 clickhouse backup
    """
    name: first
    """
    And we have created clickhouse01 clickhouse backup
    """
    name: broken
    """
    And we have created clickhouse01 clickhouse backup
    """
    name: last
    """
    When we execute command on clickhouse01
    """
    ch-backup -c /etc/yandex/ch-backup/ch-backup.conf copy broken \
        --target-config-parameter storage.credentials.bucket '"ch-backup-copy"'
    """
    And we put object in S3
    """
    bucket: ch-backup-copy
    path: ch_backup/broken/backup_light_struct.json
    data: invalid metadata
    """
    And we try to execute command on clickhouse01
    """
    ch-backup -c /etc/yandex/ch-backup/ch-backup.conf copy --all --keep-going \
        --target-config-parameter storage.credentials.bucket '"ch-backup-copy"' \
        --report-path /tmp/copy-report.json
    """
    Then command exits with code 1
    And failed copy report "/tmp/copy-report.json" on clickhouse01 has results
    """
    first: {status: copied}
    broken:
      status: failed
      error_stage: target_selection
    last: {status: copied}
    """

  Scenario: Copy reports a missing named backup
    When we try to execute command on clickhouse01
    """
    ch-backup -c /etc/yandex/ch-backup/ch-backup.conf copy missing --keep-going \
        --target-config-parameter storage.credentials.bucket '"ch-backup-copy"' \
        --report-path /tmp/copy-report.json
    """
    Then command exits with code 1
    And failed copy report "/tmp/copy-report.json" on clickhouse01 has results
    """
    missing:
      status: failed
      error_stage: selection
    """

  Scenario: Copy materializes a dependency with corrupt target metadata
    Given clickhouse on clickhouse01 has test schema
    And clickhouse01 has test clickhouse data test1
    And we have created clickhouse01 clickhouse backup
    """
    name: base
    """
    And we have created clickhouse01 clickhouse backup
    """
    name: next
    """
    And ch-backup configuration on clickhouse02
    """
    storage:
        credentials:
            bucket: ch-backup-copy
    """
    When we execute command on clickhouse01
    """
    ch-backup -c /etc/yandex/ch-backup/ch-backup.conf copy base \
        --target-config-parameter storage.credentials.bucket '"ch-backup-copy"'
    """
    And we put object in S3
    """
    bucket: ch-backup-copy
    path: ch_backup/base/backup_struct.json
    data: invalid metadata
    """
    And we execute command on clickhouse01
    """
    ch-backup -c /etc/yandex/ch-backup/ch-backup.conf copy next --keep-going \
        --target-config-parameter storage.credentials.bucket '"ch-backup-copy"'
    """
    Then we get response contains
    """
    next: copied
    """
    When we restore clickhouse backup #0 to clickhouse02
    Then we got same clickhouse data at clickhouse01 clickhouse02

  Scenario: Copy collects missing dependencies and copies independent metadata
    Given we have executed queries on clickhouse01
    """
    CREATE DATABASE test_db;
    CREATE TABLE test_db.t (n UInt32) ENGINE = MergeTree PARTITION BY n ORDER BY n;
    INSERT INTO test_db.t VALUES (1), (2);
    """
    And we have created clickhouse01 clickhouse backup
    """
    name: base
    """
    And we have created clickhouse01 clickhouse backup
    """
    name: next
    """
    When we delete data objects of clickhouse01 backup "base"
    """
    min_objects: 2
    """
    And we try to execute command on clickhouse01
    """
    ch-backup -c /etc/yandex/ch-backup/ch-backup.conf copy next --keep-going \
        --target-config-parameter storage.credentials.bucket '"ch-backup-copy"' \
        --report-path /tmp/copy-report.json
    """
    Then command exits with code 1
    And failed copy report "/tmp/copy-report.json" on clickhouse01 has results
    """
    next:
      status: failed
      error_count: 2
      error_stage: planning
      copied_objects: true
    """
    And light metadata of clickhouse01 backup "next" in bucket "ch-backup-copy" has state "failed"

  @require_version_22.8
  Scenario: Copy keeps going within a metadata file and completes on retry
    Given we have executed queries on clickhouse01
    """
    CREATE DATABASE test_db;
    CREATE TABLE test_db.t (n UInt32) ENGINE = MergeTree ORDER BY n
        SETTINGS storage_policy = 's3';
    INSERT INTO test_db.t VALUES (1), (2);
    """
    And we have created clickhouse01 clickhouse backup
    """
    name: cloud
    """
    And ch-backup configuration on clickhouse02
    """
    storage:
        credentials:
            bucket: ch-backup-copy
    """
    When we put object in S3
    """
    bucket: cloud-storage-01
    path: data/keepgoing-probe
    data: abc
    """
    And we prepend object references to clickhouse01 backup "cloud" metadata
    """
    disk: s3
    objects:
      - {size: 1, key: ../invalid1}
      - {size: 3, key: keepgoing-probe, full_key: data/keepgoing-probe}
      - {size: 1, key: ../invalid2}
    """
    And we try to execute command on clickhouse01
    """
    ch-backup -c /etc/yandex/ch-backup/ch-backup.conf copy cloud --keep-going \
        --no-server-side-copy \
        --target-config-parameter storage.credentials.bucket '"ch-backup-copy"' \
        --target-cloud-storage-bucket cloud-storage-02 \
        --report-path /tmp/copy-report.json
    """
    Then command exits with code 1
    And failed copy report "/tmp/copy-report.json" on clickhouse01 has results
    """
    cloud:
      status: failed
      error_count: 2
      error_stage: planning
      error_sources_contain: [../invalid1, ../invalid2]
    """
    And an object in S3 contains
    """
    bucket: cloud-storage-02
    path: cloud/keepgoing-probe
    data: abc
    """
    And full metadata of clickhouse01 backup "cloud" in bucket "ch-backup-copy" has state "failed"
    When we restore saved cloud storage metadata
    And we execute command on clickhouse01
    """
    ch-backup -c /etc/yandex/ch-backup/ch-backup.conf copy cloud --keep-going \
        --no-server-side-copy \
        --target-config-parameter storage.credentials.bucket '"ch-backup-copy"' \
        --target-cloud-storage-bucket cloud-storage-02 \
        --report-path /tmp/copy-retry-report.json
    """
    Then we get response contains
    """
    cloud: copied
    """
    When we execute command on clickhouse01
    """
    python3 - <<'PY'
    import json
    with open('/tmp/copy-retry-report.json') as f:
        result = json.load(f)['backups'][0]
    assert result['objects_skipped'] > 0
    assert result['objects_failed'] == 0
    assert result['objects_copied'] + result['objects_skipped'] == result['objects_total']
    PY
    """
    And we restore clickhouse backup #0 to clickhouse02
    Then we got same clickhouse data at clickhouse01 clickhouse02

  @require_version_22.8
  Scenario: Copied backup with data on S3 disk doesn't depend on the source
    Given we have executed queries on clickhouse01
    """
    CREATE DATABASE IF NOT EXISTS test_db;
    CREATE TABLE test_db.table_01 (
        CounterID UInt32,
        UserID UInt32
    )
    ENGINE = MergeTree()
    PARTITION BY CounterID % 3
    ORDER BY UserID
    SETTINGS storage_policy = 's3';

    INSERT INTO test_db.table_01 SELECT 0, number FROM system.numbers LIMIT 10;
    INSERT INTO test_db.table_01 SELECT 1, number FROM system.numbers LIMIT 10;
    """
    And we have created clickhouse01 clickhouse backup
    """
    name: 12345678T123456
    """
    And ch-backup configuration on clickhouse02
    """
    storage:
        credentials:
            bucket: ch-backup-copy
    """
    And a keyless copy configuration "/tmp/keyless-cloud.conf" on clickhouse01
    """
    backup_name: 12345678T123456
    disk: s3
    source:
      bucket: cloud-storage-01
      path: data
    """
    When we execute command on clickhouse01
    """
    ch-backup -c /tmp/keyless-cloud.conf copy 12345678T123456 \
        --target-config-parameter storage.credentials.bucket '"ch-backup-copy"' \
        --target-cloud-storage-bucket cloud-storage-02 \
        --target-cloud-storage-path copy
    """
    Then we get response contains
    """
    12345678T123456: copied
    """
    When we execute query on clickhouse01
    """
    DROP TABLE test_db.table_01
    """
    And we delete clickhouse01 clickhouse backup #0
    Then s3 bucket cloud-storage-01 contains 0 objects
    When we restore clickhouse backup #0 to clickhouse02
    And we execute query on clickhouse02
    """
    SELECT count() FROM test_db.table_01
    """
    Then we get response
    """
    20
    """
    When we execute query on clickhouse02
    """
    SELECT sum(UserID) FROM test_db.table_01
    """
    Then we get response
    """
    90
    """

  @require_version_22.8
  Scenario: Copy backup with data on S3 disk and default backup name
    Given we have executed queries on clickhouse01
    """
    CREATE DATABASE IF NOT EXISTS test_db;
    CREATE TABLE test_db.table_01 (
        CounterID UInt32,
        UserID UInt32
    )
    ENGINE = MergeTree()
    PARTITION BY CounterID % 3
    ORDER BY UserID
    SETTINGS storage_policy = 's3';

    INSERT INTO test_db.table_01 SELECT 0, number FROM system.numbers LIMIT 10;
    INSERT INTO test_db.table_01 SELECT 1, number FROM system.numbers LIMIT 10;
    """
    And we have created clickhouse01 clickhouse backup
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
    And we restore clickhouse backup #0 to clickhouse02
    Then we got same clickhouse data at clickhouse01 clickhouse02

  @require_version_22.8
  Scenario: Copy backup with data on S3 disk by scopes
    Given we have executed queries on clickhouse01
    """
    CREATE DATABASE IF NOT EXISTS test_db;
    CREATE TABLE test_db.table_01 (
        CounterID UInt32,
        UserID UInt32
    )
    ENGINE = MergeTree()
    PARTITION BY CounterID % 3
    ORDER BY UserID
    SETTINGS storage_policy = 's3';

    INSERT INTO test_db.table_01 SELECT 0, number FROM system.numbers LIMIT 10;
    INSERT INTO test_db.table_01 SELECT 1, number FROM system.numbers LIMIT 10;
    """
    And we have created clickhouse01 clickhouse backup
    And ch-backup configuration on clickhouse02
    """
    storage:
        credentials:
            bucket: ch-backup-copy
    """
    When we execute command on clickhouse01
    """
    ch-backup -c /etc/yandex/ch-backup/ch-backup.conf copy LAST --scope files --server-side-copy \
        --target-config-parameter storage.credentials.bucket '"ch-backup-copy"'
    """
    Then we get response contains
    """
    scopes: files completed, cloud_storage pending
    """
    And we got the following backups on clickhouse02
      | num | state    |
      | 0   | creating |
    When we execute command on clickhouse01
    """
    ch-backup -c /etc/yandex/ch-backup/ch-backup.conf \
        --config-parameter storage.credentials.bucket '"missing-bucket"' \
        copy LAST --scope cloud_storage --keep-going \
        --no-server-side-copy \
        --target-config-parameter storage.credentials.bucket '"ch-backup-copy"' \
        --target-cloud-storage-bucket cloud-storage-02 \
        --target-cloud-storage-path copy
    """
    Then we got the following backups on clickhouse02
      | num | state   |
      | 0   | created |
    When we restore clickhouse backup #0 to clickhouse02
    Then we got same clickhouse data at clickhouse01 clickhouse02

  @require_version_22.8
  Scenario: Deleting copy of backup with data on S3 disk doesn't affect the source backup
    Given ch-backup configuration on clickhouse02
    """
    storage:
        credentials:
            bucket: ch-backup
    """
    And we have executed queries on clickhouse01
    """
    CREATE DATABASE IF NOT EXISTS test_db;
    CREATE TABLE test_db.table_01 (
        CounterID UInt32,
        UserID UInt32
    )
    ENGINE = MergeTree()
    PARTITION BY CounterID % 3
    ORDER BY UserID
    SETTINGS storage_policy = 's3';

    INSERT INTO test_db.table_01 SELECT 0, number FROM system.numbers LIMIT 10;
    INSERT INTO test_db.table_01 SELECT 1, number FROM system.numbers LIMIT 10;
    """
    And we have created clickhouse01 clickhouse backup
    """
    name: 12345678T123456
    """
    When we execute command on clickhouse01
    """
    ch-backup -c /etc/yandex/ch-backup/ch-backup.conf copy 12345678T123456 \
        --target-config-parameter storage.credentials.bucket '"ch-backup-copy"' \
        --target-cloud-storage-bucket cloud-storage-02 \
        --target-cloud-storage-path copy
    """
    And we execute command on clickhouse01
    """
    ch-backup -c /etc/yandex/ch-backup/ch-backup.conf \
        --config-parameter storage.credentials.bucket '"ch-backup-copy"' \
        delete 12345678T123456
    """
    Then s3 bucket ch-backup-copy contains no objects with prefix "ch_backup/12345678T123456/"
    And we got the following backups on clickhouse02
      | num | name            | state   |
      | 0   | 12345678T123456  | created |
    When we execute query on clickhouse01
    """
    DROP TABLE test_db.table_01
    """
    # Restore the original: deleting the copy must not unfreeze the source's data.
    And we restore clickhouse backup "12345678T123456" to clickhouse02
    """
    cloud_storage_source_bucket: 'cloud-storage-01'
    cloud_storage_source_path: 'data'
    """
    And we execute query on clickhouse02
    """
    SELECT count() FROM test_db.table_01
    """
    Then we get response
    """
    20
    """

  Scenario Outline: Copy respects the <held_lock> storage lock
    Given clickhouse on clickhouse01 has test schema
    And we have created clickhouse01 clickhouse backup
    """
    name: lock-copy
    schema_only: true
    """
    And ch-backup configuration on clickhouse01
    """
    lock:
        zk_flock: true
        zk_flock_path: /ch_backup/copy-source
        lock_timeout: 1
        exitcode: 71
    """
    And ch-backup configuration on clickhouse02
    """
    storage:
        credentials:
            bucket: ch-backup-copy
    lock:
        zk_flock: true
        zk_flock_path: /ch_backup/copy-target
    """
    When we acquire zookeeper lock on zookeeper01 with path /ch_backup/copy-<held_lock>
    And we try to execute command on clickhouse01
    """
    ch-backup -c /etc/yandex/ch-backup/ch-backup.conf copy lock-copy \
        --target-config-parameter storage.credentials.bucket '"ch-backup-copy"' \
        --target-config-parameter lock.zk_flock_path '"/ch_backup/copy-target"'
    """
    Then command exits with code 71
    And s3 bucket ch-backup-copy contains 0 objects
    When we release zookeeper lock on zookeeper01 with path /ch_backup/copy-<held_lock>
    And we execute command on clickhouse01
    """
    ch-backup -c /etc/yandex/ch-backup/ch-backup.conf copy lock-copy \
        --target-config-parameter storage.credentials.bucket '"ch-backup-copy"' \
        --target-config-parameter lock.zk_flock_path '"/ch_backup/copy-target"'
    """
    Then we got the following backups on clickhouse02
      | num | name      | state   |
      | 0   | lock-copy | created |
    When we delete clickhouse02 clickhouse backup "lock-copy"
    Then s3 bucket ch-backup-copy contains 0 objects

    Examples:
      | held_lock |
      | source    |
      | target    |

  @require_version_24.1
  Scenario Outline: Copy full cloud storage backups with <selection>
    Given we have executed queries on clickhouse01
    """
    CREATE DATABASE test_db;
    CREATE TABLE test_db.table_s3 (partition_id UInt32, n UInt32)
    ENGINE = MergeTree() PARTITION BY partition_id ORDER BY n
    SETTINGS storage_policy = 's3';
    INSERT INTO test_db.table_s3 SELECT 0, number FROM numbers(10);
    """
    And we have created clickhouse01 clickhouse backup
    """
    name: full-base
    copy_cloud_storage_data: true
    """
    And we have executed queries on clickhouse01
    """
    INSERT INTO test_db.table_s3 SELECT 1, number FROM numbers(10);
    """
    And we have created clickhouse01 clickhouse backup
    """
    name: full-next
    copy_cloud_storage_data: true
    """
    And ch-backup configuration on clickhouse02
    """
    storage:
        credentials:
            bucket: ch-backup-copy
    backup:
        path_root: copied/
    """
    Then we got the following backups on clickhouse01
      | num | state   | data_count | link_count |
      | 0   | created | 1          | 1          |
      | 1   | created | 1          | 0          |
    When we execute command on clickhouse01
    """
    ch-backup -c /etc/yandex/ch-backup/ch-backup.conf copy <selection> \
        --target-config-parameter storage.credentials.bucket '"ch-backup-copy"' \
        --target-config-parameter backup.path_root '"copied/"'
    """
    And we delete all objects in s3 bucket ch-backup
    And we delete all objects in s3 bucket cloud-storage-01
    And we restore clickhouse backup #0 to clickhouse02
    And we execute query on clickhouse02
    """
    SELECT count(), sum(n) FROM test_db.table_s3
    """
    Then we get response
    """
    20	90
    """
    When we delete clickhouse02 clickhouse backup #0
    Then s3 bucket ch-backup-copy contains no objects with prefix "copied/full_next/"
    And s3 bucket ch-backup-copy contains no objects with prefix "copied/full-next/"

    Examples:
      | selection |
      | LAST      |
      | --all     |

  @require_version_24.9
  Scenario: Keyless copy preserves encryption of data and private definitions
    Given we have executed queries on clickhouse01
    """
    CREATE DATABASE test_db;
    CREATE TABLE test_db.private_data (n UInt32 COMMENT 'PRIVATE_DDL_50977')
    ENGINE = MergeTree ORDER BY n;
    INSERT INTO test_db.private_data VALUES (1), (2);
    CREATE NAMED COLLECTION copy_secret AS password = 'PRIVATE_NC_50977';
    """
    And we have created clickhouse01 clickhouse backup
    """
    name: keyless
    """
    And ch-backup configuration on clickhouse02
    """
    storage:
      credentials:
        bucket: ch-backup-copy
    """
    Then private definitions in clickhouse01 backup "keyless" are encrypted
    """
    database: test_db
    table: private_data
    ddl_marker: PRIVATE_DDL_50977
    collection_marker: PRIVATE_NC_50977
    """
    Given a keyless copy configuration "/tmp/keyless-copy.conf" on clickhouse01
    When we execute command on clickhouse01
    """
    ch-backup -c /tmp/keyless-copy.conf copy keyless \
        --target-config-parameter storage.credentials.bucket '"ch-backup-copy"'
    """
    When we restore clickhouse backup #0 to clickhouse02
    Then clickhouse02 has same schema as clickhouse01
    And we got same clickhouse data at clickhouse01 clickhouse02
    And clickhouse01 has same named collections as clickhouse02
