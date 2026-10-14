Feature: Copy backups to another storage

  Background:
    Given default configuration
    And a working s3
    And a working zookeeper on zookeeper01
    And a working clickhouse on clickhouse01
    And a working clickhouse on clickhouse02

  Scenario Outline: Copy backup to another bucket
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
    ch-backup -c /etc/yandex/ch-backup/ch-backup.conf copy LAST <options> \
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
    ch-backup -c /etc/yandex/ch-backup/ch-backup.conf copy LAST <options> \
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

  Scenario: Copy reuses a materialized part after its original owner fails
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
    name: b_materialized
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
    When we try to execute command on clickhouse01
    """
    python3 - <<'PY'
    from unittest.mock import patch
    from ch_backup.backup.layout import BackupLayout
    from ch_backup.backup.metadata import BackupState
    from ch_backup.cli import cli
    upload = BackupLayout.upload_backup_metadata
    def fail_owner(layout, backup, *args, **kwargs):
        if backup.name == 'a_owner' and backup.state == BackupState.CREATED:
            raise RuntimeError('injected owner publication failure')
        return upload(layout, backup, *args, **kwargs)
    with patch.object(BackupLayout, 'upload_backup_metadata', fail_owner):
        cli(['-c', '/etc/yandex/ch-backup/ch-backup.conf',
             '--config-parameter', 'multiprocessing.workers', '0',
             'copy', '--all', '--keep-going',
             '--target-config-parameter', 'storage.credentials.bucket', '"ch-backup-copy"',
             '--report-path', '/tmp/copy-report.json'])
    PY
    """
    Then command exits with code 1
    And we got the following backups on clickhouse02
      | num | state   | data_count | link_count |
      | 0   | created | 0          | 1          |
      | 1   | created | 1          | 0          |
      | 2   | failed  | 1          | 0          |
    When we execute command on clickhouse02
    """
    python3 - <<'PY'
    from ch_backup.backup.layout import BackupLayout
    from ch_backup.config import Config
    cfg = Config('/etc/yandex/ch-backup/ch-backup.conf')
    cfg['multiprocessing']['workers'] = 0
    part = BackupLayout(cfg).get_backup('c_linked').get_parts()[0]
    assert part.link == 'b_materialized', part.link
    PY
    """
    And we restore clickhouse backup #0 to clickhouse02
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
    When we execute command on clickhouse01
    """
    python3 - <<'PY'
    import json
    with open('/tmp/copy-report.json') as f:
        report = json.load(f)
    assert report['schema_version'] == 1
    assert report['error']
    assert {b['name']: b['status'] for b in report['backups']} == {
        'first': 'copied', 'conflicting': 'failed', 'last': 'copied'}
    for backup in report['backups']:
        if backup['status'] == 'copied':
            assert backup['objects_total'] == backup['objects_copied']
            assert backup['objects_failed'] == 0
    PY
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
    When we execute command on clickhouse01
    """
    python3 - <<'PY'
    import json
    with open('/tmp/copy-report.json') as f:
        report = json.load(f)
    assert {b['name']: b['status'] for b in report['backups']} == {
        'first': 'copied', 'broken': 'failed', 'last': 'copied'}
    broken = next(b for b in report['backups'] if b['name'] == 'broken')
    assert broken['objects_total'] is None
    assert broken['errors'][0]['stage'] == 'selection'
    PY
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
    When we execute command on clickhouse01
    """
    python3 - <<'PY'
    import json
    with open('/tmp/copy-report.json') as f:
        report = json.load(f)
    assert {b['name']: b['status'] for b in report['backups']} == {
        'a_cloud': 'failed', 'b_local': 'copied'}
    cloud = next(b for b in report['backups'] if b['name'] == 'a_cloud')
    assert cloud['objects_failed'] > 0
    assert cloud['objects_copied'] > 0
    assert cloud['objects_total'] == cloud['objects_copied'] + cloud['objects_failed']
    assert all(e['stage'] == 'copy' and e['source'] for e in cloud['errors'])
    PY
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
    When we execute command on clickhouse01
    """
    python3 - <<'PY'
    import json
    with open('/tmp/copy-report.json') as f:
        report = json.load(f)
    assert {b['name']: b['status'] for b in report['backups']} == {
        'first': 'copied', 'broken': 'failed', 'last': 'copied'}
    broken = next(b for b in report['backups'] if b['name'] == 'broken')
    assert broken['errors'][0]['stage'] == 'target_selection'
    PY
    """

  Scenario: Copy reports a missing named backup
    When we try to execute command on clickhouse01
    """
    ch-backup -c /etc/yandex/ch-backup/ch-backup.conf copy missing --keep-going \
        --target-config-parameter storage.credentials.bucket '"ch-backup-copy"' \
        --report-path /tmp/copy-report.json
    """
    Then command exits with code 1
    When we execute command on clickhouse01
    """
    python3 - <<'PY'
    import json
    with open('/tmp/copy-report.json') as f:
        report = json.load(f)
    assert [(b['name'], b['status']) for b in report['backups']] == [('missing', 'failed')]
    assert report['backups'][0]['errors'][0]['stage'] == 'selection'
    PY
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
    When we execute command on clickhouse01
    """
    python3 - <<'PY'
    from ch_backup.config import Config
    from ch_backup.storage.engine import get_storage_engine
    cfg = Config('/etc/yandex/ch-backup/ch-backup.conf')
    storage = get_storage_engine(cfg['storage'])
    paths = storage.list_dir('ch_backup/base/data', recursive=True, absolute=True)
    assert len(paths) >= 2
    storage.delete_files(paths)
    PY
    """
    And we try to execute command on clickhouse01
    """
    ch-backup -c /etc/yandex/ch-backup/ch-backup.conf copy next --keep-going \
        --target-config-parameter storage.credentials.bucket '"ch-backup-copy"' \
        --report-path /tmp/copy-report.json
    """
    Then command exits with code 1
    When we execute command on clickhouse01
    """
    python3 - <<'PY'
    import json
    from ch_backup.backup.layout import BackupLayout
    from ch_backup.backup.metadata import BackupState
    from ch_backup.config import Config
    with open('/tmp/copy-report.json') as f:
        report = json.load(f)
    backup = report['backups'][0]
    assert backup['name'] == 'next' and backup['status'] == 'failed'
    assert len(backup['errors']) == 2
    assert all(e['stage'] == 'planning' for e in backup['errors'])
    assert backup['objects_copied'] > 0
    cfg = Config('/etc/yandex/ch-backup/ch-backup.conf')
    cfg['storage']['credentials']['bucket'] = 'ch-backup-copy'
    cfg['multiprocessing']['workers'] = 0
    assert BackupLayout(cfg).get_backup('next', use_light_meta=True).state == BackupState.FAILED
    PY
    """

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
    When we execute command on clickhouse01
    """
    python3 - <<'PY'
    import base64
    import copy
    import json
    from ch_backup.backup.layout import BackupLayout
    from ch_backup.clickhouse.object_storage_metadata import parse_object_storage_metadata
    from ch_backup.config import Config
    from ch_backup.storage.engine import get_storage_engine
    cfg = Config('/etc/yandex/ch-backup/ch-backup.conf')
    cfg['multiprocessing']['workers'] = 0
    layout = BackupLayout(cfg)
    backup = layout.get_backup('cloud')
    path = layout.get_cloud_storage_metadata_remote_paths(
        'cloud', 's3', backup.cloud_storage.compressed, 'all')[0]
    files = layout.get_cloud_storage_metadata_files(backup, path)
    with open('/tmp/copy-original-metadata.json', 'w') as f:
        json.dump([path, [(name, base64.b64encode(data).decode()) for name, data in files]], f)
    for index, (name, data) in enumerate(files):
        if name.endswith('frozen_metadata.txt'):
            continue
        metadata = parse_object_storage_metadata(data)
        lines = data.decode().split('\n')
        count, size = map(int, lines[1].split('\t'))
        key = 'data/keepgoing-probe' if metadata.full_object_keys else 'keepgoing-probe'
        lines[1] = f'{count + 3}\t{size + 5}'
        lines[2:2] = ['1\t../invalid1', f'3\t{key}', '1\t../invalid2']
        files[index] = (name, '\n'.join(lines).encode())
        break
    else:
        raise AssertionError('No object metadata found')
    storage_config = copy.deepcopy(cfg['storage'])
    storage_config['credentials']['bucket'] = 'cloud-storage-01'
    get_storage_engine(storage_config).upload_data(b'abc', 'data/keepgoing-probe')
    layout.upload_cloud_storage_metadata_files(backup, path, files)
    layout.wait()
    PY
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
    When we execute command on clickhouse01
    """
    python3 - <<'PY'
    import base64
    import copy
    import json
    from ch_backup.backup.layout import BackupLayout
    from ch_backup.backup.metadata import BackupState
    from ch_backup.config import Config
    from ch_backup.storage.engine import get_storage_engine
    cfg = Config('/etc/yandex/ch-backup/ch-backup.conf')
    cfg['multiprocessing']['workers'] = 0
    with open('/tmp/copy-report.json') as f:
        result = json.load(f)['backups'][0]
    assert result['status'] == 'failed'
    assert len(result['errors']) == 2
    assert all(e['stage'] == 'planning' for e in result['errors'])
    assert all(f'../invalid{i}' in result['errors'][i - 1]['source'] for i in (1, 2))
    storage_config = copy.deepcopy(cfg['storage'])
    storage_config['credentials']['bucket'] = 'cloud-storage-02'
    assert get_storage_engine(storage_config).download_data('cloud/keepgoing-probe') == b'abc'
    target_cfg = cfg.derive()
    target_cfg['storage']['credentials']['bucket'] = 'ch-backup-copy'
    assert BackupLayout(target_cfg).get_backup('cloud').state == BackupState.FAILED
    layout = BackupLayout(cfg)
    with open('/tmp/copy-original-metadata.json') as f:
        path, files = json.load(f)
    layout.upload_cloud_storage_metadata_files(layout.get_backup('cloud'), path,
        [(name, base64.b64decode(data)) for name, data in files])
    layout.wait()
    PY
    """
    And we execute command on clickhouse01
    """
    ch-backup -c /etc/yandex/ch-backup/ch-backup.conf copy cloud --keep-going \
        --no-server-side-copy \
        --target-config-parameter storage.credentials.bucket '"ch-backup-copy"' \
        --target-cloud-storage-bucket cloud-storage-02
    """
    Then we get response contains
    """
    cloud: copied
    """
    When we restore clickhouse backup #0 to clickhouse02
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
    When we execute command on clickhouse01
    """
    ch-backup -c /etc/yandex/ch-backup/ch-backup.conf copy 12345678T123456 \
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
