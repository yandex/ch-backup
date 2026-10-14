@backup_keep_going
Feature: Backup of a table with an unreadable data part

  Background:
    Given default configuration
    And a working s3
    And a working zookeeper on zookeeper01
    And a working clickhouse on clickhouse01
    And a working clickhouse on clickhouse02
    And ch-backup configuration on clickhouse01
    """
    multiprocessing:
        workers: 4
    """
    And we have executed queries on clickhouse01
    """
    CREATE DATABASE test_db;
    CREATE TABLE test_db.test_table (partition_id Int32, n Int32)
    ENGINE = MergeTree() PARTITION BY partition_id ORDER BY (partition_id, n);
    INSERT INTO test_db.test_table SELECT 0, number FROM system.numbers LIMIT 100;
    INSERT INTO test_db.test_table SELECT 1, number FROM system.numbers LIMIT 100;
    CREATE TABLE test_db.intact_table (n Int32) ENGINE = MergeTree() ORDER BY n;
    INSERT INTO test_db.intact_table SELECT number FROM system.numbers LIMIT 100;
    """
    And we have executed command on clickhouse01
    """
    set -e
    part=/var/lib/clickhouse/data/test_db/test_table/1_2_2_0
    target=$(find /sys/module -maxdepth 2 -name uevent -perm 0200 | head -n 1)
    test -n "$target"
    if cat "$target" > /dev/null 2>&1; then exit 1; fi
    for file in "$part"/*.bin; do
        ln -sfn "$target" "$file"
        chown -h --reference="$part" "$file"
    done
    """

  Scenario: Unreadable data part is left out of the backup with keep_going
    Given ch-backup configuration on clickhouse01
    """
    backup:
        keep_going: true
    """
    When we create clickhouse01 clickhouse backup
    Then we got the following backups on clickhouse01
      | num | state   |
      | 0   | created |
    And metadata of clickhouse01 backup #0 contains
    """
    incomplete_tables: 1
    """
    And metadata of clickhouse01 backup #0 contains no
    """
    data_skipped_tables: 1
    """
    And metadata of clickhouse01 backup #0 for "test_db"."test_table" contains
    """
    skipped_parts: ["1_2_2_0"]
    """
    When we execute command on clickhouse01
    """
    ch-backup -c /etc/yandex/ch-backup/ch-backup.conf list -v --format json
    """
    Then we get response contains
    """
    "incomplete_tables": 1
    """
    When we restore clickhouse backup #0 to clickhouse02
    And we execute query on clickhouse02
    """
    SELECT count() FROM test_db.test_table WHERE partition_id = 0;
    """
    Then we get response
    """
    100
    """
    When we execute query on clickhouse02
    """
    SELECT count() FROM test_db.test_table WHERE partition_id = 1;
    """
    Then we get response
    """
    0
    """
    When we execute query on clickhouse02
    """
    SELECT count() FROM test_db.intact_table;
    """
    Then we get response
    """
    100
    """

  Scenario: Unreadable data part fails the backup by default
    Given ch-backup configuration on clickhouse01
    """
    backup:
        keep_going: false
    """
    When we try to execute command on clickhouse01
    """
    ch-backup -c /etc/yandex/ch-backup/ch-backup.conf backup --name failed_backup
    """
    Then command exits with code 1
    And we got the following backups on clickhouse01
      | num | name          | state  |
      | 0   | failed_backup | failed |
