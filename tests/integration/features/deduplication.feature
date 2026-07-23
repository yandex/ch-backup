Feature: Deduplication

  Background:
    Given default configuration
    And a working s3
    And a working zookeeper on zookeeper01
    And a working clickhouse on clickhouse01
    And a working clickhouse on clickhouse02
    And we have executed queries on clickhouse01
    """
    CREATE DATABASE test_db1;

    CREATE TABLE test_db1.test_table1 (partition_id Int32, n Int32)
    ENGINE = MergeTree() PARTITION BY partition_id ORDER BY (partition_id, n);

    INSERT INTO test_db1.test_table1 SELECT number % 2, number FROM system.numbers LIMIT 100;
    """
    And ch-backup configuration on clickhouse01
    """
    backup:
        deduplicate_parts: True
    """
    And we have created clickhouse01 clickhouse backup
    And we have executed queries on clickhouse01
    """
    CREATE TABLE test_db1.test_table2 (partition_id Int32, n Int32)
    ENGINE = MergeTree() PARTITION BY partition_id ORDER BY (partition_id, n);

    INSERT INTO test_db1.test_table2 SELECT number % 3, number FROM system.numbers LIMIT 100;
    """

  Scenario: Create backup with enabled deduplication
    When we create clickhouse01 clickhouse backup
    Then we got the following backups on clickhouse01
      | num | state    | data_count | link_count   |
      | 0   | created  | 3          | 2            |
      | 1   | created  | 2          | 0            |
    When we restore clickhouse backup #0 to clickhouse02
    Then we got same clickhouse data at clickhouse01 clickhouse02

  Scenario: Create backup with disabled deduplication
    Given ch-backup configuration on clickhouse01
    """
    backup:
        deduplicate_parts: False
    """
    When we create clickhouse01 clickhouse backup
    Then we got the following backups on clickhouse01
      | num | state    | data_count | link_count   |
      | 0   | created  | 5          | 0            |
      | 1   | created  | 2          | 0            |
    When we restore clickhouse backup #0 to clickhouse02
    Then we got same clickhouse data at clickhouse01 clickhouse02

    Scenario: Create backup with deduplication in multiple batches
    Given ch-backup configuration on clickhouse01
    """
    backup:
        deduplication_batch_size: 10
    """
    And ch-backup configuration on clickhouse02
    """
    backup:
        deduplication_batch_size: 10
    """
    And we have executed queries on clickhouse01
    """
    CREATE TABLE test_db1.test_table_batch (partition_id Int32, n Int32)
    ENGINE = MergeTree() PARTITION BY partition_id ORDER BY (partition_id, n);

    INSERT INTO test_db1.test_table_batch SELECT number, -1 * number FROM system.numbers LIMIT 35;
    """
    When we create clickhouse01 clickhouse backup
    Then we got the following backups on clickhouse01
      | num | state    | data_count | link_count   |
      | 0   | created  | 38         | 2            |
      | 1   | created  | 2          | 0            |
    When we restore clickhouse backup #0 to clickhouse02
    Then we got same clickhouse data at clickhouse01 clickhouse02

    Given we have executed queries on clickhouse01
    """
    INSERT INTO test_db1.test_table_batch SELECT number + 50, number FROM system.numbers LIMIT 15;
    """
    When we create clickhouse01 clickhouse backup
    Then we got the following backups on clickhouse01
      | num | state    | data_count | link_count   |
      | 0   | created  | 15         | 40           |
      | 1   | created  | 38         | 2            |
      | 2   | created  | 2          | 0            |
    When we restore clickhouse backup #0 to clickhouse02
    Then we got same clickhouse data at clickhouse01 clickhouse02

  Scenario Outline: Failed backups are used in deduplication
    Given we have executed queries on clickhouse01
    """
    CREATE TABLE test_db1.test_table3 (partition_id Int32, n Int32)
    ENGINE = MergeTree() PARTITION BY partition_id ORDER BY (partition_id, n)
    SETTINGS min_bytes_for_wide_part=0;

    INSERT INTO test_db1.test_table3 SELECT number % 2, number FROM system.numbers LIMIT 100;
    """
    When we create clickhouse01 clickhouse backup
    Given metadata of clickhouse01 backup #0 was adjusted with
    """
    meta:
        state: <state>
    """
    And file "data/test_db1/test_table3/0_1_1_0/0_1_1_0.tar" was deleted from clickhouse01 backup #0
    When we create clickhouse01 clickhouse backup
    Then we got the following backups on clickhouse01
      | num | state    | data_count | link_count   |
      | 0   | created  | 1          | 6            |
      | 1   | <state>  | 5          | 2            |
      | 2   | created  | 2          | 0            |
    When we restore clickhouse backup #0 to clickhouse02
    Then we got same clickhouse data at clickhouse01 clickhouse02

    Examples:
      | state             |
      | creating          |
      | failed            |

  Scenario Outline: Broken parts are not used in deduplication
    Given we have executed queries on clickhouse01
    """
    CREATE TABLE test_db1.test_table3 (partition_id Int32, n Int32)
    ENGINE = MergeTree() PARTITION BY partition_id ORDER BY (partition_id, n)
    SETTINGS min_bytes_for_wide_part=0;

    INSERT INTO test_db1.test_table3 SELECT number % 2, number FROM system.numbers LIMIT 100;
    """
    When we create clickhouse01 clickhouse backup
    Given metadata of clickhouse01 backup #0 was adjusted with
    """
    meta:
        state: <state>
    """
    And file "data/test_db1/test_table3/0_1_1_0/0_1_1_0.tar" in clickhouse01 backup #0 is empty
    When we create clickhouse01 clickhouse backup
    Then we got the following backups on clickhouse01
      | num | state    | data_count | link_count   |
      | 0   | created  | 1          | 6            |
      | 1   | <state>  | 5          | 2            |
      | 2   | created  | 2          | 0            |
    When we restore clickhouse backup #0 to clickhouse02
    Then we got same clickhouse data at clickhouse01 clickhouse02

    Examples:
      | state             |
      | creating          |
      | failed            |

  Scenario Outline: Deleting or partially deleted backups are used in deduplication
    When we create clickhouse01 clickhouse backup
    Then we got the following backups on clickhouse01
      | num | state    | data_count | link_count   |
      | 0   | created  | 3          | 2            |
      | 1   | created  | 2          | 0            |
    When metadata of clickhouse01 backup #1 was adjusted with
    """
    meta:
        state: <state>
    """
    Then we got the following backups on clickhouse01
      | num | state    | data_count | link_count   |
      | 0   | created  | 3          | 2            |
      | 1   | <state>  | 2          | 0            |
    When we create clickhouse01 clickhouse backup
    Then we got the following backups on clickhouse01
      | num | state    | data_count | link_count   |
      | 0   | created  | 0          | 5            |
      | 1   | created  | 3          | 2            |
      | 2   | <state>  | 2          | 0            |
    When we restore clickhouse backup #0 to clickhouse02
    Then we got same clickhouse data at clickhouse01 clickhouse02

    Examples:
      | state             |
      | partially_deleted |
      | deleting          |

  Scenario: Backups mismatched age policy are not used in deduplication
    Given ch-backup configuration on clickhouse01
    """
    backup:
        deduplication_age_limit:
            days: 7
    """
    Given metadata of clickhouse01 backup #0 was adjusted with
    """
    meta:
        state: created
        start_time: {{ backup.start_time | decrease_on('7 days') }}
        end_time: {{ backup.end_time | decrease_on('7 days') }}
    """
    When we create clickhouse01 clickhouse backup
    Then we got the following backups on clickhouse01
      | num | state    | data_count | link_count   |
      | 0   | created  | 5          | 0            |
      | 1   | created  | 2          | 0            |
    When we restore clickhouse backup #0 to clickhouse02
    Then we got same clickhouse data at clickhouse01 clickhouse02

  Scenario: Mutated part is deduplicated
    # Remove data and backups created in Background
    When we drop all databases at clickhouse01
    And we delete clickhouse01 clickhouse backup #0
    Given we have executed queries on clickhouse01
    """
    CREATE DATABASE test_db1;
    CREATE TABLE test_db1.test_table_mut2 (id Int32, val Int32)
    ENGINE = MergeTree() ORDER BY id
    SETTINGS min_bytes_for_wide_part=0;

    INSERT INTO test_db1.test_table_mut2 VALUES (1, 10), (2, 20), (3, 30);
    """
    When we create clickhouse01 clickhouse backup
    When we execute query on clickhouse01
    """
    SELECT name FROM system.parts WHERE database='test_db1' AND table='test_table_mut2' AND active FORMAT TabSeparated
    """
    Then we get response
    """
    all_1_1_0
    """
    When we execute queries on clickhouse01
    """
    ALTER TABLE test_db1.test_table_mut2 DELETE WHERE id < 0
    """
    And all mutations on clickhouse01 are done
    And we execute query on clickhouse01
    """
    SELECT name LIKE 'all_1_1_0_%' FROM system.parts WHERE database='test_db1' AND table='test_table_mut2' AND active FORMAT TabSeparated
    """
    Then we get response
    """
    1
    """
    When we create clickhouse01 clickhouse backup
    When we execute queries on clickhouse01
    """
    ALTER TABLE test_db1.test_table_mut2 DELETE WHERE id < 0
    """
    And all mutations on clickhouse01 are done
    And we execute query on clickhouse01
    """
    SELECT name LIKE 'all_1_1_0_%' FROM system.parts WHERE database='test_db1' AND table='test_table_mut2' AND active FORMAT TabSeparated
    """
    Then we get response
    """
    1
    """
    When we create clickhouse01 clickhouse backup
    Then we got the following backups on clickhouse01
      | num | state   | data_count | link_count |
      | 0   | created | 0          | 1          |
      | 1   | created | 0          | 1          |
      | 2   | created | 1          | 0          |
    When we restore clickhouse backup #0 to clickhouse02
    Then we got same clickhouse data at clickhouse01 clickhouse02

  Scenario: Mutated deduplicated part is kept after backup deletion
    # Remove data and backups created in Background
    When we drop all databases at clickhouse01
    And we delete clickhouse01 clickhouse backup #0
    Given we have executed queries on clickhouse01
    """
    CREATE DATABASE test_db1;
    CREATE TABLE test_db1.test_table_mut3 (id Int32, val Int32)
    ENGINE = MergeTree() ORDER BY id
    SETTINGS min_bytes_for_wide_part=0;

    INSERT INTO test_db1.test_table_mut3 VALUES (1, 10), (2, 20), (3, 30);
    """
    # Backup #0 — original data (part all_1_1_0)
    When we create clickhouse01 clickhouse backup
    # Mutate: trigger a mutation that renames the part (all_1_1_0 → all_1_1_0_X)
    When we execute queries on clickhouse01
    """
    ALTER TABLE test_db1.test_table_mut3 DELETE WHERE id < 0
    """
    And all mutations on clickhouse01 are done
    # Backup #1 — deduplicated, links to backup #0 with link_part_name=all_1_1_0
    When we create clickhouse01 clickhouse backup
    # Mutate again so backup #2 also deduplicates to backup #0
    When we execute queries on clickhouse01
    """
    ALTER TABLE test_db1.test_table_mut3 DELETE WHERE id < 0
    """
    And all mutations on clickhouse01 are done
    # Backup #2 — also deduplicated, links to backup #0 with link_part_name=all_1_1_0
    When we create clickhouse01 clickhouse backup
    Then we got the following backups on clickhouse01
      | num | state   | data_count | link_count |
      | 0   | created | 0          | 1          |
      | 1   | created | 0          | 1          |
      | 2   | created | 1          | 0          |
    # Delete backup #1 — an intermediate backup that references backup #0
    # The mutated part data must be preserved because backup #2 still needs it
    When we delete clickhouse01 clickhouse backup #1
    Then we got the following backups on clickhouse01
      | num | state    | data_count | link_count |
      | 0   | created  | 0          | 1          |
      | 1   | created  | 1          | 0          |
    When we delete clickhouse01 clickhouse backup #1
    Then we got the following backups on clickhouse01
      | num | state              | data_count | link_count |
      | 0   | created            | 0          | 1          |
      | 1   | partially_deleted  | 1          | 0          |
    # Restore backup #2 to clickhouse02 and verify data integrity
    When we restore clickhouse backup #0 to clickhouse02
    Then we got same clickhouse data at clickhouse01 clickhouse02
