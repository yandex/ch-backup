Feature: Parallel freeze

  Background:
    Given default configuration
    And a working s3
    And a working zookeeper on zookeeper01
    And a working clickhouse on clickhouse01
    And a working clickhouse on clickhouse02
    And clickhouse on clickhouse01 has test schema with 5 databases and 10 tables
    And clickhouse01 has test clickhouse data test1 with 5 databases, 10 tables, 100 rows and 5 partitions

  Scenario: Create backup with single freeze worker
    Given ch-backup configuration on clickhouse01
    """
    multiprocessing:
        freeze_threads: 1
    """
    When we create clickhouse01 clickhouse backup
    Then we got the following backups on clickhouse01
      | num | state    | data_count | link_count   | title         |
      | 0   | created  | 250        | 0            | shared        |
    When we restore clickhouse backup #0 to clickhouse02
    Then we got same clickhouse data at clickhouse01 clickhouse02

  Scenario: Create backup with default number of freeze workers
    When we create clickhouse01 clickhouse backup
    Then we got the following backups on clickhouse01
      | num | state    | data_count | link_count   | title         |
      | 0   | created  | 250        | 0            | shared        |
    When we restore clickhouse backup #0 to clickhouse02
    Then we got same clickhouse data at clickhouse01 clickhouse02

  Scenario: Skip a table replaced while it is being frozen
    Given ch-backup configuration on clickhouse01
    """
    multiprocessing:
        freeze_threads: 1
        freeze_partition_threads: 1
        parallelize_freeze_in_clickhouse: false
    """
    When we execute queries on clickhouse01
    """
    DROP TABLE test_db_01.test_table_10;
    SELECT sleep(1);
    CREATE TABLE test_db_01.test_table_10 (
        date Date,
        datetime DateTime,
        int_num UInt32,
        prefix String,
        str String
    ) ENGINE = MergeTree
    PARTITION BY (date, prefix)
    ORDER BY int_num;
    CREATE TABLE test_db_01.test_table_replacement (
        date Date,
        datetime DateTime,
        int_num UInt32,
        prefix String,
        str String
    ) ENGINE = Memory;
    INSERT INTO test_db_01.test_table_01
    SELECT
        toDate('2020-01-01') + number,
        toDateTime('2020-01-01 00:00:00') + number,
        toUInt32(number),
        'freeze_barrier',
        toString(number)
    FROM numbers(100);
    """
    When we start creating clickhouse01 clickhouse backup in background
    """
    tables:
      - test_db_01.test_table_01
      - test_db_01.test_table_10
    """
    And we wait until clickhouse01 starts freezing table "test_db_01"."test_table_01"
    And we execute query on clickhouse01
    """
    EXCHANGE TABLES test_db_01.test_table_10 AND test_db_01.test_table_replacement
    """
    And we wait for background clickhouse01 clickhouse backup
    Then we got the following backups on clickhouse01
      | num | state   | title  |
      | 0   | created | shared |
    And backup #0 on clickhouse01 contains table "test_db_01"."test_table_01"
    And backup #0 on clickhouse01 does not contain table "test_db_01"."test_table_10"
    When we restore clickhouse backup #0 to clickhouse02
    And we execute query on clickhouse02
    """
    SELECT countIf(prefix = 'freeze_barrier') FROM test_db_01.test_table_01;
    """
    Then we get response
    """
    100
    """
