@dependent-scenarios
Feature: Backup & Restore

  Background:
    Given default configuration
    And a working s3
    And a working zookeeper on zookeeper01
    And a working clickhouse on clickhouse01
    And a working clickhouse on clickhouse02
    And clickhouse on clickhouse01 has test schema

  Scenario: Create backup
    Given we have executed queries on clickhouse01
    """
    CREATE TABLE test_db_01.table_rus (
        EventDate DateTime,
        CounterID UInt32,
        `Пользователь` UInt32
    )
    ENGINE = MergeTree()
    PARTITION BY CounterID % 10
    ORDER BY (CounterID, EventDate)
    """
    And clickhouse01 has test clickhouse data test1
    When we create clickhouse01 clickhouse backup
    Then we got the following backups on clickhouse01
      | num | state    | data_count | link_count   | title         |
      | 0   | created  | 4          | 0            | data          |

  Scenario: Create backup with increments
    Given clickhouse01 has test clickhouse data test2
    When we create clickhouse01 clickhouse backup
    Then we got the following backups on clickhouse01
      | num | state    | data_count | link_count   | title         |
      | 0   | created  | 4          | 4            | data+links    |
      | 1   | created  | 4          | 0            | shared        |

  Scenario: Restore from backup
    When we restore clickhouse backup #0 to clickhouse02
    Then we got same clickhouse data at clickhouse01 clickhouse02

  Scenario: Restore schema from backup without data
    When we drop all databases at clickhouse02
    And we restore clickhouse backup #1 to clickhouse02
    """
    schema_only: true
    """
    Then clickhouse02 has same schema as clickhouse01
    But on clickhouse02 tables are empty

  Scenario: Create a "schema-only"
    When we create clickhouse01 clickhouse backup
    """
    schema_only: True
    """
    Then we got the following backups on clickhouse01
      | num | state    | data_count | link_count   | title         |
      | 0   | created  | 0          | 0            | schema-only   |
      | 1   | created  | 4          | 4            | data+links    |
      | 2   | created  | 4          | 0            | shared        |

  Scenario: Restore from "schema-only" backup
    When we drop all databases at clickhouse02
    And we restore clickhouse backup #0 to clickhouse02
    Then clickhouse02 has same schema as clickhouse01
    But on clickhouse02 tables are empty

  Scenario: Restore from "schema-only" backup without data
    When we drop all databases at clickhouse02
    And we restore clickhouse backup #0 to clickhouse02
    """
    schema_only: true
    """
    Then clickhouse02 has same schema as clickhouse01
    But on clickhouse02 tables are empty

  Scenario: Backup & Restore with multipart upload/download
    When we drop all databases at clickhouse01
    And we drop all databases at clickhouse02
    Given we have executed queries on clickhouse01
    """
    CREATE DATABASE test_db;
    CREATE TABLE test_db.table_01 (n Int32) ENGINE = MergeTree() PARTITION BY n % 1 ORDER BY n;
    INSERT INTO test_db.table_01 SELECT number FROM system.numbers LIMIT 10000000;
    """
    When we create clickhouse01 clickhouse backup
    And we restore clickhouse backup #0 to clickhouse02
    Then we got same clickhouse data at clickhouse01 clickhouse02

  Scenario: Backup & Restore for ReplacingMergeTree
    When we drop all databases at clickhouse01
    And we drop all databases at clickhouse02
    Given we have executed queries on clickhouse01
    """
    CREATE DATABASE test_db;
    CREATE TABLE test_db.hits (
        id UInt32,
        url String,
        visits UInt32
    )
    ENGINE ReplacingMergeTree
    ORDER BY id;
    INSERT INTO test_db.hits VALUES
      (1, '/index', 100);
    """
    Given we have executed queries on clickhouse01
    """
    INSERT INTO test_db.hits VALUES
      (1, '/index', 101);
    """
    Given we have executed queries on clickhouse01
    """
    INSERT INTO test_db.hits VALUES
      (1, '/index', 102);
    """
    Given we have executed queries on clickhouse01
    """
    INSERT INTO test_db.hits VALUES
      (1, '/index', 103);
    """
    Given we have executed queries on clickhouse01
    """
    INSERT INTO test_db.hits VALUES
      (1, '/index', 104);
    """
    Given we have executed queries on clickhouse01
    """
    INSERT INTO test_db.hits VALUES
      (1, '/index', 105);
    """
    Given we have executed queries on clickhouse01
    """
    INSERT INTO test_db.hits VALUES
      (1, '/index', 106);
    """
    When we create clickhouse01 clickhouse backup
    And we restore clickhouse backup #0 to clickhouse02
    When we execute query on clickhouse01
    """
    SELECT id, visits FROM test_db.hits FINAL ORDER BY id FORMAT Vertical;
    """
    Then we get response
    """
    Row 1:
    ──────
    id:     1
    visits: 106
    """
    When we execute query on clickhouse02
    """
    SELECT id, visits FROM test_db.hits FINAL ORDER BY id FORMAT Vertical;
    """
    Then we get response
    """
    Row 1:
    ──────
    id:     1
    visits: 106
    """
    Then we got same clickhouse data at clickhouse01 clickhouse02

  Scenario: Backup & Restore with long file names
    When we drop all databases at clickhouse01
    And we drop all databases at clickhouse02
    Given we have executed queries on clickhouse01
    """
    CREATE DATABASE test_db;
    CREATE TABLE test_db.table_long (very_very_long_name_more_than_100_characters_very_very_long_name_more_than_100_characters_very_very_long_name_more_than_100_characters Int32)
      ENGINE = MergeTree()
      PARTITION BY very_very_long_name_more_than_100_characters_very_very_long_name_more_than_100_characters_very_very_long_name_more_than_100_characters % 1
      ORDER BY very_very_long_name_more_than_100_characters_very_very_long_name_more_than_100_characters_very_very_long_name_more_than_100_characters;
    INSERT INTO test_db.table_long SELECT number FROM system.numbers LIMIT 50;
    """
    When we create clickhouse01 clickhouse backup
    And we restore clickhouse backup #0 to clickhouse02
    Then we got same clickhouse data at clickhouse01 clickhouse02

  Scenario: Backup & Restore with non-ascii file names
    When we drop all databases at clickhouse01
    And we drop all databases at clickhouse02
    Given we have executed queries on clickhouse01
    """
    CREATE DATABASE `тест-дб`;
    CREATE TABLE `тест-дб`.`тест-тбл` (`ключ` Int32)
      ENGINE = MergeTree()
      PARTITION BY `ключ` % 1
      ORDER BY `ключ`;
    INSERT INTO `тест-дб`.`тест-тбл` SELECT number FROM system.numbers LIMIT 50;
    """
    When we create clickhouse01 clickhouse backup
    And we restore clickhouse backup #0 to clickhouse02
    Then we got same clickhouse data at clickhouse01 clickhouse02

  Scenario: Restore of a backup with a corrupted data part
    When we drop all databases at clickhouse01
    And we drop all databases at clickhouse02    
    And we execute queries on clickhouse01
    """
    CREATE DATABASE test_db;
    CREATE TABLE test_db.test_table (partition_id Int32, n Int32)
    ENGINE MergeTree PARTITION BY partition_id ORDER BY (partition_id, n);

    INSERT INTO test_db.test_table SELECT number % 2, number FROM system.numbers LIMIT 100;
    """
    And we create clickhouse01 clickhouse backup
    Given ch-backup configuration on clickhouse02
    """
    backup:
        restore_fail_on_attach_error: False
    """
    And file "data/test_db/test_table/0_1_1_0/0_1_1_0.tar" in clickhouse01 backup #0 data set to
    """
    Corrupted backup data part
    """
    When we restore clickhouse backup #0 to clickhouse02
    """
    keep_going: true
    """
    And we execute query on clickhouse02
    """
    SELECT count() FROM system.parts WHERE table = 'test_table';
    """
    Then we get response
    """
    1
    """

  Scenario: Overwrite existing table on destination node when schema is mismatched
    When we drop all databases at clickhouse01
    And we drop all databases at clickhouse02    
    And we execute queries on clickhouse01
    """
    CREATE DATABASE test_db;
    CREATE TABLE test_db.test_table (partition_id Int32, a Int32)
    ENGINE MergeTree PARTITION BY partition_id ORDER BY (partition_id, a);

    INSERT INTO test_db.test_table SELECT number % 2, number FROM system.numbers LIMIT 100;
    """
    And we create clickhouse01 clickhouse backup  
    And we execute queries on clickhouse02
    """
    CREATE DATABASE test_db;
    CREATE TABLE test_db.test_table (partition_id Int32, a Int32, b Int32)
    ENGINE MergeTree PARTITION BY partition_id ORDER BY (partition_id, a, b);

    INSERT INTO test_db.test_table SELECT number % 2, number, number FROM system.numbers LIMIT 100;
    """
    And we put following info in "/etc/clickhouse-server/conf.d/max_table_size_to_drop.xml" at clickhouse02
    """
    <yandex>
      <max_table_size_to_drop>1</max_table_size_to_drop>
    </yandex>
    """
    And we execute query on clickhouse02
    """
    SYSTEM RELOAD CONFIG
    """
    And we restore clickhouse backup #0 to clickhouse02
    Then clickhouse02 has same schema as clickhouse01

  Scenario: Perform retry restore when exist the table based on the table function.
    Given we execute query on clickhouse01
    """
    CREATE DATABASE test_s3
    """
    When we put object in S3
    """
      bucket: cloud-storage-01
      path: /data.tsv
      data: '1'
    """

    When we execute query on clickhouse01
    """
    CREATE TABLE test_s3.s3_test_table (v Int) AS
    s3('{{conf['s3']['endpoint']}}/cloud-storage-01/data.tsv', '{{conf['s3']['access_key_id']}}', '{{conf['s3']['access_secret_key']}}', 'TSV');
    """

    And we create clickhouse01 clickhouse backup

    And we delete object in S3
    """
      bucket: cloud-storage-01
      path: /data.tsv
    """
    # If we restore the data, the table s3_test_table will be created through StorageProxy
    # and not all the rows from system.table will be accessible. Check that we can perform the restore operation after restore.
    And we restore clickhouse backup #0 to clickhouse01
    And we restore clickhouse backup #0 to clickhouse01
    And we execute query on clickhouse01
    """
    SELECT count(*) FROM system.tables WHERE name='s3_test_table';
    """
    Then we get response
    """
    1
    """
