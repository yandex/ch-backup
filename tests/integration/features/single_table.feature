Feature: Backup of single database table

  Background:
    Given default configuration
    And a working s3
    And a working zookeeper on zookeeper01
    And a working clickhouse on clickhouse01
    And a working clickhouse on clickhouse02
    And clickhouse on clickhouse01 has test schema

  Scenario: Create backup
    Given clickhouse01 has test clickhouse data test1
    When we create clickhouse01 clickhouse backup
    """
    tables:
      - test_db_01.test_table_01
    """
    Then we got the following backups on clickhouse01
      | num | state    | data_count | link_count   |
      | 0   | created  | 1          | 0            |
    When we restore clickhouse backup #0 to clickhouse02
    Then clickhouse02 has the subset of clickhouse01 data
    """
    tables:
      - test_db_01.test_table_01
    """

  Scenario: Create backup with existing paths in backup directory
    Given clickhouse01 has test clickhouse data test1
    When we execute query on clickhouse01
    """
    ALTER TABLE test_db_01.test_table_01 FREEZE
    """
    When we try to execute command on clickhouse01
    """
    find /var/lib/clickhouse/shadow/1/store/*/*/* -print | head -n 1 > part_dir
    """
    When we execute query on clickhouse01
    """
    # Strange but you can't UNFREEZE without specifying name
    ALTER TABLE test_db_01.test_table_01 UNFREEZE with name '1'
    """
    When we try to execute command on clickhouse01
    """
    part_dir=$(cat part_dir)
    mkdir -p $part_dir
    echo "test" > $part_dir/doo
    chown -R clickhouse:clickhouse $part_dir
    """
    When we can't create clickhouse01 clickhouse backup with exception
    """
    tables:
      - test_db_01.test_table_01
    name: '1'
    """
    When we update ch-backup configuration on clickhouse01
    """
    backup:
        retry_on_existing_dir: 1
    """
    When we create clickhouse01 clickhouse backup
    """
    tables:
      - test_db_01.test_table_01
    name: '1'
    """
