  Feature: Backup & Restore sources scenario with traffic limit. 

  Background:
    Given default configuration
    And a working s3
    And a working zookeeper on zookeeper01
    And a working clickhouse on clickhouse01
    And clickhouse on clickhouse01 has test schema
    And clickhouse01 has test clickhouse data test1
    And we have executed queries on clickhouse01
    """
    CREATE USER test_user IDENTIFIED WITH plaintext_password BY 'password';
    CREATE ROLE test_role;
    """
  Scenario: Test restore sources unlimited traffic rate.
   Given ch-backup configuration on clickhouse01
    """
    storage:
        uploading_traffic_limit_retry_time: 0
    """
    When we create clickhouse01 clickhouse backup
    Then we got the following backups on clickhouse01
      | num | state   | data_count | acl_count | udf_count | schema_only |
      | 0   | created | 4          | 2         | 0         | False       |

    When we restore clickhouse backup #0 to clickhouse02
    And we restart clickhouse on clickhouse02
    Given a working clickhouse on clickhouse02
    Then we got same clickhouse data at clickhouse01 clickhouse02

  Scenario: Test restore sources setted traffic rate limit.
   Given ch-backup configuration on clickhouse01
    """
    storage:
        uploading_traffic_limit_retry_time: 33554432
    """
    When we create clickhouse01 clickhouse backup
    Then we got the following backups on clickhouse01
      | num | state   | data_count | acl_count | udf_count | schema_only |
      | 0   | created | 4          | 2         | 0         | False       |

    When we restore clickhouse backup #0 to clickhouse02
    And we restart clickhouse on clickhouse02
    Given a working clickhouse on clickhouse02
    Then we got same clickhouse data at clickhouse01 clickhouse02

