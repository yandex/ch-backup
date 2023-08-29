  Feature: Backup & Restore sources scenario with traffic limit.

  Background:
    Given default configuration
    And a working s3
    And a working zookeeper on zookeeper01
    And a working clickhouse on clickhouse01
    And clickhouse on clickhouse01 has test schema
    And clickhouse01 has test clickhouse data test1
  
  Scenario Outline: Test restore sources set traffic rate limit.
   Given ch-backup configuration on clickhouse01
    """
    rate_limiter:
      max_upload_rate: <rate>
    """
    When we create clickhouse01 clickhouse backup
    Then we got the following backups on clickhouse01
      | num | state    | data_count | link_count   | title         |
      | 0   | created  | 4          | 0            | shared        |

    When we restore clickhouse backup #0 to clickhouse02
    Given a working clickhouse on clickhouse02
    Then we got same clickhouse data at clickhouse01 clickhouse02

    Examples:
      | rate              |
      # unlimited
      | 0                 |
      #  5MB
      | 5242880           |
      # 16 MB
      | 16777216          |
