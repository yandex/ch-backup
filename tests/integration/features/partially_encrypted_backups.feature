Feature: Support partially encrypted backups

  Background:
    Given default configuration
    And a working s3
    And a working zookeeper on zookeeper01
    And a working clickhouse on clickhouse01
    And a working clickhouse on clickhouse02
    And clickhouse on clickhouse01 has test schema

  Scenario: Create and restore backup into not encrypted backup with encryption check
    Given ch-backup configuration on clickhouse01
    """
    encryption:
      type: nacl
      enabled: True
      key: odaYtYjhvmeP8GO7vwWlXsViiDbgu4Ti
    """
    Given ch-backup configuration on clickhouse02
    """
    encryption:
      type: nacl
      enabled: False
      key: odaYtYjhvmeP8GO7vwWlXsViiDbgu4Ti
    """
    And clickhouse01 has test clickhouse data test1
    When we create clickhouse01 clickhouse backup
    Then we got the following backups on clickhouse01
      | num | state   | data_count | link_count |
      | 0   | created | 4          | 0          |
    Given ch-backup configuration on clickhouse01
    """
    encryption:
      type: nacl
      enabled: False
      key: odaYtYjhvmeP8GO7vwWlXsViiDbgu4Ti
    """
    And clickhouse01 has test clickhouse data test2
    When we create clickhouse01 clickhouse backup
    Then we got the following backups on clickhouse01
      | num | state   | data_count | link_count |
      | 0   | created | 4          | 4          |
      | 1   | created | 4          | 0          |
    When we restore clickhouse backup #0 to clickhouse02
    Then clickhouse02 has same schema as clickhouse01
    And we got same clickhouse data at clickhouse01 clickhouse02

  Scenario: Create and restore backup into encrypted backup with encryption check
    Given ch-backup configuration on clickhouse01
    """
    encryption:
      type: nacl
      enabled: True
      key: odaYtYjhvmeP8GO7vwWlXsViiDbgu4Ti
    """
    Given ch-backup configuration on clickhouse02
    """
    encryption:
      type: nacl
      enabled: True
      key: odaYtYjhvmeP8GO7vwWlXsViiDbgu4Ti
    """
    And clickhouse01 has test clickhouse data test1
    When we create clickhouse01 clickhouse backup
    Then we got the following backups on clickhouse01
      | num | state   | data_count | link_count |
      | 0   | created | 4          | 0          |
    Given ch-backup configuration on clickhouse01
    """
    encryption:
      type: nacl
      enabled: False
      key: odaYtYjhvmeP8GO7vwWlXsViiDbgu4Ti
    """
    And clickhouse01 has test clickhouse data test2
    When we create clickhouse01 clickhouse backup
    Then we got the following backups on clickhouse01
      | num | state   | data_count | link_count |
      | 0   | created | 4          | 4          |
      | 1   | created | 4          | 0          |
    When we restore clickhouse backup #0 to clickhouse02
    Then clickhouse02 has same schema as clickhouse01
    And we got same clickhouse data at clickhouse01 clickhouse02
