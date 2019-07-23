@dependent-scenarios
Feature: Backup & Restore

  Background:
    Given default configuration
    And a working s3
    And a working clickhouse on clickhouse01
    And clickhouse on clickhouse01 has test schema

  Scenario: Create backups
    Given ch-backup configuration on clickhouse01
    """
    backup:
        deduplicate_parts: True
    """
    And clickhouse01 has test clickhouse data test1
    And we have created clickhouse01 clickhouse backup

    Given ch-backup configuration on clickhouse01
    """
    backup:
        deduplicate_parts: True
    """
    And clickhouse01 has test clickhouse data test2
    And we have created clickhouse01 clickhouse backup

    Given ch-backup configuration on clickhouse01
    """
    backup:
        deduplicate_parts: False
    """
    And we have created clickhouse01 clickhouse backup

    Given ch-backup configuration on clickhouse01
    """
    backup:
        deduplicate_parts: True
    """
    And clickhouse01 has test clickhouse data test4
    And we have created clickhouse01 clickhouse backup

    And we have created clickhouse01 clickhouse backup
    """
    schema_only: True
    """

    Given ch-backup configuration on clickhouse01
    """
    backup:
        deduplicate_parts: False
    """
    And we have created clickhouse01 clickhouse backup

    Given ch-backup configuration on clickhouse01
    """
    backup:
        deduplicate_parts: True
    """
    And clickhouse01 has test clickhouse data test6

    When we create clickhouse01 clickhouse backup
    Then we got the following backups on clickhouse01
      | num | state    | data_count | link_count   | title          |
      | 0   | created  | 4          | 12           | data+links     |
      | 1   | created  | 12         | 0            | shared         |
      | 2   | created  | 0          | 0            | schema-only    |
      | 3   | created  | 4          | 8            | data+links     |
      | 4   | created  | 8          | 0            | shared         |
      | 5   | created  | 4          | 4            | data+links     |
      | 6   | created  | 4          | 0            | shared         |

  Scenario: Purge with count removal = 0 and time removal = 0 leads to 0 deletes
    Given ch-backup configuration on clickhouse01
    """
    backup:
        retain_time:
            days: 1
        retain_count: 10
    """
    When we purge clickhouse01 clickhouse backups
    Then we got the following backups on clickhouse01
      | num | state    | data_count | link_count   | title          |
      | 0   | created  | 4          | 12           | data+links     |
      | 1   | created  | 12         | 0            | shared         |
      | 2   | created  | 0          | 0            | schema-only    |
      | 3   | created  | 4          | 8            | data+links     |
      | 4   | created  | 8          | 0            | shared         |
      | 5   | created  | 4          | 4            | data+links     |
      | 6   | created  | 4          | 0            | shared         |

  Scenario: Purge with count removal = 0 and time removal >=1 leads to 0 deletes
    Given metadata of clickhouse01 backup #6 was adjusted with
    """
    meta:
        start_time: {{ backup.meta.start_time | decrease_on('1 week') }}
        end_time: {{ backup.meta.end_time | decrease_on('1 week') }}
    """
    And metadata of clickhouse01 backup #5 was adjusted with
    """
    meta:
        start_time: {{ backup.meta.start_time | decrease_on('1 week') }}
        end_time: {{ backup.meta.end_time | decrease_on('1 week') }}
    """
    And ch-backup configuration on clickhouse01
    """
    backup:
        retain_time:
            days: 1
        retain_count: 10
    """
    When we purge clickhouse01 clickhouse backups
    Then we got the following backups on clickhouse01
      | num | state    | data_count | link_count   | title          |
      | 0   | created  | 4          | 12           | data+links     |
      | 1   | created  | 12         | 0            | shared         |
      | 2   | created  | 0          | 0            | schema-only    |
      | 3   | created  | 4          | 8            | data+links     |
      | 4   | created  | 8          | 0            | shared         |
      | 5   | created  | 4          | 4            | data+links     |
      | 6   | created  | 4          | 0            | shared         |

  Scenario: Purge with count removal >=1 and time removal = 0 leads to 0 deletes
    Given ch-backup configuration on clickhouse01
    """
    backup:
        retain_time:
            weeks: 2
            days: 0
        retain_count: 1
    """
    When we purge clickhouse01 clickhouse backups
    Then we got the following backups on clickhouse01
      | num | state    | data_count | link_count   | title          |
      | 0   | created  | 4          | 12           | data+links     |
      | 1   | created  | 12         | 0            | shared         |
      | 2   | created  | 0          | 0            | schema-only    |
      | 3   | created  | 4          | 8            | data+links     |
      | 4   | created  | 8          | 0            | shared         |
      | 5   | created  | 4          | 4            | data+links     |
      | 6   | created  | 4          | 0            | shared         |

  Scenario: Purge with count removal = 4 and time removal = 2 leads to 2 deletes
  Given ch-backup configuration on clickhouse01
  """
  backup:
      retain_time:
          weeks: 0
          days: 1
      retain_count: 2
  """
  When we purge clickhouse01 clickhouse backups
  Then we got the following backups on clickhouse01
    | num | state    | data_count | link_count   | title          |
    | 0   | created  | 4          | 12           | data+links     |
    | 1   | created  | 12         | 0            | shared         |
    | 2   | created  | 0          | 0            | schema-only    |
    | 3   | created  | 4          | 8            | data+links     |
    | 4   | created  | 8          | 0            | shared         |

  Scenario: Purge with count removal = 2 and time removal = 4 leads to 2 deletes
  Given ch-backup configuration on clickhouse01
  """
  backup:
      retain_time:
          days: 0
          seconds: 1
      retain_count: 2
  """
  When we purge clickhouse01 clickhouse backups
  Then we got the following backups on clickhouse01
    | num | state    | data_count | link_count   | title          |
    | 0   | created  | 4          | 12           | data+links     |
    | 1   | created  | 12         | 0            | shared         |