@dependent-scenarios
Feature: Backup & Clean & Restore

  Background:
    Given default configuration
    And a working s3
    And a working zookeeper on zookeeper01
    And a working clickhouse on clickhouse01
    And clickhouse on clickhouse01 has test schema

  Scenario: Create "shared" backup
    Given ch-backup configuration on clickhouse01
    """
    backup:
        deduplicate_parts: True
    """
    And clickhouse01 has test clickhouse data test1
    When we create clickhouse01 clickhouse backup
    Then we got the following backups on clickhouse01
      | num | state    | data_count | link_count   | title         |
      | 0   | created  | 4          | 0            | shared        |

  Scenario: Create "shared + links" backup
    Given ch-backup configuration on clickhouse01
    """
    backup:
        deduplicate_parts: True
    """
    And clickhouse01 has test clickhouse data test2
    When we create clickhouse01 clickhouse backup
    Then we got the following backups on clickhouse01
      | num | state    | data_count | link_count   | title         |
      | 0   | created  | 4          | 4            | shared+links  |
      | 1   | created  | 4          | 0            | shared        |

  Scenario: Create "links" backup
    Given ch-backup configuration on clickhouse01
    """
    backup:
        deduplicate_parts: True
    """
    When we create clickhouse01 clickhouse backup
    Then we got the following backups on clickhouse01
      | num | state    | data_count | link_count   | title         |
      | 0   | created  | 0          | 8            | links         |
      | 1   | created  | 4          | 4            | shared+links  |
      | 2   | created  | 4          | 0            | shared        |

  Scenario: Create "shared + data" backup
    Given ch-backup configuration on clickhouse01
    """
    backup:
        deduplicate_parts: False
    """
    And clickhouse01 has test clickhouse data test4
    When we create clickhouse01 clickhouse backup
    Then we got the following backups on clickhouse01
      | num | state    | data_count | link_count   | title         |
      | 0   | created  | 12         | 0            | shared+data   |
      | 1   | created  | 0          | 8            | links         |
      | 2   | created  | 4          | 4            | shared+links  |
      | 3   | created  | 4          | 0            | shared        |

  Scenario: Create "links + data" backup
    Given ch-backup configuration on clickhouse01
    """
    backup:
        deduplicate_parts: True
    """
    And we have dropped test table #1 in db #1 on clickhouse01
    And clickhouse on clickhouse01 has test schema
    And clickhouse01 has test clickhouse data test5
    When we create clickhouse01 clickhouse backup
    Then we got the following backups on clickhouse01
      | num | state    | data_count | link_count   | title         |
      | 0   | created  | 4          | 9            | links+data    |
      | 1   | created  | 12         | 0            | shared+data   |
      | 2   | created  | 0          | 8            | links         |
      | 3   | created  | 4          | 4            | shared+links  |
      | 4   | created  | 4          | 0            | shared        |

  Scenario: Create "data" backup
    Given ch-backup configuration on clickhouse01
    """
    backup:
        deduplicate_parts: False
    """
    When we create clickhouse01 clickhouse backup
    Then we got the following backups on clickhouse01
      | num | state    | data_count | link_count   | title         |
      | 0   | created  | 13         | 0            | data          |
      | 1   | created  | 4          | 9            | links+data    |
      | 2   | created  | 12         | 0            | shared+data   |
      | 3   | created  | 0          | 8            | links         |
      | 4   | created  | 4          | 4            | shared+links  |
      | 5   | created  | 4          | 0            | shared        |

  Scenario: Create "schema-only" backup
    When we create clickhouse01 clickhouse backup
    """
    schema_only: True
    """
    Then we got the following backups on clickhouse01
      | num | state    | data_count | link_count   | title         |
      | 0   | created  | 0          | 0            | schema-only   |
      | 1   | created  | 13         | 0            | data          |
      | 2   | created  | 4          | 9            | links+data    |
      | 3   | created  | 12         | 0            | shared+data   |
      | 4   | created  | 0          | 8            | links         |
      | 5   | created  | 4          | 4            | shared+links  |
      | 6   | created  | 4          | 0            | shared        |

  Scenario: Attempt to delete "shared" backup deletes no data
    When we delete clickhouse01 clickhouse backup #6
    Then we got the following backups on clickhouse01
      | num | state    | data_count | link_count   | title         |
      | 0   | created  | 0          | 0            | schema-only   |
      | 1   | created  | 13         | 0            | data          |
      | 2   | created  | 4          | 9            | links+data    |
      | 3   | created  | 12         | 0            | shared+data   |
      | 4   | created  | 0          | 8            | links         |
      | 5   | created  | 4          | 4            | shared+links  |
      | 6   | created  | 4          | 0            | shared        |

  Scenario: Attempt to delete "schema-only" backup succeeds
    When we delete clickhouse01 clickhouse backup #0
    Then we got the following backups on clickhouse01
      | num | state    | data_count | link_count   | title         |
      | 0   | created  | 13         | 0            | data          |
      | 1   | created  | 4          | 9            | links+data    |
      | 2   | created  | 12         | 0            | shared+data   |
      | 3   | created  | 0          | 8            | links         |
      | 4   | created  | 4          | 4            | shared+links  |
      | 5   | created  | 4          | 0            | shared        |

  Scenario: Attempt to delete "shared + links" backup deletes links only
    When we delete clickhouse01 clickhouse backup #4
    Then we got the following backups on clickhouse01
      | num | state             | data_count | link_count   | title        |
      | 0   | created           | 13         | 0            | data         |
      | 1   | created           | 4          | 9            | links+data   |
      | 2   | created           | 12         | 0            | shared+data  |
      | 3   | created           | 0          | 8            | links        |
      | 4   | partially_deleted | 4          | 0            | shared+links |
      | 5   | created           | 4          | 0            | shared       |

  Scenario: Attempt to delete "links" backup succeeds
    When we delete clickhouse01 clickhouse backup #3
    Then we got the following backups on clickhouse01
      | num | state             | data_count | link_count   | title        |
      | 0   | created           | 13         | 0            | data         |
      | 1   | created           | 4          | 9            | links+data   |
      | 2   | created           | 12         | 0            | shared+data  |
      | 3   | partially_deleted | 4          | 0            | shared+links |
      | 4   | created           | 4          | 0            | shared       |

  Scenario: Attempt to delete "shared + data" backup deletes non-shared data only
    When we delete clickhouse01 clickhouse backup #2
    Then we got the following backups on clickhouse01
      | num | state             | data_count | link_count   | title        |
      | 0   | created           | 13         | 0            | data         |
      | 1   | created           | 4          | 9            | links+data   |
      | 2   | partially_deleted | 9          | 0            | shared+data  |
      | 3   | partially_deleted | 4          | 0            | shared+links |
      | 4   | created           | 4          | 0            | shared       |

  Scenario: Attempt to delete  "links + data" backup succeeds
    When we delete clickhouse01 clickhouse backup #1
    Then we got the following backups on clickhouse01
      | num | state             | data_count | link_count   | title        |
      | 0   | created           | 13         | 0            | data         |
      | 1   | partially_deleted | 9          | 0            | shared+data  |
      | 2   | partially_deleted | 4          | 0            | shared+links |
      | 3   | created           | 4          | 0            | shared       |

  Scenario: Attempt to delete  "data" backup succeeds
    When we delete clickhouse01 clickhouse backup #0
    Then we got the following backups on clickhouse01
      | num | state             | data_count | link_count   | title        |
      | 0   | partially_deleted | 9          | 0            | shared+data  |
      | 1   | partially_deleted | 4          | 0            | shared+links |
      | 2   | created           | 4          | 0            | shared       |
