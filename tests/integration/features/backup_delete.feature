Feature: Backup & Clean & Restore

  Background: Insert initial data into clickhouse
    Given default configuration
    And a working s3
    And a working clickhouse on clickhouse01
    And clickhouse on clickhouse01 has test schema


  Scenario: Backup "shared" done successfully
    Given ch-backup config on clickhouse01 was merged with following
    """
    backup:
        deduplicate_parts: True
    """
    And clickhouse01 has test clickhouse data test1
    When we create clickhouse01 clickhouse backup
    Then ch_backup entries of clickhouse01 are in proper condition
      | num | state    | data_count | link_count   | title         |
      | 0   | created  | 4          | 0            | shared        |

  Scenario: Backup "shared + links" done successfully
    Given ch-backup config on clickhouse01 was merged with following
    """
    backup:
        deduplicate_parts: True
    """
    And clickhouse01 has test clickhouse data test2
    When we create clickhouse01 clickhouse backup
    Then ch_backup entries of clickhouse01 are in proper condition
      | num | state    | data_count | link_count   | title         |
      | 0   | created  | 4          | 4            | shared+links  |
      | 1   | created  | 4          | 0            | shared        |

  Scenario: Backup "links" done successfully
    Given ch-backup config on clickhouse01 was merged with following
    """
    backup:
        deduplicate_parts: True
    """
    When we create clickhouse01 clickhouse backup
    Then ch_backup entries of clickhouse01 are in proper condition
      | num | state    | data_count | link_count   | title         |
      | 0   | created  | 0          | 8            | links         |
      | 1   | created  | 4          | 4            | shared+links  |
      | 2   | created  | 4          | 0            | shared        |

  Scenario: Backup "shared + data" done successfully
    Given ch-backup config on clickhouse01 was merged with following
    """
    backup:
        deduplicate_parts: False
    """
    And clickhouse01 has test clickhouse data test4
    When we create clickhouse01 clickhouse backup
    Then ch_backup entries of clickhouse01 are in proper condition
      | num | state    | data_count | link_count   | title         |
      | 0   | created  | 12         | 0            | shared+data   |
      | 1   | created  | 0          | 8            | links         |
      | 2   | created  | 4          | 4            | shared+links  |
      | 3   | created  | 4          | 0            | shared        |

  Scenario: Backup "links + data" done successfully
    Given ch-backup config on clickhouse01 was merged with following
    """
    backup:
        deduplicate_parts: True
    """
    And we have dropped test table #1 in db #1 on clickhouse01
    And clickhouse on clickhouse01 has test schema
    And clickhouse01 has test clickhouse data test5
    When we create clickhouse01 clickhouse backup
    Then ch_backup entries of clickhouse01 are in proper condition
      | num | state    | data_count | link_count   | title         |
      | 0   | created  | 4          | 9            | links+data    |
      | 1   | created  | 12         | 0            | shared+data   |
      | 2   | created  | 0          | 8            | links         |
      | 3   | created  | 4          | 4            | shared+links  |
      | 4   | created  | 4          | 0            | shared        |

  Scenario: Backup "data" done successfully
    Given ch-backup config on clickhouse01 was merged with following
    """
    backup:
        deduplicate_parts: False
    """
    When we create clickhouse01 clickhouse backup
    Then ch_backup entries of clickhouse01 are in proper condition
      | num | state    | data_count | link_count   | title         |
      | 0   | created  | 13         | 0            | data          |
      | 1   | created  | 4          | 9            | links+data    |
      | 2   | created  | 12         | 0            | shared+data   |
      | 3   | created  | 0          | 8            | links         |
      | 4   | created  | 4          | 4            | shared+links  |
      | 5   | created  | 4          | 0            | shared        |

  Scenario: Backup "shared" is not deleted
    When we delete clickhouse01 clickhouse backup #5
    Then ch_backup entries of clickhouse01 are in proper condition
      | num | state    | data_count | link_count   | title         |
      | 0   | created  | 13         | 0            | data          |
      | 1   | created  | 4          | 9            | links+data    |
      | 2   | created  | 12         | 0            | shared+data   |
      | 3   | created  | 0          | 8            | links         |
      | 4   | created  | 4          | 4            | shared+links  |
      | 5   | created  | 4          | 0            | shared        |

  Scenario: Backup "shared + links" deletes links but not shared data
    When we delete clickhouse01 clickhouse backup #4
    Then ch_backup entries of clickhouse01 are in proper condition
      | num | state             | data_count | link_count   | title        |
      | 0   | created           | 13         | 0            | data         |
      | 1   | created           | 4          | 9            | links+data   |
      | 2   | created           | 12         | 0            | shared+data  |
      | 3   | created           | 0          | 8            | links        |
      | 4   | partially_deleted | 4          | 0            | shared+links |
      | 5   | created           | 4          | 0            | shared       |

  Scenario: Backup "links" deleted successfully
    When we delete clickhouse01 clickhouse backup #3
    Then ch_backup entries of clickhouse01 are in proper condition
      | num | state             | data_count | link_count   | title        |
      | 0   | created           | 13         | 0            | data         |
      | 1   | created           | 4          | 9            | links+data   |
      | 2   | created           | 12         | 0            | shared+data  |
      | 3   | partially_deleted | 4          | 0            | shared+links |
      | 4   | created           | 4          | 0            | shared       |

  Scenario: Backup "shared + data" deletes data but not shared data
    When we delete clickhouse01 clickhouse backup #2
    Then ch_backup entries of clickhouse01 are in proper condition
      | num | state             | data_count | link_count   | title        |
      | 0   | created           | 13         | 0            | data         |
      | 1   | created           | 4          | 9            | links+data   |
      | 2   | partially_deleted | 9          | 0            | shared+data  |
      | 3   | partially_deleted | 4          | 0            | shared+links |
      | 4   | created           | 4          | 0            | shared       |

  Scenario: Backup "links + data" deleted successfully
    When we delete clickhouse01 clickhouse backup #1
    Then ch_backup entries of clickhouse01 are in proper condition
      | num | state             | data_count | link_count   | title        |
      | 0   | created           | 13         | 0            | data         |
      | 1   | partially_deleted | 9          | 0            | shared+data  |
      | 2   | partially_deleted | 4          | 0            | shared+links |
      | 3   | created           | 4          | 0            | shared       |

  Scenario: Backup "data" deleted successfully
    When we delete clickhouse01 clickhouse backup #0
    Then ch_backup entries of clickhouse01 are in proper condition
      | num | state             | data_count | link_count   | title        |
      | 0   | partially_deleted | 9          | 0            | shared+data  |
      | 1   | partially_deleted | 4          | 0            | shared+links |
      | 2   | created           | 4          | 0            | shared       |