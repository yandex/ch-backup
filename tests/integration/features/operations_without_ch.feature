Feature: Ch-backup operations without connection to clickhouse-server.

  Background:
    Given default configuration
    And a working s3
    And a working zookeeper on zookeeper01
    And a working clickhouse on clickhouse01
    And a working clickhouse on clickhouse02

  Scenario: Version command without ch-server.
    When we stop clickhouse at clickhouse01
    Then we got a valid ch-backup version on clickhouse01

  Scenario: List command without ch-server.
    When we create clickhouse01 clickhouse backup
    And we stop clickhouse at clickhouse01

    Then we got the following backups on clickhouse01
    | num | state   |
    | 0   | created |
