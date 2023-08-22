@without_zookeeper
Feature: Commands with shutted down ch server.

  Background:
    Given default configuration
    And a working s3
    And a working clickhouse on clickhouse02

    
  Scenario: Version command without clickhouse server
    When we stop clickhouse at clickhouse02
    Then we got a valid ch-backup version on clickhouse02
