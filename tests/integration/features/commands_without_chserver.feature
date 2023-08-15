
Feature: Commands with shutted down ch server.

  Background:
    Given default configuration
    And a working s3
    And a working clickhouse on clickhousenozk02

    
  Scenario: Version command without clickhouse server
    When we stop clickhouse at clickhousenozk02
    Then we got a vaild ch-backup version on clickhousenozk02
