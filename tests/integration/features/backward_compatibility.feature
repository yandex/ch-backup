Feature: Backward compatibility support for old backups

  Background:
    Given default configuration
    And a working s3
    And a working zookeeper on zookeeper01
    And a working clickhouse on clickhouse01
    And a working clickhouse on clickhouse02
    And ch-backup configuration on clickhouse01
    """
    encryption:
      enabled: False
    """
    And ch-backup configuration on clickhouse02
    """
    encryption:
      enabled: False
    """


  Scenario: Restore with old metadata layout
    When we execute queries on clickhouse01
    """
    CREATE DATABASE test_db_01 UUID '82aa76a0-45cd-42f2-b355-852cc8c9c0af' ENGINE = Atomic;
    CREATE TABLE test_db_01.table_01 UUID '10000000-0000-0000-0000-000000000001' (id UInt32, val String) ENGINE = MergeTree ORDER BY id;
    CREATE TABLE test_db_01.table_02 UUID '10000000-0000-0000-0000-000000000002' (id UInt32, abc String, def UInt64) ENGINE = MergeTree ORDER BY id;
    """
    When we create clickhouse01 clickhouse backup
    """
    name: test_backup
    """
    Then s3 bucket ch-backup contains 4 objects
    """
        bucket: ch-backup
    """
    When we delete object in S3
    """
        bucket: ch-backup
        path: /ch_backup/test_backup/metadata/databases.tar
    """
    And we delete object in S3
    """
        bucket: ch-backup
        path: /ch_backup/test_backup/metadata/test_db_01.tar
    """
    Then s3 bucket ch-backup contains 2 objects
    """
        bucket: ch-backup
    """
    When we put object in S3
    """
        bucket: ch-backup
        path: /ch_backup/test_backup/metadata/test_db_01.sql
        data: "ATTACH DATABASE _ UUID '82aa76a0-45cd-42f2-b355-852cc8c9c0af'\nENGINE = Atomic\n"
    """
    And we put object in S3
    """
        bucket: ch-backup
        path: /ch_backup/test_backup/metadata/test_db_01/table_01.sql
        data: "ATTACH TABLE _ UUID '10000000-0000-0000-0000-000000000001'\n(\n    `id` UInt32,\n    `val` String\n)\nENGINE = MergeTree\nORDER BY id\nSETTINGS index_granularity = 8192\n"
    """
    And we put object in S3
    """
        bucket: ch-backup
        path: /ch_backup/test_backup/metadata/test_db_01/table_02.sql
        data: "ATTACH TABLE _ UUID '10000000-0000-0000-0000-000000000002'\n(\n    `id` UInt32,\n    `abc` String,\n    `def` UInt64\n)\nENGINE = MergeTree\nORDER BY id\nSETTINGS index_granularity = 8192\n"
    """
    When we restore clickhouse backup #0 to clickhouse02
    Then clickhouse02 has same schema as clickhouse01

  # Regression: part.link used to store a full storage path (e.g. "ch_backup/<name>").
  # After the path→name refactor, normalize_backup_link() must transparently convert
  # the old format so that:
  #   1. restore of a legacy links-backup works (PartMetadata.load + download_data_part);
  #   2. a new incremental backup on top of a legacy chain still deduplicates correctly
  #      (dedup table read path in _populate_dedup_info);
  #   3. validate_part_after_upload passes for the new incremental backup.
  Scenario: Legacy full-path links are normalised for restore, dedup and validation
    Given we have executed queries on clickhouse01
    """
    CREATE DATABASE test_db_legacy;
    CREATE TABLE test_db_legacy.tbl (id UInt32, val String)
    ENGINE = MergeTree ORDER BY id;
    INSERT INTO test_db_legacy.tbl SELECT number, toString(number) FROM system.numbers LIMIT 20;
    """
    And ch-backup configuration on clickhouse01
    """
    backup:
        deduplicate_parts: True
        validate_part_after_upload: True
    """
    # Backup #1 — shared backup (new format, no links).
    When we create clickhouse01 clickhouse backup
    Then we got the following backups on clickhouse01
      | num | state   | data_count | link_count |
      | 0   | created | 1          | 0          |
    # Backup #0 — links backup (new format, link = plain backup name).
    When we create clickhouse01 clickhouse backup
    Then we got the following backups on clickhouse01
      | num | state   | data_count | link_count |
      | 0   | created | 0          | 1          |
      | 1   | created | 1          | 0          |
    # Simulate old ch-backup: rewrite link values in backup #0 to full paths.
    When part links of clickhouse01 backup #0 were rewritten to legacy path format
    # (1) Restore of the legacy links-backup must succeed.
    When we restore clickhouse backup #0 to clickhouse02
    Then we got same clickhouse data at clickhouse01 clickhouse02
    # (2+3) Create another incremental backup on top of the legacy-format chain.
    # Dedup must normalise the legacy link and produce link_count > 0;
    # validate_part_after_upload must also pass.
    When we create clickhouse01 clickhouse backup
    Then we got the following backups on clickhouse01
      | num | state   | data_count | link_count |
      | 0   | created | 0          | 1          |
      | 1   | created | 0          | 1          |
      | 2   | created | 1          | 0          |
    When we restore clickhouse backup #0 to clickhouse02
    Then we got same clickhouse data at clickhouse01 clickhouse02

  # Regression: purge/delete must correctly identify which parts are referenced
  # by a links-backup even when those links are stored in the legacy full-path
  # format.  If normalize_backup_link() is not applied, the dedup-reference
  # collector will fail to match the link against the set of backup names and
  # will incorrectly delete the shared data parts.
  Scenario: Purge mixed-format chain does not delete referenced parts
    Given we have executed queries on clickhouse01
    """
    CREATE DATABASE test_db_purge;
    CREATE TABLE test_db_purge.tbl (id UInt32, val String)
    ENGINE = MergeTree ORDER BY id;
    INSERT INTO test_db_purge.tbl SELECT number, toString(number) FROM system.numbers LIMIT 20;
    """
    And ch-backup configuration on clickhouse01
    """
    backup:
        deduplicate_parts: True
        retain_count: 1
        retain_time:
            days: 0
            seconds: 0
    """
    # Backup #1 — shared backup (new format).
    When we create clickhouse01 clickhouse backup
    # Backup #0 — links backup (new format).
    When we create clickhouse01 clickhouse backup
    Then we got the following backups on clickhouse01
      | num | state   | data_count | link_count |
      | 0   | created | 0          | 1          |
      | 1   | created | 1          | 0          |
    # Simulate legacy format: rewrite links in backup #0 to full paths.
    When part links of clickhouse01 backup #0 were rewritten to legacy path format
    # Purge should keep backup #0 (newest) and attempt to delete backup #1.
    # Because backup #0 still references backup #1's data parts (via legacy links),
    # backup #1 must become partially_deleted (data preserved) rather than fully removed.
    # This proves that normalize_backup_link() correctly resolves the legacy link so
    # that collect_dedup_references_for_batch_backup_deletion() protects the shared parts.
    When we purge clickhouse01 clickhouse backups
    Then we got the following backups on clickhouse01
      | num | state             | data_count | link_count |
      | 0   | created           | 0          | 1          |
      | 1   | partially_deleted | 1          | 0          |
    # Restore must succeed — the referenced data parts must still be present in S3.
    When we restore clickhouse backup #0 to clickhouse02
    Then we got same clickhouse data at clickhouse01 clickhouse02
