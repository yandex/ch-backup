"""
Unit tests for deduplication module.
"""

from datetime import timedelta
from unittest.mock import MagicMock

from tests.unit.utils import (
    assert_equal,
    backup_metadata,
    parametrize,
    parts,
    parts_dedup_info,
)

from ch_backup.backup.deduplication import (
    DatabaseDedupInfo,
    DedupInfo,
    collect_dedup_info,
    collect_dedup_references_for_batch_backup_deletion,
)
from ch_backup.backup.metadata import BackupState
from ch_backup.backup.metadata.backup_metadata import BackupMetadata
from ch_backup.backup_context import BackupContext
from ch_backup.clickhouse.models import Database


@parametrize(
    {
        "id": "initial backup",
        "args": {
            "config": {
                "backup": {
                    "deduplicate_parts": True,
                    "deduplication_age_limit": {
                        "days": 7,
                    },
                },
            },
            "databases": ["db1"],
            "creating_backup": backup_metadata("new_backup", BackupState.CREATING),
            "backups": [],
            "result": DedupInfo(),
        },
    },
    {
        "id": "ordinary incremental backup",
        "args": {
            "config": {
                "backup": {
                    "deduplicate_parts": True,
                    "deduplication_age_limit": {
                        "days": 7,
                    },
                },
            },
            "databases": ["db1"],
            "creating_backup": backup_metadata("new_backup", BackupState.CREATING),
            "backups": [
                backup_metadata(
                    name="backup1",
                    state=BackupState.CREATED,
                    age=timedelta(days=1),
                    databases={
                        "db1": {
                            "tables": {
                                "table1": {
                                    "engine": "MergeTree",
                                    "parts": parts(1),
                                },
                            },
                        },
                    },
                ),
            ],
            "result": DedupInfo(
                {
                    "db1": DatabaseDedupInfo(
                        {"table1": parts_dedup_info("ch_backup/backup1", 1)}
                    ),
                }
            ),
        },
    },
    {
        "id": "deduplication disabled",
        "args": {
            "config": {
                "backup": {
                    "deduplicate_parts": False,
                },
            },
            "databases": ["db1"],
            "creating_backup": backup_metadata("new_backup", BackupState.CREATING),
            "backups": [
                backup_metadata(
                    name="backup1",
                    state=BackupState.CREATED,
                    age=timedelta(days=1),
                    databases={
                        "db1": {
                            "tables": {
                                "table1": {
                                    "engine": "MergeTree",
                                    "parts": parts(1),
                                },
                            },
                        },
                    },
                ),
            ],
            "result": DedupInfo(),
        },
    },
    {
        "id": "schema-only backup",
        "args": {
            "config": {
                "backup": {
                    "deduplicate_parts": True,
                    "deduplication_age_limit": {
                        "days": 7,
                    },
                },
            },
            "databases": ["db1"],
            "creating_backup": backup_metadata(
                "new_backup", BackupState.CREATING, schema_only=True
            ),
            "backups": [
                backup_metadata(
                    name="backup1",
                    state=BackupState.CREATED,
                    age=timedelta(days=1),
                    databases={
                        "db1": {
                            "tables": {
                                "table1": {
                                    "engine": "MergeTree",
                                    "parts": parts(1),
                                },
                            },
                        },
                    },
                ),
            ],
            "result": DedupInfo(),
        },
    },
    {
        "id": "irrelevant parts of old backups are ignored",
        "args": {
            "config": {
                "backup": {
                    "deduplicate_parts": True,
                    "deduplication_age_limit": {
                        "days": 7,
                    },
                },
            },
            "databases": ["db1"],
            "creating_backup": backup_metadata("new_backup", BackupState.CREATING),
            "backups": [
                backup_metadata(
                    name="backup2",
                    state=BackupState.CREATED,
                    age=timedelta(days=1),
                    databases={
                        "db1": {
                            "tables": {
                                "table1": {
                                    "engine": "MergeTree",
                                    "parts": parts(1, link="ch_backup/backup1"),
                                },
                                "table3": {
                                    "engine": "ReplicatedMergeTree",
                                    "parts": parts(1, link="ch_backup/backup1"),
                                },
                                "table5": {
                                    "engine": "ReplicatedMergeTree",
                                    "parts": parts(1),
                                },
                            },
                        },
                    },
                ),
                backup_metadata(
                    name="backup1",
                    state=BackupState.CREATED,
                    age=timedelta(days=2),
                    databases={
                        "db1": {
                            "tables": {
                                "table1": {
                                    "engine": "MergeTree",
                                    "parts": parts(1),
                                },
                                "table2": {
                                    "engine": "MergeTree",
                                    "parts": parts(1),
                                },
                                "table3": {
                                    "engine": "ReplicatedMergeTree",
                                    "parts": parts(1),
                                },
                                "table4": {
                                    "engine": "ReplicatedMergeTree",
                                    "parts": parts(1),
                                },
                            },
                        },
                    },
                ),
            ],
            "result": DedupInfo(
                {
                    "db1": DatabaseDedupInfo(
                        {
                            "table1": parts_dedup_info(
                                "ch_backup/backup1", 1, verified=True
                            ),
                            "table3": parts_dedup_info(
                                "ch_backup/backup1", 1, verified=True
                            ),
                            "table5": parts_dedup_info("ch_backup/backup2", 1),
                        }
                    ),
                }
            ),
        },
    },
    {
        "id": "deduplication info is collected only for requested databases",
        "args": {
            "config": {
                "backup": {
                    "deduplicate_parts": True,
                    "deduplication_age_limit": {
                        "days": 7,
                    },
                },
            },
            "databases": ["db1"],
            "creating_backup": backup_metadata("new_backup", BackupState.CREATING),
            "backups": [
                backup_metadata(
                    name="backup1",
                    state=BackupState.CREATED,
                    age=timedelta(days=1),
                    databases={
                        "db1": {
                            "tables": {
                                "table1": {
                                    "engine": "MergeTree",
                                    "parts": parts(1),
                                },
                            },
                        },
                        "db2": {
                            "tables": {
                                "table1": {
                                    "engine": "MergeTree",
                                    "parts": parts(1),
                                },
                            },
                        },
                    },
                ),
            ],
            "result": DedupInfo(
                {
                    "db1": DatabaseDedupInfo(
                        {"table1": parts_dedup_info("ch_backup/backup1", 1)}
                    ),
                }
            ),
        },
    },
    {
        "id": "parts of failed and partially deleted backups are used for deduplication",
        "args": {
            "config": {
                "backup": {
                    "deduplicate_parts": True,
                    "deduplication_age_limit": {
                        "days": 7,
                    },
                },
            },
            "databases": ["db1", "db2"],
            "creating_backup": backup_metadata("new_backup", BackupState.CREATING),
            "backups": [
                backup_metadata(
                    name="backup3",
                    state=BackupState.FAILED,
                    age=timedelta(days=1),
                    databases={
                        "db1": {
                            "tables": {
                                "table1": {
                                    "engine": "MergeTree",
                                    "parts": parts(1, link="ch_backup/backup1"),
                                },
                                "table2": {
                                    "engine": "MergeTree",
                                    "parts": parts(1),
                                },
                            },
                        },
                    },
                ),
                backup_metadata(
                    name="backup2",
                    state=BackupState.CREATED,
                    age=timedelta(days=2),
                    databases={
                        "db1": {
                            "tables": {
                                "table1": {
                                    "engine": "MergeTree",
                                    "parts": parts(1, link="ch_backup/backup1"),
                                },
                            },
                        },
                        "db2": {
                            "tables": {
                                "table1": {
                                    "engine": "MergeTree",
                                    "parts": parts(1),
                                },
                            },
                        },
                    },
                ),
                backup_metadata(
                    name="backup1",
                    state=BackupState.PARTIALLY_DELETED,
                    age=timedelta(days=3),
                    databases={
                        "db1": {
                            "tables": {
                                "table1": {
                                    "engine": "MergeTree",
                                    "parts": parts(1),
                                },
                            },
                        },
                    },
                ),
            ],
            "result": DedupInfo(
                {
                    "db1": DatabaseDedupInfo(
                        {
                            "table1": parts_dedup_info(
                                "ch_backup/backup1", 1, verified=True
                            ),
                            "table2": parts_dedup_info("ch_backup/backup3", 1),
                        }
                    ),
                    "db2": DatabaseDedupInfo(
                        {
                            "table1": parts_dedup_info("ch_backup/backup2", 1),
                        }
                    ),
                }
            ),
        },
    },
    {
        "id": "parts of backups that are out of deduction window are ignored",
        "args": {
            "config": {
                "backup": {
                    "deduplicate_parts": True,
                    "deduplication_age_limit": {
                        "days": 7,
                    },
                },
            },
            "databases": ["db1"],
            "creating_backup": backup_metadata("new_backup", BackupState.CREATING),
            "backups": [
                backup_metadata(
                    name="backup2",
                    state=BackupState.CREATED,
                    age=timedelta(days=1),
                    databases={
                        "db1": {
                            "tables": {
                                "table1": {
                                    "engine": "MergeTree",
                                    "parts": parts(1, link="ch_backup/backup1"),
                                },
                                "table2": {
                                    "engine": "MergeTree",
                                    "parts": parts(1),
                                },
                            },
                        },
                    },
                ),
                backup_metadata(
                    name="backup1",
                    state=BackupState.CREATED,
                    age=timedelta(days=10),
                    databases={
                        "db1": {
                            "tables": {
                                "table1": {
                                    "engine": "MergeTree",
                                    "parts": parts(1),
                                },
                            },
                        },
                    },
                ),
            ],
            "result": DedupInfo(
                {
                    "db1": DatabaseDedupInfo(
                        {
                            "table1": {},
                            "table2": parts_dedup_info("ch_backup/backup2", 1),
                        }
                    ),
                }
            ),
        },
    },
    {
        "id": "only local backups are used for deduplicating parts of non-replicated tables",
        "args": {
            "config": {
                "backup": {
                    "deduplicate_parts": True,
                    "deduplication_age_limit": {
                        "days": 7,
                    },
                },
            },
            "databases": ["db1"],
            "creating_backup": backup_metadata(
                "new_backup", BackupState.CREATING, hostname="host1"
            ),
            "backups": [
                backup_metadata(
                    name="backup2",
                    state=BackupState.CREATED,
                    age=timedelta(days=1),
                    hostname="host2",
                    databases={
                        "db1": {
                            "tables": {
                                "replicated_table": {
                                    "engine": "ReplicatedMergeTree",
                                    "parts": parts(1, link="ch_backup/backup1"),
                                },
                                "host2_table": {
                                    "engine": "MergeTree",
                                    "parts": parts(1),
                                },
                            },
                        },
                    },
                ),
                backup_metadata(
                    name="backup1",
                    state=BackupState.CREATED,
                    age=timedelta(days=2),
                    hostname="host1",
                    databases={
                        "db1": {
                            "tables": {
                                "replicated_table": {
                                    "engine": "ReplicatedMergeTree",
                                    "parts": parts(1),
                                },
                                "host1_table": {
                                    "engine": "MergeTree",
                                    "parts": parts(1),
                                },
                            },
                        },
                    },
                ),
            ],
            "result": DedupInfo(
                {
                    "db1": DatabaseDedupInfo(
                        {
                            "replicated_table": parts_dedup_info(
                                "ch_backup/backup1", 1, verified=True
                            ),
                            "host1_table": parts_dedup_info("ch_backup/backup1", 1),
                        }
                    ),
                }
            ),
        },
    },
)
def test_collect_dedup_info(config, creating_backup, databases, backups, result):
    context = BackupContext(config)
    context.backup_layout = layout_mock()
    context.backup_meta = creating_backup
    dbs = list(map(lambda db_name: Database(db_name, "", ""), databases))
    dedup_info = collect_dedup_info(
        context=context, databases=dbs, backups_with_light_meta=backups
    )
    assert_equal(dedup_info, result)


@parametrize(
    {
        "id": "single data part",
        "args": {
            "retained_backups_light_meta": [
                backup_metadata(
                    name="backup2",
                    state=BackupState.CREATED,
                    databases={
                        "db1": {
                            "tables": {
                                "table1": {
                                    "engine": "MergeTree",
                                    "parts": parts(1, link="ch_backup/backup1"),
                                },
                            },
                        },
                    },
                ),
            ],
            "deleting_backups_light_meta": [
                backup_metadata(
                    name="backup1",
                    state=BackupState.CREATED,
                    databases={
                        "db1": {
                            "tables": {
                                "table1": {
                                    "engine": "MergeTree",
                                    "parts": parts(1),
                                },
                            },
                        },
                    },
                ),
            ],
            "result": {
                "backup1": {
                    "db1": {
                        "table1": {"part1"},
                    },
                },
            },
        },
    }
)
def test_collect_dedup_references_for_batch_backup_deletion(
    retained_backups_light_meta, deleting_backups_light_meta, result
):
    layout = layout_mock()
    retained_backups = [
        layout.reload_backup(backup_light, False)
        for backup_light in retained_backups_light_meta
    ]
    deleting_backups = [
        layout.reload_backup(backup_light, False)
        for backup_light in deleting_backups_light_meta
    ]

    assert (
        collect_dedup_references_for_batch_backup_deletion(
            layout=layout_mock(),
            retained_backups_light_meta=retained_backups,
            deleting_backups_light_meta=deleting_backups,
        )
        == result
    )


def layout_mock():
    layout = MagicMock()
    layout.reload_backup = lambda backup, use_light_meta: backup
    # Passing str to ijson causes deprecation warning
    layout.reload_backup_raw = lambda backup, use_light_meta: bytes(
        BackupMetadata.dump_json(backup), "utf-8"
    )
    return layout
