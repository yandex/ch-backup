"""
Unit tests for layout module.
"""

import json
from datetime import datetime

import pytest
from ch_backup.backup.metadata import (
    AccessControlMetadata,
    BackupMetadata,
    BackupState,
    BackupStorageFormat,
)


class TestBackupMetadata:
    """
    Tests for ClickhouseBackupStructure.
    """

    @pytest.mark.parametrize(
        "meta",
        [
            {
                "state": "created",
                "start_time": "2018-10-18 00:03:00 +0300",
                "end_time": "2018-10-18 00:04:00 +0300",
            },
            {
                "state": "creating",
                "start_time": "2018-10-18 00:03:00 +0300",
                "end_time": None,
            },
        ],
    )
    def test_load_json(self, meta):
        """
        load_json() test.
        """

        meta = {
            "name": "20181017T210300",
            "path": "ch_backup/20181017T210300",
            "time_format": "%Y-%m-%d %H:%M:%S %z",
            "bytes": 0,
            "real_bytes": 0,
            "hostname": "clickhouse01.test_net_711",
            "version": "1.0.100",
            "ch_version": "19.1.16",
            "labels": None,
            **meta,
        }
        metadata = {
            "meta": meta,
            "databases": [],
        }

        backup = BackupMetadata.load_json(json.dumps(metadata))
        assert backup.name == meta["name"]
        assert backup.path == meta["path"]
        assert backup.state == BackupState(meta["state"])
        time_format = meta["time_format"]
        assert backup.time_format == time_format
        assert backup.start_time == datetime.strptime(meta["start_time"], time_format)
        end_time_str = meta["end_time"]
        end_time = (
            datetime.strptime(end_time_str, time_format) if end_time_str else None
        )
        assert backup.end_time == end_time
        assert backup.size == meta["bytes"]
        assert backup.real_size == meta["real_bytes"]
        assert backup.hostname == meta["hostname"]
        assert backup.version == meta["version"]
        assert backup.ch_version == meta["ch_version"]
        assert backup.labels == meta["labels"]

    def test_dump_is_compact(self):
        backup = BackupMetadata(
            name="20181017T210300",
            path="ch_backup/20181017T210300",
            version="1.0.100",
            ch_version="19.1.16",
            time_format="%Y-%m-%dT%H:%M:%S%Z",
            hostname="clickhouse01.test_net_711",
        )

        assert backup.dump_json().find(" ") == -1

    @pytest.mark.parametrize(
        "access_control",
        [
            {
                "access_control": ["id_1"],  # Old version
                "access_control_meta": {
                    "0": {
                        "name": "acl1",
                        "char": "U",
                    },
                },
            },
            {
                "access_controls": {  # New version
                    "acl_ids": ["id_1"],
                    "acl_meta": {
                        "0": {
                            "name": "acl1",
                            "char": "U",
                        },
                    },
                    "backup_format": "plain",
                },
            },
        ],
    )
    def test_access_control_loading(self, access_control: dict) -> None:
        metadata = {
            "meta": {
                "name": "20181017T210300",
                "path": "ch_backup/20181017T210300",
                "time_format": "%Y-%m-%d %H:%M:%S %z",
                "bytes": 0,
                "real_bytes": 0,
                "hostname": "clickhouse01.test_net_711",
                "version": "1.0.100",
                "ch_version": "19.1.16",
                "labels": None,
                "state": "created",
                "start_time": "2018-10-18 00:03:00 +0300",
                "end_time": "2018-10-18 00:04:00 +0300",
            },
            "databases": [],
        }
        metadata.update(access_control)

        backup = BackupMetadata.load(metadata)
        acl = backup.access_control

        assert acl.acl_ids == ["id_1"]
        assert acl.acl_meta == {
            "0": {
                "name": "acl1",
                "char": "U",
            },
        }
        assert acl.backup_format == BackupStorageFormat.PLAIN


class TestAccessControlMetadata:
    @pytest.mark.parametrize(
        ("objects", "expected_list", "expected_meta"),
        [
            ([], [], {}),
            (
                [
                    {
                        "id": "1",
                        "name": "acl1",
                        "char": "U",
                    }
                ],
                ["1"],
                {
                    "0": {
                        "name": "acl1",
                        "char": "U",
                    },
                },
            ),
            (
                [
                    {
                        "id": "2",
                        "name": "acl2",
                        "char": "P",
                    },
                    {
                        "id": "1",
                        "name": "acl1",
                        "char": "U",
                    },
                ],
                ["2", "1"],
                {
                    "0": {
                        "name": "acl2",
                        "char": "P",
                    },
                    "1": {
                        "name": "acl1",
                        "char": "U",
                    },
                },
            ),
        ],
    )
    def test_from_ch_objects(self, objects, expected_list, expected_meta):
        # pylint: disable=protected-access
        acl = AccessControlMetadata.from_ch_objects(objects)
        assert len(acl.acl_ids) == len(objects)
        assert len(acl.acl_meta) == len(objects)
        assert acl.acl_ids == expected_list
        assert acl.acl_meta == expected_meta
        assert acl.backup_format == BackupStorageFormat.TAR

    @pytest.mark.parametrize(
        "data",
        [
            {},
            {
                "backup_format": "tar",
            },
            {
                "acl_ids": ["id_1"],
                "acl_meta": {
                    "0": {
                        "name": "acl_1",
                        "char": "U",
                    },
                },
                "backup_format": "tar",
            },
        ],
    )
    def test_load(self, data: dict) -> None:
        acl = AccessControlMetadata.load(data)

        assert acl.acl_ids == data.get("acl_ids", [])
        assert acl.acl_meta == data.get("acl_meta", {})
        assert acl.backup_format == BackupStorageFormat(
            data.get("backup_format", "tar")
        )

    def test_dump(self) -> None:
        acl = AccessControlMetadata(
            acl_ids=["id_1"],
            acl_meta={
                "0": {
                    "name": "acl_1",
                    "char": "U",
                }
            },
            backup_format=BackupStorageFormat.TAR,
        )

        assert acl.dump() == {
            "acl_ids": ["id_1"],
            "acl_meta": {
                "0": {
                    "name": "acl_1",
                    "char": "U",
                },
            },
            "backup_format": "tar",
        }
