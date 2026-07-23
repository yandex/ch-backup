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
    PartMetadata,
    normalize_backup_link,
)
from ch_backup.backup.metadata.table_metadata import PartInfo, split_part_name


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

        dump = backup.dump()
        assert backup.dump_json().find(" ") == -1
        # Deprecated `path` field is still emitted for backward compatibility
        # with older ch-backup versions that read it.
        assert dump["meta"]["path"] == "ch_backup/20181017T210300"

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

    def test_load_json_accepts_legacy_path_field(self):
        """
        BackupMetadata.load_json() must succeed when the JSON contains the
        deprecated ``meta.path`` field (present in old backups) and must
        still populate all other fields correctly.

        Regression test: ensure we do not break reading old backup metadata.
        """
        metadata = {
            "meta": {
                "name": "20181017T210300",
                # DEPRECATED legacy field — preserved on load and re-emitted
                # on dump for backward compatibility with older ch-backup
                # versions.
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

        backup = BackupMetadata.load_json(json.dumps(metadata))

        assert backup.name == "20181017T210300"
        assert backup.state == BackupState.CREATED
        assert backup.hostname == "clickhouse01.test_net_711"
        assert backup.version == "1.0.100"
        assert backup.ch_version == "19.1.16"
        # Legacy ``path`` is preserved through the load/dump round-trip
        # (deprecated, but still required for older ch-backup versions).
        assert backup.path == "ch_backup/20181017T210300"
        assert (
            json.loads(backup.dump_json())["meta"]["path"]
            == "ch_backup/20181017T210300"
        )


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


class TestPartMetadata:
    """
    Tests for PartMetadata.
    """

    _BASE_RAW = {
        "checksum": "abc123",
        "bytes": 1024,
        "files": ["data.bin"],
        "tarball": False,
        "disk_name": "default",
        "encrypted": True,
        "database": "test_db",
        "table": "test_table",
        "name": "20181017T210300",
    }

    @pytest.mark.parametrize(
        ("raw_link", "expected_backup_name"),
        [
            # New format: plain backup name — returned as-is.
            ("20181017T210300", "20181017T210300"),
            # Old format: full path with path_root prefix.
            ("ch_backup/20181017T210300", "20181017T210300"),
            # Old format: absolute path.
            ("/srv/backups/20181017T210300", "20181017T210300"),
            # Nested path with trailing slash — must reduce to last component.
            ("/srv/backups/daily/20181017T210300/", "20181017T210300"),
            # No link (non-deduplicated part).
            (None, None),
            # Empty string treated as no link.
            ("", None),
        ],
    )
    def test_load_link_normalization(self, raw_link, expected_backup_name):
        """
        PartMetadata.load() must normalise the ``link`` field to a plain
        backup name regardless of whether it was stored as a full path
        (old format) or already as a name (new format).

        Also asserts that non-link fields are preserved unchanged.
        """
        raw = {**self._BASE_RAW, "link": raw_link}
        part = PartMetadata.load("db1", "table1", "part1", raw)

        assert part.link == expected_backup_name
        assert part.link_part_name is None

        # non-link fields must be preserved
        assert part.database == "db1"
        assert part.table == "table1"
        assert part.name == "part1"
        assert part.files == raw["files"]

    def test_link_with_part_name(self):
        """
        When link_part_name is present in raw metadata, it must be
        accessible via the link_part_name property.
        """
        raw = {
            **self._BASE_RAW,
            "link": "20240101T000000",
            "link_part_name": "all_1_1_0",
        }
        part = PartMetadata.load("db1", "table1", "part1", raw)

        assert part.link == "20240101T000000"
        assert part.link_part_name == "all_1_1_0"


class TestNormalizeBackupLink:
    """
    Tests for the normalize_backup_link() module-level helper.
    """

    @pytest.mark.parametrize(
        ("raw_link", "expected"),
        [
            # New format: plain backup name — returned as-is.
            ("20181017T210300", "20181017T210300"),
            # Old format: relative path with path_root prefix.
            ("ch_backup/20181017T210300", "20181017T210300"),
            # Old format: absolute path.
            ("/srv/backups/20181017T210300", "20181017T210300"),
            # Nested path with trailing slash — must reduce to last component.
            ("/srv/backups/daily/20181017T210300/", "20181017T210300"),
            # None → None.
            (None, None),
            # Empty string → None.
            ("", None),
        ],
    )
    def test_normalize_backup_link(self, raw_link, expected):
        """
        normalize_backup_link() must return a plain backup name for both
        old full-path and new name-only formats, and None for falsy input.
        """
        assert normalize_backup_link(raw_link) == expected


class TestSplitPartName:
    """
    Tests for split_part_name().
    """

    @pytest.mark.parametrize(
        ("part_name", "expected"),
        [
            # Standard format without mutation: partition_min_max_level
            (
                "all_1_1_0",
                PartInfo(
                    partition_id="all",
                    min_block_num=1,
                    max_block_num=1,
                    level=0,
                    mutation=0,
                ),
            ),
            # Numeric partition_id
            (
                "20200101_1_5_2",
                PartInfo(
                    partition_id="20200101",
                    min_block_num=1,
                    max_block_num=5,
                    level=2,
                    mutation=0,
                ),
            ),
            # Format with mutation: partition_min_max_level_mutation
            (
                "all_1_1_0_42",
                PartInfo(
                    partition_id="all",
                    min_block_num=1,
                    max_block_num=1,
                    level=0,
                    mutation=42,
                ),
            ),
            # Numeric partition_id with mutation
            (
                "20200101_3_7_1_100",
                PartInfo(
                    partition_id="20200101",
                    min_block_num=3,
                    max_block_num=7,
                    level=1,
                    mutation=100,
                ),
            ),
            # Large block numbers
            (
                "all_1000000_9999999_999",
                PartInfo(
                    partition_id="all",
                    min_block_num=1000000,
                    max_block_num=9999999,
                    level=999,
                    mutation=0,
                ),
            ),
            # partition_id is the first chunk only; remaining chunks are parsed as numbers
            (
                "tuple_1_2_3",
                PartInfo(
                    partition_id="tuple",
                    min_block_num=1,
                    max_block_num=2,
                    level=3,
                    mutation=0,
                ),
            ),
        ],
    )
    def test_valid_part_names(self, part_name: str, expected: PartInfo) -> None:
        """
        split_part_name() must correctly parse valid part names.
        """
        assert split_part_name(part_name) == expected

    @pytest.mark.parametrize(
        "part_name",
        [
            # Too few segments
            "all",
            "all_1",
            "all_1_2",
            # Empty string
            "",
        ],
    )
    def test_too_few_segments_raises(self, part_name: str) -> None:
        """
        split_part_name() must raise ValueError when fewer than four segments are present.
        """
        with pytest.raises(ValueError, match="Invalid part name format"):
            split_part_name(part_name)

    @pytest.mark.parametrize(
        "part_name",
        [
            # Non-numeric values in numeric fields
            "all_x_1_0",
            "all_1_y_0",
            "all_1_1_z",
            "all_1_1_0_abc",
        ],
    )
    def test_non_numeric_segments_raises(self, part_name: str) -> None:
        """
        split_part_name() must raise ValueError when numeric fields contain
        non-numeric values.
        """
        with pytest.raises(ValueError, match="Invalid part name format"):
            split_part_name(part_name)

    def test_returns_part_info_namedtuple(self) -> None:
        """
        split_part_name() must return a PartInfo instance.
        """
        result = split_part_name("all_1_1_0")
        assert isinstance(result, PartInfo)

    def test_mutation_zero_when_absent(self) -> None:
        """
        When mutation is not specified, the mutation field must default to 0.
        """
        result = split_part_name("all_1_1_0")
        assert result.mutation == 0

    def test_mutation_present(self) -> None:
        """
        When mutation is specified, the mutation field must contain its value.
        """
        result = split_part_name("all_1_1_0_7")
        assert result.mutation == 7
