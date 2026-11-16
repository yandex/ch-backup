"""Check streamed metadata bytes and bounded part encoding."""

# pylint: disable=protected-access

import copy
import json
import os
from typing import Any, Iterator
from unittest.mock import patch

import pytest

from ch_backup.backup.metadata import BackupMetadata, PartMetadata
from ch_backup.backup.metadata.table_metadata import TableMetadata
from ch_backup.clickhouse.models import Database


@pytest.fixture(name="backup")
def backup_fixture() -> BackupMetadata:
    """Build a small backup with mixed links and Unicode metadata."""
    metadata = BackupMetadata(
        "current",
        "2",
        "26.3",
        "%Y-%m-%d %H:%M:%S %z",
        hostname="host",
        path="backups/current",
        labels={"label": '雪 "\\\n'},
    )
    metadata.add_database(Database("база", "Atomic", None, None, None))
    table = TableMetadata("база", "таблица", "MergeTree", None)
    for index, link in enumerate((None, "", "источник", "legacy/older")):
        table.add_part(
            PartMetadata(
                "база",
                "таблица",
                f"all_{index}_{index}_0",
                "checksum",
                10,
                ['данные"\\\n.bin'],
                True,
                link=link,
            )
        )
    metadata.add_table(table)
    metadata.add_udf("функция")
    metadata.add_named_collection("коллекция")
    metadata.add_workload_entity("нагрузка")
    metadata.add_http_handler("обработчик")
    metadata.cloud_storage.add_disk("s3")
    return metadata


@pytest.mark.parametrize("path", ["backups/current", "current", ""])
def test_streamed_bytes_match_dump_without_copying_tree(
    backup: BackupMetadata, path: str
) -> None:
    """Keep exact bytes and source data for converted and unchanged links."""
    backup.path = path
    before = copy.deepcopy(backup._databases)
    expected = backup.dump_json().encode()
    with (
        patch.object(
            backup, "_databases_with_legacy_part_links", side_effect=AssertionError
        ),
        patch.object(backup, "_dump_fields", wraps=backup._dump_fields) as fields,
    ):
        actual = "".join(backup.iter_json()).encode()
    assert actual == expected
    assert fields.call_args.args[0] is backup._databases
    assert backup._databases == before
    parts = json.loads(actual)["databases"]["база"]["tables"]["таблица"]["parts"]
    assert [part["link"] for part in parts.values()] == [
        None,
        "",
        os.path.join(os.path.dirname(path), "источник"),
        "legacy/older",
    ]


def test_light_dump_preserves_public_argument_and_fields(
    backup: BackupMetadata,
) -> None:
    """Omit detailed fields while preserving the same common metadata."""
    before = copy.deepcopy(backup._databases)
    full = backup.dump()
    with patch.object(
        backup, "_databases_with_legacy_part_links", side_effect=AssertionError
    ):
        light = backup.dump(True)
        assert backup.dump_json(light=True) == json.dumps(light, separators=(",", ":"))
    for key in ("databases", "access_controls"):
        assert light[key] == {}
    for key in (
        "user_defined_functions",
        "named_collections",
        "workload_entities",
        "http_handlers",
    ):
        assert light[key] == []
        assert full[key]
    assert light["meta"] == full["meta"]
    assert light["cloud_storage"] == full["cloud_storage"]
    full["databases"]["база"]["tables"]["таблица"]["parts"]["all_0_0_0"][
        "link"
    ] = "changed"
    assert backup._databases == before


@pytest.mark.parametrize("part_count", [1, 4])
def test_parts_use_single_chunk_and_one_shot_encoder(
    backup: BackupMetadata, part_count: int
) -> None:
    """Encode each part in one shot without yielding its individual tokens."""
    table = backup._databases["база"]["tables"]["таблица"]
    table["parts"] = dict(list(table["parts"].items())[:part_count])
    expected = backup.dump_json()
    expected_parts = json.loads(expected)["databases"]["база"]["tables"]["таблица"][
        "parts"
    ]
    iterencode = json.JSONEncoder.iterencode
    part_calls = []

    def track_encoding(
        encoder: json.JSONEncoder, value: Any, _one_shot: bool = False
    ) -> Iterator[str]:
        """Record whether part encoding enables the native JSON fast path."""
        if isinstance(value, dict) and "checksum" in value:
            part_calls.append(_one_shot)
        return iterencode(encoder, value, _one_shot)

    with patch.object(json.JSONEncoder, "iterencode", track_encoding):
        chunks = list(backup.iter_json())
    assert "".join(chunks) == expected
    assert part_calls == [True] * part_count
    for part in expected_parts.values():
        assert json.dumps(part, separators=(",", ":")) in chunks
