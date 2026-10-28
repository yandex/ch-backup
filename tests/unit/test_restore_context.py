"""Restore progress follows logical addresses, independently of stored owners."""

import json
from collections.abc import Callable
from functools import partial
from pathlib import Path

import pytest

from ch_backup.backup.metadata import PartMetadata
from ch_backup.backup.restore_context import PartState, RestoreContext


@pytest.fixture(name="restore_config")
def fixture_restore_config(tmp_path: Path) -> dict:
    return {
        "restore_context_path": str(tmp_path / "restore.json"),
        "restore_context_sync_threshold_ops": 2,
    }


@pytest.fixture(name="make_part")
def fixture_make_part() -> Callable[..., PartMetadata]:
    return partial(
        PartMetadata,
        database="db",
        table="table",
        name="all_1_1_0_7",
        checksum="checksum",
        size=100,
        files=["data.bin"],
        tarball=True,
        link="owner",
        link_part_name="all_1_1_0",
    )


@pytest.mark.parametrize(
    "change",
    [
        {"database": "other"},
        {"table": "other"},
        {"name": "all_1_1_0_8"},
        {"name": "all_1_1_0"},
    ],
)
def test_restored_state_does_not_transfer_to_another_address(
    restore_config, make_part, change
):
    context = RestoreContext(restore_config)
    context.change_part_state(PartState.RESTORED, make_part())

    assert context.part_restored(make_part())
    assert not context.part_restored(make_part(**change))


def test_changed_storage_owner_does_not_reset_restore_progress(
    restore_config, make_part
):
    context = RestoreContext(restore_config)
    context.change_part_state(PartState.DOWNLOADED, make_part())

    assert context.part_downloaded(make_part(link="other", link_part_name="other_part"))


def test_automatic_save_preserves_json_shape_and_logical_failure_name(
    restore_config, make_part
):
    context = RestoreContext(restore_config)
    state_file = Path(restore_config["restore_context_path"])
    context.change_part_state(PartState.DOWNLOADED, make_part())
    assert not state_file.exists()

    context.add_failed_part(make_part(), ValueError("attach failed"))

    assert json.loads(state_file.read_text(encoding="utf-8")) == {
        "databases": {"db": {"table": {"all_1_1_0_7": "downloaded"}}},
        "failed": {
            "db": {
                "table": {
                    "failed_paths": [],
                    "failed_parts": {"all_1_1_0_7": "ValueError('attach failed')"},
                }
            }
        },
    }


@pytest.fixture(name="existing_state")
def fixture_existing_state(restore_config: dict) -> dict:
    state = {
        "databases": {
            "db": {"table": {"all_1_1_0_7": "restored", "all_2_2_0": "downloaded"}}
        },
        "failed": {},
    }
    Path(restore_config["restore_context_path"]).write_text(
        json.dumps(state), encoding="utf-8"
    )
    return state


def test_load_and_update_existing_state(restore_config, make_part, existing_state):
    context = RestoreContext(restore_config)
    assert context.part_restored(make_part())
    assert context.part_downloaded(make_part(name="all_2_2_0"))
    context.change_part_state(PartState.RESTORED, make_part(name="all_2_2_0"))
    context.dump_state()

    existing_state["databases"]["db"]["table"]["all_2_2_0"] = "restored"
    assert (
        json.loads(
            Path(restore_config["restore_context_path"]).read_text(encoding="utf-8")
        )
        == existing_state
    )


def test_dump_loads_existing_state_before_opening_file(restore_config, existing_state):
    RestoreContext(restore_config).dump_state()

    assert (
        json.loads(
            Path(restore_config["restore_context_path"]).read_text(encoding="utf-8")
        )
        == existing_state
    )


def test_unknown_part_keeps_not_downloaded_state_in_json(restore_config, make_part):
    context = RestoreContext(restore_config)
    assert not context.part_downloaded(make_part())
    context.dump_state()

    assert json.loads(
        Path(restore_config["restore_context_path"]).read_text(encoding="utf-8")
    ) == {
        "databases": {"db": {"table": {"all_1_1_0_7": "not_downloaded"}}},
        "failed": {},
    }
