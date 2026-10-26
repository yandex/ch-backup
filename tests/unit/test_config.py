"""
Unit tests for config module.
"""

from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

import pytest

from ch_backup.backup.copy_deduplication import copy_deduplication_cutoff
from ch_backup.ch_backup import ClickhouseBackup
from ch_backup.config import Config, as_datetime
from ch_backup.exceptions import ConfigurationError


@pytest.mark.parametrize(
    "value",
    [
        None,
        False,
        "false",
        7,
        [],
        {"months": 1},
        {"days": -1},
        {"days": True},
        {"days": "7"},
        {"days": float("inf")},
        {"seconds": float("nan")},
        {"days": 10**1000},
        {"days": 999999999},
    ],
)
def test_invalid_copy_deduplication_window(value):
    with pytest.raises(ConfigurationError, match="copy.deduplication_age_limit"):
        copy_deduplication_cutoff(
            {"deduplication_age_limit": value},
            datetime(2026, 10, 22, tzinfo=timezone.utc),
        )


@pytest.mark.parametrize(
    "value,age",
    [
        ({}, None),
        ({"days": 0}, None),
        ({"days": 7}, timedelta(days=7)),
        ({"hours": 1.5, "seconds": 2}, timedelta(hours=1.5, seconds=2)),
    ],
)
def test_copy_deduplication_window(value, age):
    now = datetime(2026, 10, 22, tzinfo=timezone.utc)
    expected = now - age if age is not None else None
    assert (
        copy_deduplication_cutoff({"deduplication_age_limit": value}, now) == expected
    )


def test_copy_window_config_precedence(tmp_path):
    main = tmp_path / "main.yaml"
    target = tmp_path / "target.yaml"
    main.write_text("copy:\n  deduplication_age_limit: {days: 7}\n")
    target.write_text("copy:\n  deduplication_age_limit: {days: 30}\n")
    config = Config(str(main))
    derived = config.derive(
        str(target), {"copy": {"deduplication_age_limit": {"days": 90}}}
    )
    assert config["copy"]["deduplication_age_limit"] == {"days": 7}
    assert derived["copy"]["deduplication_age_limit"] == {"days": 90}
    assert config.derive(str(target))["copy"]["deduplication_age_limit"] == {"days": 30}


def test_as_datetime():
    assert as_datetime(None) is None
    for value in [
        "2026-10-14T00:00:00Z",
        "2026-10-14T03:00:00+03:00",
        "2026-10-13T19:00:00-05:00",
        datetime(2026, 10, 14, 3, tzinfo=timezone(timedelta(hours=3))),
    ]:
        parsed = as_datetime(value)
        assert parsed == datetime(2026, 10, 14, tzinfo=timezone.utc), value
        assert parsed.tzinfo is timezone.utc

    for value in [
        "not-a-timestamp",
        "2026-02-30T00:00:00Z",
        "2026-10-14T00:00:00",
        "2026-10-14",
        datetime(2026, 10, 14),
        date(2026, 10, 14),
        "",
        123,
        False,
        [],
    ]:
        with pytest.raises(ValueError, match="ISO 8601 timestamp with a timezone"):
            as_datetime(value)


def test_derive(tmp_path: Path) -> None:
    main_config = tmp_path / "main.yaml"
    main_config.write_text(
        """
storage:
    credentials:
        endpoint_url: https://s3.test
        bucket: backups
backup:
    path_root: ch_backup
encryption:
    key: secret
"""
    )
    target_config = tmp_path / "target.yaml"
    target_config.write_text(
        """
storage:
    credentials:
        bucket: backups-copy
"""
    )
    config = Config(str(main_config))

    derived = config.derive(
        str(target_config), {"backup": {"path_root": "ch_backup_copy"}}
    )

    assert derived["storage"]["credentials"] == {
        "endpoint_url": "https://s3.test",
        "access_key_id": None,
        "secret_access_key": None,
        "bucket": "backups-copy",
    }
    assert derived["backup"]["path_root"] == "ch_backup_copy"
    assert derived["encryption"]["key"] == "secret"
    # The original config is not changed.
    assert config["storage"]["credentials"]["bucket"] == "backups"
    assert config["backup"]["path_root"] == "ch_backup"


def test_derive_without_changes(tmp_path: Path) -> None:
    main_config = tmp_path / "main.yaml"
    main_config.write_text("backup:\n    path_root: ch_backup\n")
    config = Config(str(main_config))

    derived = config.derive()
    derived["backup"]["path_root"] = "changed"

    assert config["backup"]["path_root"] == "ch_backup"


def test_copy_window_rejected_before_context_or_locks(tmp_path):
    path = tmp_path / "config.yaml"
    path.write_text("")
    config = Config(str(path))
    backup = ClickhouseBackup(config)
    invalid = config.derive(patch={"copy": {"deduplication_age_limit": {"days": -1}}})
    with (
        patch("ch_backup.ch_backup.BackupContext") as context,
        patch("ch_backup.ch_backup.BackupLayout") as layout,
        patch("ch_backup.ch_backup.storage_locks") as locks,
    ):
        with pytest.raises(ConfigurationError, match="copy.deduplication_age_limit"):
            ClickhouseBackup(invalid)
        with pytest.raises(ConfigurationError, match="copy.deduplication_age_limit"):
            backup.reload_config(invalid)
        with pytest.raises(ConfigurationError, match="copy.deduplication_age_limit"):
            backup.copy(invalid, ["new"])
        context.assert_not_called()
        layout.assert_not_called()
        locks.assert_not_called()
        assert backup.config is config
