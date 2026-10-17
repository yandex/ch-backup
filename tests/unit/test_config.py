"""
Unit tests for config module.
"""

from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest

from ch_backup.config import Config, as_datetime


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
