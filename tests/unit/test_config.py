"""
Unit tests for config module.
"""

from pathlib import Path

from ch_backup.config import Config


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
