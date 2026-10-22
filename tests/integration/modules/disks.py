"""Exercise command quoting against the installed clickhouse-disks binary."""

# The regression exercises the private helpers used by backup and restore.
# pylint: disable=protected-access

from pathlib import Path
from subprocess import check_output
from tempfile import TemporaryDirectory
from unittest.mock import Mock, patch

from packaging.version import Version

from ch_backup.clickhouse import disks


def check_literal_paths() -> None:
    """Copy and remove literal paths through both supported CLI interfaces."""
    version = check_output(["clickhouse-client", "--version"], text=True).split()[3]
    ch_ctl = Mock(ch_version_ge=lambda minimum: Version(version) >= Version(minimum))
    with TemporaryDirectory(prefix="disks quoting ") as directory:
        root = Path(directory)
        guard = root / "guard"
        guard.write_text("keep")
        config = root / "config with spaces.xml"
        disks._render_disks_config(
            str(config),
            {"test": {"type": "local", "path": directory + "/"}},
            history_file=str(root / "history"),
        )
        with patch.object(disks, "CH_DISK_CONFIG_PATH", str(config)):
            for name in (
                "with spaces",
                "'\"\\\t\n$HOME $(printf x)`printf x` * ?; remove guard;",
            ):
                source, target = "source " + name, "target " + name
                (root / source).write_text("data")
                disks._ch_disks_copy(ch_ctl, "test", source, "test", target, "part")
                assert (root / target).read_text() == "data"
                disks._ch_disks_remove(ch_ctl, "test", target, "part")
                assert not (root / target).exists()
                assert (root / source).read_text() == "data"
                assert guard.read_text() == "keep"


if __name__ == "__main__":
    check_literal_paths()
