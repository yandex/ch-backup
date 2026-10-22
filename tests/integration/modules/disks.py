"""Exercise command quoting against the installed clickhouse-disks binary."""

# The regression exercises the private helpers used by backup and restore.
# pylint: disable=protected-access

from pathlib import Path
from subprocess import check_output
from tempfile import TemporaryDirectory
from unittest.mock import Mock, patch

from ch_backup.clickhouse import disks


def check_literal_paths() -> None:
    """Copy and remove literal paths through both supported CLI interfaces."""
    version_text = check_output(["clickhouse-client", "--version"], text=True)
    version = tuple(map(int, version_text.split()[3].split(".")[:2]))
    ch_ctl = Mock()
    ch_ctl.ch_version_ge.side_effect = lambda minimum: version >= tuple(
        map(int, minimum.split("."))
    )
    names = [
        "path with spaces",
        "tab\tand\nnewline",
        "single'quote",
        'double"quote',
        r"back\slash\n",
        "$(printf expanded)`printf expanded`",
        "$HOME * ? [abc] {a,b} & | < > ~",
        "file; remove --recursive guard;",
        "mixed'\";\\ name",
    ]
    with TemporaryDirectory(prefix="disks quoting ") as directory:
        root = Path(directory)
        source, target = root / "source", root / "target"
        source.mkdir()
        target.mkdir()
        guard = target / "guard"
        guard.write_text("keep")
        config = root / "config with spaces.xml"
        disks._render_disks_config(
            str(config),
            {
                "source": {"type": "local", "path": str(source) + "/"},
                "target": {"type": "local", "path": str(target) + "/"},
            },
            history_file=str(root / "history"),
        )
        with patch.object(disks, "CH_DISK_CONFIG_PATH", str(config)):
            for name in names:
                (source / name).write_text(name)
                disks._ch_disks_copy(ch_ctl, "source", name, "target", name, name)
                assert (target / name).read_text() == name
                disks._ch_disks_remove(ch_ctl, "target", name, name)
                assert not (target / name).exists()
                assert (source / name).read_text() == name
                assert guard.read_text() == "keep"
    print(f"Literal copy/remove paths passed on ClickHouse {version_text.strip()}")


if __name__ == "__main__":
    check_literal_paths()
