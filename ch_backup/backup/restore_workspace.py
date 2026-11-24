"""Recognize the on-disk layout used to stage local parts during restore."""

from pathlib import Path

from ch_backup.backup.metadata.table_metadata import split_part_name

LOCAL_RESTORE_DIR = ".ch-backup-restore"


def is_local_restore_workspace(root: Path) -> bool:
    """Recognize the reserved staging layout, even before it contains parts."""
    if root.name != LOCAL_RESTORE_DIR or root.is_symlink() or not root.is_dir():
        return False
    for directory in root.iterdir():
        if (
            directory.name not in {"incoming", "previous"}
            or directory.is_symlink()
            or not directory.is_dir()
        ):
            return False
        for entry in directory.iterdir():
            try:
                split_part_name(entry.name)
            except ValueError:
                return False
            # An old detached destination may itself have been a symlink.
            if entry.is_symlink():
                if directory.name != "previous":
                    return False
            elif not entry.is_dir():
                return False
    return True
