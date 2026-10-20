"""Open-file limit inheritance."""

import subprocess
import sys
import textwrap


def test_raised_nofile_limit_is_inherited_by_child():
    """Use an isolated process so pytest's own limits remain unchanged."""
    script = textwrap.dedent(
        """
        import resource
        import subprocess
        import sys
        from ch_backup.util import raise_nofile_limit

        _, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        target = 4096 if hard == resource.RLIM_INFINITY else min(hard, 4096)
        assert target > 64
        resource.setrlimit(resource.RLIMIT_NOFILE, (64, target))
        raise_nofile_limit()
        assert resource.getrlimit(resource.RLIMIT_NOFILE) == (target, target)
        subprocess.run([
            sys.executable, "-c",
            f"import resource; assert resource.getrlimit(resource.RLIMIT_NOFILE) == ({target}, {target})",
        ], check=True, timeout=5)
        """
    )
    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        timeout=15,
        check=False,
    )
    assert result.returncode == 0, result.stderr
