"""Select whole Behave features and schedule them within a slot budget."""

from dataclasses import dataclass
from pathlib import Path
from typing import Sequence

from behave.configuration import Configuration
from behave.parser import parse_file


@dataclass(frozen=True)
class Feature:
    """A repository-relative feature and its scheduling information."""

    path: str
    scenarios: int
    tags: frozenset[str]

    def slots(self, jobs: int) -> int:
        """Reserve the entire budget for exclusive features."""
        if "parallel_exclusive" in self.tags:
            return jobs
        return min(2, jobs) if "parallel_heavy" in self.tags else 1


def load_features(
    root: Path, featureset: Path, behave_args: Sequence[str]
) -> list[Feature]:
    """Validate the canonical list before selecting features with Behave filters."""
    config = Configuration(list(behave_args))
    if config.paths:
        raise ValueError("Use INTEGRATION_FEATURESET instead of positional BEHAVE_ARGS")
    if config.dry_run:
        raise ValueError("Use the parallel runner's --dry-run option")
    selected = []
    seen = set()
    for line_number, raw_line in enumerate(featureset.read_text().splitlines(), 1):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        path = (featureset.parent / line).resolve()
        if path in seen:
            raise ValueError(f"Duplicate feature at {featureset}:{line_number}: {line}")
        seen.add(path)
        relative = path.relative_to(root).as_posix()
        if not relative.startswith("tests/") or not path.is_file():
            raise ValueError(f"Feature must be an existing file under tests/: {path}")
        model = parse_file(str(path))
        scenarios = [s for s in model.walk_scenarios() if s.should_run(config)]
        if config.exclude(relative) or not scenarios:
            continue
        selected.append(Feature(relative, len(scenarios), frozenset(model.tags)))
    if not selected:
        raise ValueError("No features selected")
    return selected


class FeatureQueue:
    """A single-coordinator queue; callers reserve and release slots explicitly."""

    def __init__(self, features: Sequence[Feature], jobs: int) -> None:
        if jobs < 1:
            raise ValueError("INTEGRATION_JOBS must be positive")
        self.jobs = jobs
        self.pending = sorted(features, key=lambda feature: -feature.scenarios)
        self.active: dict[str, Feature] = {}
        self.stopped = False

    def take(self) -> Feature | None:
        """Backfill available slots without scheduling past an exclusive barrier."""
        if self.stopped:
            return None
        available = self.jobs - sum(f.slots(self.jobs) for f in self.active.values())
        for index, feature in enumerate(self.pending):
            if feature.slots(self.jobs) <= available:
                self.pending.pop(index)
                self.active[feature.path] = feature
                return feature
            if "parallel_exclusive" in feature.tags:
                break
        return None

    def finish(self, feature: Feature, success: bool) -> None:
        """A failed feature closes admission while active features finish."""
        del self.active[feature.path]
        if not success:
            self.stopped = True
