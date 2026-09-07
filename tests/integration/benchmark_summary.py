"""Compare two three-slot and two four-slot integration benchmark artifacts."""

import argparse
import json
import os
import re
from pathlib import Path
from statistics import median

MIN_SPEEDUP = 0.15
MAX_LONG_FEATURE_GROWTH = 0.20
LONG_FEATURE_SECONDS = 600
ARTIFACT = re.compile(r"integration-benchmark-(3|4)-repeat-(1|2)-attempt-(\d+)")


def _read_single(directory: Path, name: str, parent: str) -> dict:
    paths = [path for path in directory.rglob(name) if parent in path.parts]
    if len(paths) != 1:
        raise ValueError(
            f"Expected one {parent}/.../{name} in {directory}, found {len(paths)}"
        )
    return json.loads(paths[0].read_text())


def _load_artifacts(root: Path) -> list[dict]:
    artifacts: list[dict] = []
    for directory in sorted(root.iterdir()):
        match = ARTIFACT.fullmatch(directory.name)
        if not directory.is_dir() or match is None:
            continue
        mode, repeat, attempt = map(int, match.groups())
        benchmark = _read_single(directory, "benchmark.json", "benchmark")
        summary = _read_single(directory, "summary.json", "parallel")
        resources = _read_single(directory, "resource-summary.json", "parallel")
        if benchmark.get("mode") != str(mode) or summary.get("jobs") != mode:
            raise ValueError(f"Mode mismatch in {directory}")
        if not str(benchmark.get("python_version", "")).startswith("3.10."):
            raise ValueError(f"Benchmark did not use Python 3.10 in {directory}")
        features = summary.get("features", {})
        if not features or summary.get("selected_features") != len(features):
            raise ValueError(f"Incomplete feature inventory in {directory}")
        artifacts.append(
            {
                "artifact": directory.name,
                "mode": mode,
                "repeat": repeat,
                "attempt": attempt,
                "benchmark": benchmark,
                "summary": summary,
                "resources": resources,
            }
        )
    expected = {(mode, repeat) for mode in (3, 4) for repeat in (1, 2)}
    actual = {(item["mode"], item["repeat"]) for item in artifacts}
    if len(artifacts) != 4 or actual != expected:
        raise ValueError(
            "Expected exactly repeats 1 and 2 for modes 3 and 4; "
            f"found {sorted(actual)} in {len(artifacts)} artifacts"
        )
    versions = {item["benchmark"].get("clickhouse_version") for item in artifacts}
    if len(versions) != 1 or not re.fullmatch(
        r"\d+(?:\.\d+){3}", str(next(iter(versions)))
    ):
        raise ValueError(f"Benchmarks use different or unpinned versions: {versions}")
    shas = {item["benchmark"].get("git_sha") for item in artifacts}
    if len(shas) != 1 or not next(iter(shas)):
        raise ValueError(f"Benchmarks use different or missing git SHAs: {shas}")
    return artifacts


def compare(root: Path) -> dict:  # pylint: disable=too-many-locals
    """Return a conservative recommendation from complete benchmark evidence."""
    artifacts = _load_artifacts(root)
    version = artifacts[0]["benchmark"]["clickhouse_version"]
    reasons = []
    runs: dict[str, list[dict]] = {"3": [], "4": []}
    feature_times: dict[int, dict[str, list[float]]] = {3: {}, 4: {}}
    for item in artifacts:
        benchmark = item["benchmark"]
        summary = item["summary"]
        resources = item["resources"]
        mode = item["mode"]
        passed = (
            item["attempt"] == 1
            and benchmark.get("status") == "passed"
            and benchmark.get("returncode") == 0
            and not benchmark.get("cleanup_errors")
            and summary.get("status") == "passed"
        )
        oom_kills = resources.get("host", {}).get("oom_kill_delta")
        if oom_kills != 0:
            passed = False
            reasons.append(
                f"{item['artifact']}: OOM evidence is {oom_kills!r}, expected 0"
            )
        if not passed:
            reasons.append(f"{item['artifact']}: benchmark did not pass first attempt")
        wall = benchmark.get("wall_seconds")
        if not isinstance(wall, (int, float)) or wall <= 0:
            raise ValueError(f"Invalid wall_seconds in {item['artifact']}: {wall!r}")
        runs[str(mode)].append(
            {
                "artifact": item["artifact"],
                "repeat": item["repeat"],
                "wall_seconds": wall,
                "passed": passed,
            }
        )
        for feature, outcome in summary.get("features", {}).items():
            duration = outcome.get("wall_seconds")
            if outcome.get("status") != "passed" or not isinstance(
                duration, (int, float)
            ):
                continue
            feature_times[mode].setdefault(feature, []).append(duration)

    wall_medians = {
        mode: median(run["wall_seconds"] for run in values)
        for mode, values in runs.items()
    }
    speedup = 1 - wall_medians["4"] / wall_medians["3"]
    if speedup < MIN_SPEEDUP:
        reasons.append(f"Four-worker median speedup is only {100 * speedup:.2f}%")

    long_features = {}
    if all(run["passed"] for values in runs.values() for run in values):
        feature_sets = {
            mode: {feature for feature, values in timings.items() if len(values) == 2}
            for mode, timings in feature_times.items()
        }
        if feature_sets[3] != feature_sets[4]:
            raise ValueError("Passed feature sets differ between modes or repeats")
        for feature in sorted(feature_sets[3]):
            baseline = median(feature_times[3][feature])
            if baseline < LONG_FEATURE_SECONDS:
                continue
            candidate = median(feature_times[4][feature])
            growth = candidate / baseline - 1
            within_limit = growth <= MAX_LONG_FEATURE_GROWTH
            if not within_limit:
                reasons.append(f"{feature}: median grew by {100 * growth:.2f}%")
            long_features[feature] = {
                "workers_3_median_seconds": baseline,
                "workers_4_median_seconds": candidate,
                "growth_percent": 100 * growth,
                "within_limit": within_limit,
            }

    return {
        "valid": True,
        "decision": "use_4" if not reasons else "keep_3",
        "clickhouse_version": version,
        "criteria": {
            "minimum_speedup_percent": 100 * MIN_SPEEDUP,
            "long_feature_minimum_seconds": LONG_FEATURE_SECONDS,
            "maximum_long_feature_growth_percent": 100 * MAX_LONG_FEATURE_GROWTH,
        },
        "runs": runs,
        "wall_seconds_median": wall_medians,
        "speedup_percent": 100 * speedup,
        "long_features": long_features,
        "reasons": reasons,
    }


def markdown(result: dict) -> str:
    """Render comparison evidence for the Actions job summary."""
    lines = [
        "## Integration benchmark comparison",
        "",
        f"Decision: **{result['decision']}**",
        "",
    ]
    if not result.get("valid"):
        lines.extend([f"- {reason}" for reason in result["reasons"]])
        return "\n".join(lines) + "\n"
    lines.extend(
        [
            f"ClickHouse: `{result['clickhouse_version']}`; median speedup: "
            f"{result['speedup_percent']:.2f}%.",
            "",
            "| Workers | Median wall min |",
            "|---:|---:|",
            f"| 3 | {result['wall_seconds_median']['3'] / 60:.2f} |",
            f"| 4 | {result['wall_seconds_median']['4'] / 60:.2f} |",
            "",
            "| Long feature | 3-worker min | 4-worker min | Growth | Within 20% |",
            "|---|---:|---:|---:|:---:|",
        ]
    )
    for feature, values in result["long_features"].items():
        lines.append(
            f"| {Path(feature).stem} | "
            f"{values['workers_3_median_seconds'] / 60:.2f} | "
            f"{values['workers_4_median_seconds'] / 60:.2f} | "
            f"{values['growth_percent']:.2f}% | "
            f"{'yes' if values['within_limit'] else 'no'} |"
        )
    if result["reasons"]:
        lines.extend(["", "Reasons to keep three workers:", ""])
        lines.extend(f"- {reason}" for reason in result["reasons"])
    return "\n".join(lines) + "\n"


def write_result(result: dict, output: Path) -> None:
    output.mkdir(parents=True, exist_ok=True)
    (output / "benchmark-comparison.json").write_text(
        json.dumps(result, indent=2) + "\n"
    )
    text = markdown(result)
    (output / "benchmark-comparison.md").write_text(text)
    if destination := os.getenv("GITHUB_STEP_SUMMARY"):
        with open(destination, "a", encoding="utf-8") as summary:
            summary.write(text)


def cli_main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("artifacts", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    try:
        result = compare(args.artifacts)
    except (OSError, ValueError, json.JSONDecodeError) as error:
        result = {"valid": False, "decision": "keep_3", "reasons": [str(error)]}
        write_result(result, args.output)
        raise SystemExit(2) from error
    write_result(result, args.output)


if __name__ == "__main__":
    cli_main()
