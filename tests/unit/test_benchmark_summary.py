"""The worker-count recommendation must follow the published thresholds."""

import json

import pytest

from tests.integration.benchmark_summary import compare


def write_artifact(
    root,
    mode,
    repeat,
    wall_seconds,
    long_seconds,
    *,
    version="26.8.2.7",
    attempt=1,
    status="passed",
    oom_kills=0,
):
    artifact = root / (
        f"integration-benchmark-{mode}-repeat-{repeat}-attempt-{attempt}"
    )
    benchmark = artifact / "benchmark/run"
    results = artifact / "parallel/run/results"
    benchmark.mkdir(parents=True)
    results.mkdir(parents=True)
    (benchmark / "benchmark.json").write_text(
        json.dumps(
            {
                "mode": str(mode),
                "clickhouse_version": version,
                "python_version": "3.10.19",
                "git_sha": "a" * 40,
                "status": status,
                "returncode": 0 if status == "passed" else 1,
                "wall_seconds": wall_seconds,
                "cleanup_errors": [],
            }
        )
    )
    (results / "summary.json").write_text(
        json.dumps(
            {
                "jobs": mode,
                "status": status,
                "selected_features": 2,
                "features": {
                    "tests/long.feature": {
                        "status": "passed",
                        "wall_seconds": long_seconds,
                    },
                    "tests/short.feature": {
                        "status": "passed",
                        "wall_seconds": 599,
                    },
                },
            }
        )
    )
    (results / "resource-summary.json").write_text(
        json.dumps({"host": {"oom_kill_delta": oom_kills}})
    )


def write_comparison(root, wall_4=850, long_4=720, **kwargs):
    for mode in (3, 4):
        for repeat in (1, 2):
            write_artifact(
                root,
                mode,
                repeat,
                1000 if mode == 3 else wall_4,
                600 if mode == 3 else long_4,
                **kwargs,
            )


def test_boundary_speedup_and_long_feature_growth_recommend_four(tmp_path):
    write_comparison(tmp_path)
    result = compare(tmp_path)
    assert result["decision"] == "use_4"
    assert result["speedup_percent"] == pytest.approx(15)
    assert result["long_features"]["tests/long.feature"]["growth_percent"] == (
        pytest.approx(20)
    )
    assert "tests/short.feature" not in result["long_features"]


@pytest.mark.parametrize(
    "wall_4,long_4,reason",
    [
        (851, 720, "speedup"),
        (850, 721, "tests/long.feature"),
    ],
)
def test_threshold_failure_keeps_three(tmp_path, wall_4, long_4, reason):
    write_comparison(tmp_path, wall_4, long_4)
    result = compare(tmp_path)
    assert result["decision"] == "keep_3"
    assert any(reason in item for item in result["reasons"])


@pytest.mark.parametrize("kwargs", [{"oom_kills": None}, {"status": "failed"}])
def test_failure_or_missing_oom_evidence_keeps_three(tmp_path, kwargs):
    write_comparison(tmp_path, **kwargs)
    assert compare(tmp_path)["decision"] == "keep_3"


def test_mismatched_version_or_missing_artifact_is_invalid(tmp_path):
    write_comparison(tmp_path)
    artifact = tmp_path / "integration-benchmark-4-repeat-2-attempt-1"
    report = next(artifact.rglob("benchmark.json"))
    data = json.loads(report.read_text())
    data["clickhouse_version"] = "26.9.1.1"
    report.write_text(json.dumps(data))
    with pytest.raises(ValueError, match="different or unpinned"):
        compare(tmp_path)

    report.write_text(json.dumps({**data, "clickhouse_version": "26.8.2.7"}))
    artifact.rename(artifact.with_name("ignored"))
    with pytest.raises(ValueError, match="Expected exactly"):
        compare(tmp_path)


def test_rerun_attempt_keeps_three(tmp_path):
    write_comparison(tmp_path, attempt=2)
    result = compare(tmp_path)
    assert result["decision"] == "keep_3"
    assert all(not run["passed"] for runs in result["runs"].values() for run in runs)
