"""Resource accounting must not mistake missing counters or restarts for capacity."""

import json

from tests.integration.resource_summary import (
    feature_resources,
    write_resource_summary,
)


def sample(timestamp, container_cpu, process_cpu, identity="container"):
    return {
        "time": timestamp,
        "cpu_count": 4,
        "cpu": {"user": timestamp * 2, "idle": timestamp * 2},
        "memory": {"total": 16000, "available": 12000},
        "containers": [
            {
                "id": identity,
                "cpu": {"cpu_usage": {"total_usage": container_cpu * 1e9}},
                "memory": {"usage": 100, "stats": {"inactive_file": 20}},
                "block_io": {"io_service_bytes_recursive": None},
            }
        ],
        "processes": [
            {
                "pid": 42,
                "created_at": 0,
                "cpu": {"user": process_cpu, "system": 0, "children_user": 900},
                "memory": {"rss": 30},
            }
        ],
    }


def test_cpu_includes_python_without_double_counting_children_and_memory_is_simultaneous():
    samples = [sample(0, 0, 0), sample(5, 10, 5)]
    metrics = feature_resources(samples, {"features": {}})
    assert metrics["cpu_cores"]["mean"] == 3
    assert metrics["combined_memory_bytes"]["max"] == 110
    assert metrics["docker_block_bytes_per_second"]["p95"] is None
    assert metrics["python_disk_bytes_per_second"]["p95"] is None


def test_counter_restarts_and_pid_reuse_do_not_create_spikes():
    samples = [sample(0, 100, 100), sample(5, 1, 1), sample(10, 200, 200, "new")]
    samples[-1]["processes"][0]["created_at"] = 8
    metrics = feature_resources(samples, {"features": {}})
    assert metrics["cpu_cores"]["max"] == 0
    assert metrics["cpu_observed_seconds"] == 10


def test_io_does_not_count_total_twice_and_ignores_missing_intervals():
    samples = [sample(0, 0, 0), sample(5, 0, 0), sample(10, 0, 0)]
    for index, item in enumerate(samples[1:], 1):
        item["containers"][0]["block_io"]["io_service_bytes_recursive"] = [
            {"op": "Read", "value": 100 * index},
            {"op": "Write", "value": 200 * index},
            {"op": "Total", "value": 300 * index},
        ]
        item["processes"][0]["io"] = {
            "read_bytes": 10 * index,
            "write_bytes": 20 * index,
        }
    metrics = feature_resources(samples, {"features": {}})
    assert metrics["docker_block_bytes_per_second"]["mean"] == 60
    assert metrics["python_disk_bytes_per_second"]["mean"] == 6


def test_summary_preserves_failures_neighbors_and_outer_restart_timing(
    tmp_path, monkeypatch
):
    path = "tests/a.feature"
    directory = tmp_path / "features/tests/a"
    directory.mkdir(parents=True)
    report = {
        "jobs": 4,
        "features": {
            path: {
                "started_at": 0,
                "finished_at": 11,
                "wall_seconds": 11,
                "status": "failed",
            },
            "tests/b.feature": {"started_at": 0, "finished_at": 6, "status": "passed"},
            "tests/c.feature": {"status": "not_run"},
        },
    }
    samples = [sample(1, 1, 1), sample(6, 6, 1)]
    samples[-1]["containers"][0]["memory"]["usage"] = 20
    samples[-1]["processes"][0]["memory"]["rss"] = 80
    data = "\n".join(json.dumps(s) for s in samples) + '\n{"time":'
    (tmp_path / "resources.jsonl").write_text(data)
    (directory / "resources.jsonl").write_text(data)
    (directory / "stages.jsonl").write_text(
        "\n".join(
            json.dumps(s)
            for s in [
                {"stage": "environment:restart", "wall_seconds": 5},
                {"stage": "start_compose", "wall_seconds": 4},
            ]
        )
    )
    step_summary = tmp_path / "job.md"
    monkeypatch.setenv("GITHUB_STEP_SUMMARY", str(step_summary))
    summary = write_resource_summary(tmp_path, report)
    metrics = summary["features"][path]
    assert metrics["status"] == "failed"
    assert metrics["stages"] == {"environment:restart": {"count": 1, "wall_seconds": 5}}
    assert metrics["neighbors"] == ["tests/b.feature"]
    assert metrics["neighbor_count_samples"] == {"1": 1, "0": 1}
    assert metrics["combined_memory_bytes"]["max"] == 110  # Not 80 + 80.
    assert summary["host"]["cpu_percent"]["p95"] == 50
    assert summary["host"]["sample_errors"] == 1
    assert summary["features"]["tests/c.feature"]["cpu_cores"]["mean"] is None
    assert "n/a" in step_summary.read_text()
