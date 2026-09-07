"""Resource accounting must not mistake missing counters or restarts for capacity."""

import json

from tests.integration.profiling import disk_inodes, read_pressure, read_vmstat
from tests.integration.resource_summary import (
    feature_resources,
    host_resources,
    scheduler_resources,
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


def test_linux_pressure_vmstat_and_inode_readers_are_best_effort(tmp_path):
    pressure = tmp_path / "pressure"
    pressure.mkdir()
    (pressure / "cpu").write_text(
        "some avg10=1.25 avg60=2.50 avg300=3.75 total=12345\nmalformed value\n"
    )
    (pressure / "memory").write_text("full avg10=bad total=7\n")
    assert read_pressure(pressure) == {
        "cpu": {
            "some": {
                "avg10": 1.25,
                "avg60": 2.5,
                "avg300": 3.75,
                "total": 12345,
            }
        }
    }
    vmstat = tmp_path / "vmstat"
    vmstat.write_text("pgfault 10\noom_kill 2\n")
    assert read_vmstat(vmstat) == {"oom_kill": 2}
    assert not read_vmstat(tmp_path / "missing")
    assert disk_inodes(tmp_path)["available"] >= 0


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


def test_host_pressure_separates_busy_iowait_and_steal_and_tracks_counters():
    samples = [sample(0, 0, 0), sample(5, 0, 0)]
    samples[0].update(
        {
            "cpu": {"user": 0, "idle": 0, "iowait": 0, "steal": 0},
            "swap": {"used": 0, "sin": 100, "sout": 200},
            "vmstat": {"oom_kill": 4},
            "disk": {"free": 900},
            "disk_inodes": {"available": 90},
            "pressure": {"cpu": {"some": {"avg10": 1.0, "total": 1_000_000}}},
        }
    )
    samples[1].update(
        {
            "cpu": {"user": 10, "idle": 10, "iowait": 5, "steal": 5},
            "swap": {"used": 10, "sin": 112, "sout": 220},
            "vmstat": {"oom_kill": 5},
            "disk": {"free": 800},
            "disk_inodes": {"available": 80},
            "pressure": {"cpu": {"some": {"avg10": 3.0, "total": 3_500_000}}},
        }
    )
    metrics = host_resources(samples, {"features": {}})
    assert metrics["cpu_percent"]["mean"] == 100 / 3
    assert metrics["iowait_percent"]["mean"] == 100 / 6
    assert metrics["steal_percent"]["mean"] == 100 / 6
    assert metrics["pressure"]["cpu"]["some"]["total_seconds"] == 2.5
    assert metrics["swap_in_bytes"] == 12
    assert metrics["swap_out_bytes"] == 20
    assert metrics["oom_kill_delta"] == 1
    assert metrics["disk_free_min_bytes"] == 800
    assert metrics["disk_inodes_free_min"] == 80


def test_scheduler_summary_integrates_slots_and_queue_wait():
    report = {
        "jobs": 3,
        "features": {
            "a": {"queue_wait_seconds": 1},
            "b": {"queue_wait_seconds": 5},
            "c": {"queue_wait_seconds": 9, "queue_exit_reason": "failure"},
        },
        "scheduler": {
            "events": [
                {"time": 0, "slots_used": 0},
                {"time": 1, "slots_used": 2},
                {"time": 5, "slots_used": 3},
                {"time": 9, "slots_used": 1},
                {"time": 10, "slots_used": 0},
            ]
        },
    }
    metrics = scheduler_resources(report)
    assert metrics["slot_utilization_percent"] == 70
    assert metrics["slot_occupancy_seconds"] == {
        "0": 1.0,
        "2": 4.0,
        "3": 4.0,
        "1": 1.0,
    }
    assert metrics["queue_wait_seconds"]["p95"] == 9


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
