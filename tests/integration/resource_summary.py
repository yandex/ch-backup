"""Summarize sampled resource demand without turning estimates into scheduling rules."""

import argparse
import json
import math
import os
from collections import Counter
from pathlib import Path
from typing import Sequence


def read_samples(path: Path) -> list[dict]:
    """Read available evidence, tolerating a truncated last sample after cancellation."""
    samples = []
    if path.exists():
        for line in path.read_text().splitlines():
            try:
                samples.append(json.loads(line))
            except json.JSONDecodeError:
                samples.append({"error": "Incomplete sample"})
    return samples


def distribution(values: Sequence[float]) -> dict:
    if not values:
        return {"mean": None, "p95": None, "max": None}
    ordered = sorted(values)
    return {
        "mean": sum(values) / len(values),
        "p95": ordered[math.ceil(len(ordered) * 0.95) - 1],
        "max": ordered[-1],
    }


def counters(sample: dict, kind: str) -> dict:
    """Keep identities so restarts and reused process IDs cannot create CPU spikes."""
    if kind == "containers":
        return {
            item["id"]: (item.get("cpu") or {})
            .get("cpu_usage", {})
            .get("total_usage", 0)
            / 1e9
            for item in sample.get(kind, [])
        }
    return {
        (item["pid"], item.get("created_at")): sum(
            item.get("cpu", {}).get(key, 0) for key in ("user", "system")
        )
        for item in sample.get(kind, [])
    }


def counter_delta(previous: dict, current: dict) -> float:
    return sum(
        current[key] - previous[key]
        for key in current.keys() & previous.keys()
        if current[key] >= previous[key]
    )


def cumulative_delta(samples: list[dict], *path: str) -> float | None:
    """Sum monotonic counter deltas while tolerating resets and missing fields."""

    def value(sample: dict) -> float | None:
        current = sample
        for key in path:
            if not isinstance(current, dict) or key not in current:
                return None
            current = current[key]
        return current if isinstance(current, (int, float)) else None

    total = 0.0
    observed = False
    for previous, current in zip(samples, samples[1:]):
        before, after = value(previous), value(current)
        if before is None or after is None or after < before:
            continue
        total += after - before
        observed = True
    return total if observed else None


def block_counters(sample: dict) -> dict:
    result = {}
    for item in sample.get("containers", []):
        entries = (item.get("block_io") or {}).get("io_service_bytes_recursive")
        if entries is not None:
            result[item["id"]] = sum(
                entry["value"]
                for entry in entries
                if entry["op"].lower() in ("read", "write")
            )
    return result


def io_rates(samples: list[dict], kind: str) -> dict:
    values = []
    before: dict
    after: dict
    for previous, current in zip(samples, samples[1:]):
        seconds = current["time"] - previous["time"]
        if not 0 < seconds <= 15:
            continue
        if kind == "processes":
            before, after = [
                {
                    (p["pid"], p.get("created_at")): p["io"]["read_bytes"]
                    + p["io"]["write_bytes"]
                    for p in sample.get("processes", [])
                    if p.get("io")
                }
                for sample in (previous, current)
            ]
        else:
            before, after = [
                (
                    {
                        "host": sample["disk_io"]["read_bytes"]
                        + sample["disk_io"]["write_bytes"]
                    }
                    if sample.get("disk_io")
                    else {}
                )
                for sample in (previous, current)
            ]
        if before.keys() & after.keys():
            values.append(counter_delta(before, after) / seconds)
    return distribution(values)


def active_features(report: dict, timestamp: float) -> list[str]:
    return sorted(
        path
        for path, outcome in report["features"].items()
        if outcome.get("started_at", math.inf)
        <= timestamp
        < outcome.get("finished_at", -math.inf)
    )


def container_memory(sample: dict) -> int:
    memory = [item.get("memory") or {} for item in sample.get("containers", [])]
    return sum(
        max(
            0,
            item.get("usage", 0)
            - item.get("stats", {}).get(
                "inactive_file", item.get("stats", {}).get("total_inactive_file", 0)
            ),
        )
        for item in memory
    )


def pressure_resources(samples: list[dict]) -> dict:
    """Summarize PSI avg10 observations and cumulative stall time."""
    result: dict[str, dict[str, dict]] = {}
    for resource in ("cpu", "memory", "io"):
        result[resource] = {}
        for kind in ("some", "full"):
            avg10 = [
                value
                for sample in samples
                if isinstance(
                    value := (
                        sample.get("pressure", {})
                        .get(resource, {})
                        .get(kind, {})
                        .get("avg10")
                    ),
                    (int, float),
                )
            ]
            total = cumulative_delta(samples, "pressure", resource, kind, "total")
            result[resource][kind] = {
                "avg10_percent": distribution(avg10),
                "total_seconds": None if total is None else total / 1_000_000,
            }
    return result


def scheduler_resources(report: dict) -> dict:
    """Calculate exact queue and slot metrics from scheduler transitions."""
    events = report.get("scheduler", {}).get("events", [])
    occupancy: dict[str, float] = {}
    elapsed = 0.0
    slot_seconds = 0.0
    for previous, current in zip(events, events[1:]):
        seconds = current.get("time", 0) - previous.get("time", 0)
        slots = previous.get("slots_used")
        if seconds < 0 or not isinstance(slots, int):
            continue
        elapsed += seconds
        slot_seconds += slots * seconds
        key = str(slots)
        occupancy[key] = occupancy.get(key, 0.0) + seconds
    jobs = report.get("jobs")
    utilization = (
        100 * slot_seconds / (jobs * elapsed)
        if isinstance(jobs, int) and jobs > 0 and elapsed > 0
        else None
    )
    waits = [
        outcome["queue_wait_seconds"]
        for outcome in report.get("features", {}).values()
        if isinstance(outcome.get("queue_wait_seconds"), (int, float))
    ]
    return {
        "elapsed_seconds": elapsed if events else None,
        "slot_utilization_percent": utilization,
        "slot_occupancy_seconds": occupancy,
        "queue_wait_seconds": distribution(waits),
    }


def feature_resources(samples: list[dict], report: dict) -> dict:
    memory = []
    docker_memory = []
    python_memory = []
    cpu = []
    block_io = []
    covered_seconds = 0.0
    valid = [s for s in samples if "error" not in s]
    for sample in valid:
        working_set = container_memory(sample)
        rss = sum(
            p.get("memory", {}).get("rss", 0) for p in sample.get("processes", [])
        )
        docker_memory.append(working_set)
        python_memory.append(rss)
        memory.append(working_set + rss)
    for previous, current in zip(valid, valid[1:]):
        seconds = current["time"] - previous["time"]
        # Do not average across lost samples or lengthy Docker-stat stalls.
        if not 0 < seconds <= 15:
            continue
        covered_seconds += seconds
        cpu.append(
            sum(
                counter_delta(counters(previous, kind), counters(current, kind))
                for kind in ("containers", "processes")
            )
            / seconds
        )
        before, after = block_counters(previous), block_counters(current)
        if before.keys() & after.keys():
            block_io.append(counter_delta(before, after) / seconds)
    return {
        "samples": len(valid),
        "sample_errors": len(samples) - len(valid),
        "cpu_observed_seconds": covered_seconds,
        "cpu_cores": distribution(cpu),
        "container_working_set_bytes": distribution(docker_memory),
        "python_rss_bytes": distribution(python_memory),
        "combined_memory_bytes": distribution(memory),
        "docker_block_bytes_per_second": distribution(block_io),
        "python_disk_bytes_per_second": io_rates(valid, "processes"),
        "neighbor_count_samples": dict(
            Counter(
                str(max(0, len(active_features(report, sample["time"])) - 1))
                for sample in valid
            )
        ),
    }


def host_resources(samples: list[dict], report: dict) -> dict:
    valid = [s for s in samples if "cpu" in s and "memory" in s]
    cpu = []
    iowait = []
    steal = []
    overlaps = []
    for previous, current in zip(valid, valid[1:]):
        delta = {
            key: value - previous["cpu"].get(key, value)
            for key, value in current["cpu"].items()
            if key not in ("guest", "guest_nice")
        }
        total = sum(delta.values())
        if total <= 0 or any(value < 0 for value in delta.values()):
            continue
        busy = (
            100
            * (
                total
                - delta.get("idle", 0)
                - delta.get("iowait", 0)
                - delta.get("steal", 0)
            )
            / total
        )
        cpu.append(busy)
        iowait.append(100 * delta.get("iowait", 0) / total)
        steal.append(100 * delta.get("steal", 0) / total)
        active = active_features(report, current["time"])
        if active:
            overlaps.append(
                {
                    "time": current["time"],
                    "cpu_percent": busy,
                    "available_memory_bytes": current["memory"]["available"],
                    "features": active,
                }
            )
    # Show different combinations, not five adjacent samples of the same group.
    busiest: dict[tuple[str, ...], dict] = {}
    for overlap in sorted(overlaps, key=lambda item: -item["cpu_percent"]):
        busiest.setdefault(tuple(overlap["features"]), overlap)
    cpu_count = next((s["cpu_count"] for s in valid if s.get("cpu_count")), None)
    if cpu_count is None:
        cpu_count = next(
            (
                c["cpu"]["online_cpus"]
                for s in valid
                for c in s.get("containers", [])
                if (c.get("cpu") or {}).get("online_cpus")
            ),
            None,
        )
    return {
        "cpu_count": cpu_count,
        "samples": len(valid),
        "sample_errors": sum("error" in s for s in samples),
        "cpu_percent": distribution(cpu),
        "iowait_percent": distribution(iowait),
        "steal_percent": distribution(steal),
        "pressure": pressure_resources(samples),
        "disk_bytes_per_second": io_rates(valid, "host"),
        "memory_total_bytes": valid[0]["memory"]["total"] if valid else None,
        "available_memory_min_bytes": min(
            (s["memory"]["available"] for s in valid), default=None
        ),
        "swap_used_max_bytes": max(
            (
                s["swap"]["used"]
                for s in valid
                if isinstance(s.get("swap", {}).get("used"), (int, float))
            ),
            default=None,
        ),
        "swap_in_bytes": cumulative_delta(valid, "swap", "sin"),
        "swap_out_bytes": cumulative_delta(valid, "swap", "sout"),
        "oom_kill_delta": cumulative_delta(valid, "vmstat", "oom_kill"),
        "disk_free_min_bytes": min(
            (s["disk"]["free"] for s in valid if "disk" in s), default=None
        ),
        "disk_inodes_free_min": min(
            (
                s["disk_inodes"]["available"]
                for s in valid
                if s.get("disk_inodes", {}).get("available") is not None
            ),
            default=None,
        ),
        "busiest_combinations": list(busiest.values())[:5],
    }


def format_value(value: float | None, divisor: float = 1) -> str:
    return "n/a" if value is None else f"{value / divisor:.2f}"


def markdown_summary(summary: dict) -> str:
    host = summary["host"]
    scheduler = summary["scheduler"]
    pressure = host["pressure"]
    lines = [
        "## Integration resource profile",
        f"Workers: {summary['jobs']}; CPUs: {host['cpu_count']}; "
        f"host CPU mean/p95: {format_value(host['cpu_percent']['mean'])}% / "
        f"{format_value(host['cpu_percent']['p95'])}%; "
        f"minimum available RAM: {format_value(host['available_memory_min_bytes'], 2**30)} GiB.",
        f"Host iowait mean/p95: {format_value(host['iowait_percent']['mean'])}% / "
        f"{format_value(host['iowait_percent']['p95'])}%; steal mean/p95: "
        f"{format_value(host['steal_percent']['mean'])}% / "
        f"{format_value(host['steal_percent']['p95'])}%.",
        "PSI avg10 p95 (cpu some / memory full / io full): "
        f"{format_value(pressure['cpu']['some']['avg10_percent']['p95'])}% / "
        f"{format_value(pressure['memory']['full']['avg10_percent']['p95'])}% / "
        f"{format_value(pressure['io']['full']['avg10_percent']['p95'])}%.",
        f"Swap peak/in/out: {format_value(host['swap_used_max_bytes'], 2**20)} / "
        f"{format_value(host['swap_in_bytes'], 2**20)} / "
        f"{format_value(host['swap_out_bytes'], 2**20)} MiB; OOM kills: "
        f"{format_value(host['oom_kill_delta'])}; minimum disk free: "
        f"{format_value(host['disk_free_min_bytes'], 2**30)} GiB; minimum free inodes: "
        f"{format_value(host['disk_inodes_free_min'])}.",
        f"Slot utilization: {format_value(scheduler['slot_utilization_percent'])}%; "
        f"queue wait p95/max: {format_value(scheduler['queue_wait_seconds']['p95'], 60)} / "
        f"{format_value(scheduler['queue_wait_seconds']['max'], 60)} min.",
        "",
        "CPU includes containers and the feature's Python process tree. Memory is simultaneous "
        "container working set plus Python RSS. Sampling every 5 seconds misses short peaks "
        "and short-lived processes; counter resets are excluded. RSS can count shared pages twice. "
        "n/a means unavailable, not zero. CPU percentiles describe observed intervals, not capacity guarantees.",
        "",
        "Use successful full runs with the same pinned images to compare 3 and 4 workers. "
        "High CPU demand alone does not prove that a feature needs exclusive execution. "
        "Correlate failures with neighbors and host pressure; confirm separately before changing tags.",
        "",
        "| Feature | Status | Slots / queue min | Wall min | Restart count / min | CPU cores mean / p95 | "
        "Peak GiB (containers / Python / simultaneous total) | Docker I/O p95 MiB/s | Neighbors (samples) |",
        "|---|---|---:|---:|---:|---:|---:|---:|---|",
    ]
    for path, item in sorted(
        summary["features"].items(),
        key=lambda pair: -(pair[1]["cpu_cores"]["p95"] or 0),
    ):
        restart = {"count": 0, "wall_seconds": 0.0}
        for name in ("environment:restart", "environment:restart_clickhouse"):
            for key in restart:
                restart[key] += item["stages"].get(name, {}).get(key, 0)
        lines.append(
            f"| {Path(path).stem} | {item['status']} | "
            f"{format_value(item.get('slots_reserved'))} / "
            f"{format_value(item.get('queue_wait_seconds'), 60)} | "
            f"{format_value(item.get('wall_seconds'), 60)} | "
            f"{restart.get('count', 0)} / {format_value(restart.get('wall_seconds', 0), 60)} | "
            f"{format_value(item['cpu_cores']['mean'])} / {format_value(item['cpu_cores']['p95'])} | "
            f"{format_value(item['container_working_set_bytes']['max'], 2**30)} / "
            f"{format_value(item['python_rss_bytes']['max'], 2**30)} / "
            f"{format_value(item['combined_memory_bytes']['max'], 2**30)} | "
            f"{format_value(item['docker_block_bytes_per_second']['p95'], 2**20)} | "
            f"{item['neighbor_count_samples']} |"
        )
    lines.extend(["", "Highest observed host CPU by feature combination:", ""])
    for overlap in host["busiest_combinations"]:
        lines.append(
            f"- {overlap['cpu_percent']:.1f}% CPU, "
            f"{overlap['available_memory_bytes'] / 2**30:.2f} GiB available: "
            + ", ".join(Path(p).stem for p in overlap["features"])
        )
    return "\n".join(lines) + "\n"


def write_resource_summary(results: Path, report: dict) -> dict:
    """Generate readable CI evidence from the same files retained in artifacts."""
    summary = {
        "jobs": report["jobs"],
        "run_id": report.get("run_id"),
        "preparation_seconds": report.get("preparation_seconds"),
        "workers": report.get("workers", {}),
        "features": {},
        "host": host_resources(read_samples(results / "resources.jsonl"), report),
        "scheduler": scheduler_resources(report),
    }
    for path, outcome in report["features"].items():
        directory = results / "features" / Path(path).with_suffix("")
        samples = read_samples(directory / "resources.jsonl")
        metrics = feature_resources(samples, report)
        metrics["host_during_feature"] = host_resources(samples, report)
        metrics["neighbors"] = sorted(
            {
                neighbor
                for sample in samples
                if "time" in sample
                for neighbor in active_features(report, sample["time"])
                if neighbor != path
            }
        )
        stages: dict[str, dict] = {}
        for stage in read_samples(directory / "stages.jsonl"):
            name = stage.get("stage", "")
            if name.startswith("environment:"):
                total = stages.setdefault(name, {"count": 0, "wall_seconds": 0.0})
                total["count"] += 1
                total["wall_seconds"] += stage["wall_seconds"]
        summary["features"][path] = {
            **metrics,
            "status": outcome["status"],
            "wall_seconds": outcome.get("wall_seconds"),
            "slots_reserved": outcome.get("slots_reserved"),
            "queue_wait_seconds": outcome.get("queue_wait_seconds"),
            "queue_exit_reason": outcome.get("queue_exit_reason"),
            "stages": stages,
        }
    (results / "resource-summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    markdown = markdown_summary(summary)
    (results / "resource-summary.md").write_text(markdown)
    if destination := os.getenv("GITHUB_STEP_SUMMARY"):
        with open(destination, "a", encoding="utf-8") as output:
            output.write(markdown)
    print(f"Resource profile: {results / 'resource-summary.md'}", flush=True)
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("results", type=Path)
    args = parser.parse_args()
    write_resource_summary(
        args.results, json.loads((args.results / "summary.json").read_text())
    )


if __name__ == "__main__":
    main()
