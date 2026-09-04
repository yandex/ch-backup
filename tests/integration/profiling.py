"""Wall-time and resource evidence for integration runs, including their hooks."""

import json
import os
import threading
import time
from contextlib import ExitStack
from pathlib import Path

import docker
import psutil

ENVIRONMENT_LABEL = "ch-backup.integration.environment"


def record_stage(stage: str, started: float, success: bool) -> None:
    """Append stage timing only when profiling is requested by the coordinator."""
    destination = os.getenv("INTEGRATION_STAGE_PROFILE")
    if destination:
        with open(destination, "a", encoding="utf-8") as output:
            output.write(
                json.dumps(
                    {
                        "stage": stage,
                        "wall_seconds": time.monotonic() - started,
                        "success": success,
                    }
                )
                + "\n"
            )


class ResourceSampler:
    """Sample host, coordinator descendants and owned Docker containers."""

    def __init__(self, destination: Path, environments: list[str]) -> None:
        self.destination = destination
        self.environments = environments
        self.stopped = threading.Event()
        self.thread = threading.Thread(target=self._run, daemon=True)

    def start(self) -> None:
        """Start sampling without blocking test execution on Docker stats."""
        self.thread.start()

    def stop(self) -> None:
        """Finish the current sample and close the Docker client."""
        self.stopped.set()
        self.thread.join(timeout=30)

    def _run(self) -> None:
        client = None
        try:
            client = docker.from_env(timeout=2)
            with self.destination.open("a", encoding="utf-8") as output:
                while not self.stopped.is_set():
                    started = time.monotonic()
                    try:
                        sample = self._sample(client)
                    except Exception as error:
                        sample = {"time": time.time(), "error": str(error)}
                    output.write(json.dumps(sample) + "\n")
                    output.flush()
                    self.stopped.wait(max(0, 5 - (time.monotonic() - started)))
        except Exception as error:
            with self.destination.open("a", encoding="utf-8") as output:
                output.write(json.dumps({"error": str(error)}) + "\n")
        finally:
            if client is not None:
                client.close()

    def _sample(self, client: docker.DockerClient) -> dict:
        containers = []
        for container in client.containers.list(filters={"label": ENVIRONMENT_LABEL}):
            if self.stopped.is_set():
                break
            environment = container.labels.get(ENVIRONMENT_LABEL)
            if environment not in self.environments:
                continue
            try:
                stats = container.stats(stream=False, one_shot=True)
                containers.append(
                    {
                        "id": container.id,
                        "name": container.name,
                        "environment": environment,
                        "image": container.attrs["Image"],
                        "cpu": stats.get("cpu_stats"),
                        "memory": stats.get("memory_stats"),
                        "block_io": stats.get("blkio_stats"),
                    }
                )
            except docker.errors.NotFound:
                continue  # A scenario restart removed the container during sampling.
        processes = []
        parent = psutil.Process()
        for process in [parent, *parent.children(recursive=True)]:
            try:
                processes.append(
                    {
                        "pid": process.pid,
                        "ppid": process.ppid(),
                        "name": process.name(),
                        "created_at": process.create_time(),
                        "cpu": process.cpu_times()._asdict(),
                        "memory": process.memory_info()._asdict(),
                        "io": process.io_counters()._asdict(),
                    }
                )
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue
        return {
            "time": time.time(),
            "cpu": psutil.cpu_times()._asdict(),
            "cpu_count": psutil.cpu_count(),
            "memory": psutil.virtual_memory()._asdict(),
            "swap": psutil.swap_memory()._asdict(),
            "disk": psutil.disk_usage(str(self.destination.parent))._asdict(),
            "disk_io": psutil.disk_io_counters()._asdict(),
            "processes": processes,
            "containers": containers,
        }


def write_feature_profiles(results: Path, report: dict) -> None:
    """Partition samples by feature interval, environment and process ancestry."""
    resources = results / "resources.jsonl"
    if not resources.exists():
        return
    with ExitStack() as stack:
        targets = []
        for path, outcome in report["features"].items():
            if "finished_at" not in outcome:
                continue
            destination = results / "features" / Path(path).with_suffix("")
            output = stack.enter_context((destination / "resources.jsonl").open("w"))
            environment = report["workers"][outcome["worker"]]["environment"]
            targets.append((outcome, environment, output))
        source = stack.enter_context(resources.open())
        for line in source:
            sample = json.loads(line)
            for outcome, environment, output in targets:
                if (
                    not outcome["started_at"]
                    <= sample.get("time", 0)
                    <= outcome["finished_at"]
                ):
                    continue
                process_ids = {outcome["pid"]}
                processes = sample.get("processes", [])
                # Resolve ancestry even if psutil returned children out of order.
                for _ in processes:
                    descendants = {
                        p["pid"] for p in processes if p["ppid"] in process_ids
                    }
                    if descendants.issubset(process_ids):
                        break
                    process_ids.update(descendants)
                feature_sample = {
                    **sample,
                    "containers": [
                        c
                        for c in sample.get("containers", [])
                        if c["environment"] == environment
                    ],
                    "processes": [p for p in processes if p["pid"] in process_ids],
                }
                output.write(json.dumps(feature_sample) + "\n")
