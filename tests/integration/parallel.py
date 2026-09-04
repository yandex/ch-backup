"""Run isolated whole-feature Behave processes on one Docker host."""

import argparse
import json
import os
import shlex
import shutil
import signal
import subprocess
import sys
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from types import FrameType
from typing import IO

from ch_backup import logging
from tests.integration.feature_queue import Feature, FeatureQueue, load_features
from tests.integration.parallel_runtime import (
    cleanup_environment,
    image_inventory,
    read_outcome,
    remove_image_tags,
    snapshot,
)
from tests.integration.profiling import ResourceSampler, write_feature_profiles


@dataclass
class Worker:
    """An isolated workspace with images prepared once for the whole run."""

    path: Path
    environment: str

    def env(self, output: Path) -> dict[str, str]:
        """Each child writes its stage timings and outcomes into its own report."""
        return {
            **os.environ,
            "INTEGRATION_ENV_ID": self.environment,
            "INTEGRATION_STAGE_PROFILE": str(output / "stages.jsonl"),
            "INTEGRATION_FEATURE_RESULT": str(output / "outcome.json"),
            "PYTHONDONTWRITEBYTECODE": "1",
        }


@dataclass
class RunningFeature:
    """Process and output handles owned by the coordinator."""

    worker: Worker
    feature: Feature
    process: subprocess.Popen
    output: Path
    log: IO
    started: float


class ParallelRun:
    """Coordinate admission, completion, signals and cleanup in one thread."""

    def __init__(
        self, root: Path, features: list[Feature], jobs: int, behave_args: list[str]
    ) -> None:
        self.root = root
        self.queue = FeatureQueue(features, jobs)
        self.behave_args = behave_args
        self.run_id = uuid.uuid4().hex
        self.directory = root / "staging/parallel" / self.run_id
        self.results = self.directory / "results"
        self.workers = [
            Worker(self.directory / f"worker-{index}", f"chb-{self.run_id}-w{index}")
            for index in range(1, min(jobs, len(features)) + 1)
        ]
        self.cancelled = 0
        self.active: list[RunningFeature] = []
        self.report: dict = {
            "run_id": self.run_id,
            "jobs": jobs,
            "selected_features": len(features),
            "selected_scenarios": sum(f.scenarios for f in features),
            "python": sys.version,
            "clickhouse_requested": os.getenv("CLICKHOUSE_VERSION", "latest"),
            "features": {f.path: {"status": "not_run"} for f in features},
            "workers": {},
            "errors": [],
            "status": "running",
        }

    def _signal(self, signum: int, _frame: FrameType | None) -> None:
        self.cancelled = signum
        self.queue.stopped = True

    def _save(self) -> None:
        temporary = self.results / "summary.tmp"
        temporary.write_text(json.dumps(self.report, indent=2) + "\n")
        temporary.replace(self.results / "summary.json")

    def execute(self) -> int:
        """Always retain a final report, including setup and cancellation failures."""
        started = time.monotonic()
        self.results.mkdir(parents=True)
        handlers = {
            sig: signal.signal(sig, self._signal)
            for sig in (signal.SIGINT, signal.SIGTERM)
        }
        sampler = ResourceSampler(
            self.results / "resources.jsonl", [w.environment for w in self.workers]
        )
        self._save()
        print(f"Integration results: {self.results}", flush=True)
        sampler.start()
        try:
            # Freeze inputs before slow builds; all workers must test the same files.
            snapshot(self.root, self.directory / "source")
            for worker in self.workers:
                if self.cancelled:
                    break
                self._prepare(worker)
                self._save()
            self.report["preparation_seconds"] = time.monotonic() - started
            if not self.cancelled:
                self._schedule()
        except Exception as error:
            self.queue.stopped = True
            self.report["errors"].append(str(error))
            logging.error("Parallel integration run failed: {}", error)
        finally:
            self._finish_active()
            for worker in self.workers:
                self._cleanup(worker)
            sampler.stop()
            try:
                write_feature_profiles(self.results, self.report)
            except Exception as error:
                self.report["errors"].append(f"Writing resource profiles: {error}")
            self.report["wall_seconds"] = time.monotonic() - started
            self.report["signal"] = self.cancelled or None
            successful = (
                not self.cancelled
                and not self.report["errors"]
                and all(
                    outcome["status"] in ("passed", "skipped")
                    for outcome in self.report["features"].values()
                )
            )
            self.report["status"] = (
                "cancelled"
                if self.cancelled
                else ("passed" if successful else "failed")
            )
            self._save()
            for sig, handler in handlers.items():
                signal.signal(sig, handler)
        print(
            f"Integration {self.report['status']}: {self.results / 'summary.json'}",
            flush=True,
        )
        return 128 + self.cancelled if self.cancelled else int(not successful)

    def _wait(self, process: subprocess.Popen) -> int:
        interrupted = None
        while process.poll() is None:
            if self.cancelled:
                if interrupted is None:
                    interrupted = time.monotonic()
                    self._kill_group(process, signal.SIGTERM)
                elif time.monotonic() - interrupted > 15:
                    self._kill_group(process, signal.SIGKILL)
            time.sleep(0.2)
        return process.returncode

    @staticmethod
    def _kill_group(process: subprocess.Popen, signum: int) -> None:
        try:
            os.killpg(process.pid, signum)
        except ProcessLookupError:
            pass

    def _prepare(self, worker: Worker) -> None:
        started = time.monotonic()
        output = self.results / worker.path.name
        output.mkdir()
        snapshot(self.directory / "source", worker.path)
        print(f"Preparing {worker.path.name}", flush=True)
        with (output / "setup.log").open("w") as log:
            with subprocess.Popen(
                [sys.executable, "-m", "tests.integration.env_control", "create"],
                cwd=worker.path,
                env=worker.env(output),
                stdout=log,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            ) as process:
                code = self._wait(process)
        images = image_inventory(worker.path)
        self.report["workers"][worker.path.name] = {
            "environment": worker.environment,
            "images": images,
            "setup_seconds": time.monotonic() - started,
        }
        if code:
            raise RuntimeError(
                f"{worker.path.name} setup exited {code}; see {output / 'setup.log'}"
            )
        # Probe the built binary without starting a server or attaching a test network.
        with (output / "clickhouse-version.txt").open("w") as log:
            with subprocess.Popen(
                [
                    "docker",
                    "run",
                    "--rm",
                    "--network",
                    "none",
                    "--label",
                    f"ch-backup.integration.environment={worker.environment}",
                    "--entrypoint",
                    "clickhouse",
                    f"clickhouse01:{worker.environment}",
                    "--version",
                ],
                stdout=log,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            ) as process:
                code = self._wait(process)
        if code:
            raise RuntimeError(f"ClickHouse version probe exited {code}")
        self.report["workers"][worker.path.name]["clickhouse_version"] = (
            (output / "clickhouse-version.txt").read_text().strip()
        )
        versions = {
            entry["clickhouse_version"]
            for entry in self.report["workers"].values()
            if "clickhouse_version" in entry
        }
        if len(versions) != 1:
            raise RuntimeError(
                f"Workers built different ClickHouse versions: {versions}"
            )

    def _start(self, worker: Worker, feature: Feature) -> None:
        output = self.results / "features" / Path(feature.path).with_suffix("")
        output.mkdir(parents=True)
        log = (output / "behave.log").open("w")
        command = [
            sys.executable,
            "-m",
            "behave",
            *self.behave_args,
            "--show-timings",
            "--stop",
            "-D",
            "skip_setup",
            "--junit",
            "--junit-directory",
            str(output / "junit"),
            feature.path,
        ]
        try:
            # Lifetime spans scheduler iterations; _complete closes the log and reaps it.
            process = subprocess.Popen(  # pylint: disable=consider-using-with
                command,
                cwd=worker.path,
                env=worker.env(output),
                stdout=log,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )
        except Exception:
            log.close()
            raise
        self.active.append(
            RunningFeature(worker, feature, process, output, log, time.monotonic())
        )
        self.report["features"][feature.path] = {
            "status": "running",
            "worker": worker.path.name,
            "started_at": time.time(),
            "pid": process.pid,
        }
        print(
            f"{worker.path.name}: {feature.path} ({feature.slots(self.queue.jobs)} slots)",
            flush=True,
        )
        self._save()

    def _complete(self, running: RunningFeature) -> None:
        running.log.close()
        outcome = read_outcome(
            running.output, running.process.returncode, running.feature.scenarios
        )
        outcome.update(
            {
                "wall_seconds": time.monotonic() - running.started,
                "finished_at": time.time(),
                "worker": running.worker.path.name,
            }
        )
        self.report["features"][running.feature.path].update(outcome)
        success = outcome["status"] in ("passed", "skipped")
        self.queue.finish(running.feature, success)
        self.active.remove(running)
        print(
            f"{running.worker.path.name}: {running.feature.path}: {outcome['status']} ({outcome['wall_seconds']:.1f}s)",
            flush=True,
        )
        if not success:
            logs = running.worker.path / "staging/logs"
            if logs.exists():
                try:
                    shutil.copytree(logs, running.output / "logs")
                except OSError as error:
                    self.report["errors"].append(
                        f"Copying failure diagnostics: {error}"
                    )
            errors = cleanup_environment(
                running.worker.environment, running.output / "cleanup"
            )
            self.report["errors"].extend(errors)
        self._save()

    def _schedule(self) -> None:
        while self.active or (self.queue.pending and not self.queue.stopped):
            # Reap every completion before handing out any new work.
            for running in list(self.active):
                if running.process.poll() is not None:
                    self._complete(running)
            if self.cancelled:
                break
            busy = {running.worker.environment for running in self.active}
            for worker in self.workers:
                if worker.environment in busy:
                    continue
                feature = self.queue.take()
                if feature is not None:
                    self._start(worker, feature)
            if self.active:
                time.sleep(0.2)

    def _finish_active(self) -> None:
        if self.cancelled:
            for running in self.active:
                self._kill_group(running.process, signal.SIGTERM)
        for running in list(self.active):
            try:
                self._wait(running.process)
                self._complete(running)
            except Exception as error:
                self.report["errors"].append(str(error))

    def _cleanup(self, worker: Worker) -> None:
        try:
            self.report["errors"].extend(
                cleanup_environment(
                    worker.environment, self.results / worker.path.name / "cleanup"
                )
            )
            images = self.report["workers"].get(worker.path.name, {}).get("images")
            if images is None:
                images = image_inventory(worker.path)
            self.report["errors"].extend(remove_image_tags(images))
        except Exception as error:
            self.report["errors"].append(f"Cleanup {worker.path.name}: {error}")


def cli_main() -> None:
    """Expose dry-run selection and a configurable number of local workers."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--jobs", type=int, default=int(os.getenv("INTEGRATION_JOBS", "3"))
    )
    parser.add_argument(
        "--featureset",
        type=Path,
        default=Path(
            os.getenv(
                "INTEGRATION_FEATURESET", "tests/integration/ch_backup.featureset"
            )
        ),
    )
    parser.add_argument(
        "--timings", type=Path, default=os.getenv("INTEGRATION_TIMINGS") or None
    )
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    root = Path.cwd().resolve()
    behave_args = shlex.split(os.getenv("BEHAVE_ARGS", ""))
    try:
        features = load_features(
            root, args.featureset.resolve(), behave_args, args.timings
        )
        run = ParallelRun(root, features, args.jobs, behave_args)
    except (OSError, ValueError) as error:
        parser.error(str(error))
    if args.dry_run:
        for feature in run.queue.pending:
            print(
                f"{feature.slots(args.jobs)} slots, {feature.weight:g} weight, {feature.scenarios} scenarios: {feature.path}"
            )
        return
    raise SystemExit(run.execute())


if __name__ == "__main__":
    cli_main()
