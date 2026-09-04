"""Measure a serial or parallel suite on a clean checkout and a fixed CH version."""

import argparse
import json
import os
import re
import signal
import subprocess
import time
import uuid
from pathlib import Path
from types import FrameType

import psutil

from tests.integration.parallel_runtime import (
    cleanup_environment,
    image_inventory,
    remove_image_tags,
)
from tests.integration.profiling import ResourceSampler


def cli_main() -> None:
    """Each benchmark matrix job measures exactly one mode on its own runner."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=["serial", "1", "2", "3"], required=True)
    args = parser.parse_args()
    version = os.getenv("CLICKHOUSE_VERSION", "")
    if not re.fullmatch(r"\d+(?:\.\d+){3}", version):
        parser.error("Pin CLICKHOUSE_VERSION to four numeric components, not latest")
    if Path(".session_conf.sav").exists():
        parser.error("Benchmark requires a clean checkout without .session_conf.sav")
    environment = f"chb-bench-{uuid.uuid4().hex}"
    output = (Path("staging/benchmark") / environment).resolve()
    output.mkdir(parents=True)
    print(f"Benchmark report: {output / 'benchmark.json'}", flush=True)
    sampler = ResourceSampler(output / "resources.jsonl", [environment])
    env = {
        **os.environ,
        "INTEGRATION_ENV_ID": environment,
        "INTEGRATION_STAGE_PROFILE": str(output / "stages.jsonl"),
        "BEHAVE_ARGS": "--junit --junit-directory staging/junit",
        "INTEGRATION_FEATURESET": "tests/integration/ch_backup.featureset",
        "INTEGRATION_TIMINGS": "",
    }
    target = (
        "test-integration" if args.mode == "serial" else "test-integration-parallel"
    )
    if args.mode != "serial":
        env["INTEGRATION_JOBS"] = args.mode
    report = {
        "mode": args.mode,
        "clickhouse_version": version,
        "cpu_count": psutil.cpu_count(),
        "memory_bytes": psutil.virtual_memory().total,
    }
    started = time.monotonic()
    sampler.start()
    try:
        with subprocess.Popen(
            ["make", target], env=env, start_new_session=True
        ) as process:

            def forward(signum: int, _frame: FrameType | None) -> None:
                try:
                    os.killpg(process.pid, signum)
                except ProcessLookupError:
                    pass

            previous = {
                sig: signal.signal(sig, forward)
                for sig in (signal.SIGTERM, signal.SIGINT)
            }
            try:
                report["returncode"] = process.wait()
            finally:
                for sig, handler in previous.items():
                    signal.signal(sig, handler)
        # This includes make's wheel preparation and every worker image build.
        report["wall_seconds"] = time.monotonic() - started
        if args.mode == "serial":
            report["images"] = image_inventory(Path.cwd())
    finally:
        sampler.stop()
        try:
            errors = cleanup_environment(environment, output / "cleanup")
            if args.mode == "serial":
                errors.extend(remove_image_tags(image_inventory(Path.cwd())))
        except Exception as error:
            errors = [str(error)]
        report["cleanup_errors"] = errors
        report["status"] = (
            "passed" if report.get("returncode", 1) == 0 and not errors else "failed"
        )
        (output / "benchmark.json").write_text(json.dumps(report, indent=2) + "\n")
    raise SystemExit(int(report.get("returncode", 1) != 0 or bool(errors)))


if __name__ == "__main__":
    cli_main()
