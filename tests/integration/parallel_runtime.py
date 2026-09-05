"""Workspace snapshots, owned-resource cleanup and parallel test reports."""

import json
import shutil
import xml.etree.ElementTree as ET
from contextlib import closing
from pathlib import Path

import docker
import yaml

from tests.integration.profiling import ENVIRONMENT_LABEL


def snapshot(root: Path, destination: Path) -> None:
    """Copy inputs, including uncommitted source changes, without writable links."""
    destination.mkdir(parents=True)
    ignored = shutil.ignore_patterns("__pycache__", "*.pyc", ".pytest_cache")
    for directory in ("ch_backup", "tests", "images"):
        shutil.copytree(root / directory, destination / directory, ignore=ignored)
    for name in (
        "pyproject.toml",
        "uv.lock",
        "README.md",
        ".python-version",
        "Makefile",
    ):
        shutil.copy2(root / name, destination / name)
    version = (root / "ch_backup/version.txt").read_text().strip()
    wheels = list((root / "dist").glob(f"ch_backup-{version}-*.whl"))
    if len(wheels) != 1:
        raise ValueError(f"Expected one built wheel for {version}, found {len(wheels)}")
    (destination / "dist").mkdir()
    shutil.copy2(wheels[0], destination / "dist" / wheels[0].name)


def image_inventory(workspace: Path) -> dict[str, str]:
    """Record exact image IDs; also used to remove only this run's image tags."""
    config_file = workspace / "staging/docker-compose.yml"
    if not config_file.exists():
        return {}
    services = yaml.safe_load(config_file.read_text())["services"]
    result = {}
    with closing(docker.from_env(timeout=10)) as client:
        for service in services.values():
            name = service["image"]
            try:
                result[name] = client.images.get(name).id
            except docker.errors.ImageNotFound:
                continue
    return result


def cleanup_environment(environment: str, destination: Path) -> list[str]:
    """Remove only resources with the exact environment ownership label."""
    errors = []
    destination.mkdir(parents=True, exist_ok=True)
    with closing(docker.from_env(timeout=30)) as client:
        containers = client.containers.list(
            all=True, filters={"label": f"{ENVIRONMENT_LABEL}={environment}"}
        )
        for container in containers:
            try:
                (destination / f"{container.name}.json").write_text(
                    json.dumps(
                        {
                            "id": container.id,
                            "image": container.attrs["Image"],
                            "state": container.attrs.get("State"),
                        },
                        indent=2,
                    )
                )
                (destination / f"{container.name}.log").write_bytes(
                    container.logs(tail=2000, timestamps=True)
                )
            except Exception as error:
                errors.append(f"Diagnostics for {container.name}: {error}")
            try:
                container.remove(force=True, v=True)
            except docker.errors.NotFound:
                continue
            except Exception as error:
                errors.append(f"Removing {container.name}: {error}")
        networks = client.networks.list(
            filters={"label": f"{ENVIRONMENT_LABEL}={environment}"}
        )
        for network in networks:
            if network.name != environment:
                errors.append(f"Unexpected owned network name: {network.name}")
                continue
            try:
                network.remove()
            except docker.errors.NotFound:
                continue
            except Exception as error:
                errors.append(f"Removing network {network.name}: {error}")
    return errors


def remove_image_tags(images: dict[str, str]) -> list[str]:
    """Remove run-specific tags without pruning the shared build cache."""
    errors = []
    with closing(docker.from_env(timeout=30)) as client:
        for name, image_id in images.items():
            try:
                if client.images.get(name).id != image_id:
                    errors.append(f"Image tag changed during run: {name}")
                    continue
                client.images.remove(name, noprune=True)
            except docker.errors.ImageNotFound:
                continue
            except Exception as error:
                errors.append(f"Removing image tag {name}: {error}")
    return errors


def read_outcome(destination: Path, returncode: int, expected: int) -> dict:
    """Missing, incomplete or contradictory reports cannot turn a run green."""
    outcome: dict = {"status": "failed", "returncode": returncode}
    try:
        details = json.loads((destination / "outcome.json").read_text())
        outcome.update(details)
        outcome["status"] = "failed"
        outcome["returncode"] = returncode
        reports = list((destination / "junit").glob("*.xml"))
        if not reports:
            raise ValueError("Missing JUnit report")
        cases = [
            case for report in reports for case in ET.parse(report).iter("testcase")
        ]
        if len(cases) < expected:
            raise ValueError(
                f"JUnit report has {len(cases)} scenarios, expected at least {expected}"
            )
        statuses = [scenario["status"] for scenario in details["scenarios"]]
        if len(statuses) != expected:
            raise ValueError(
                f"Expected {expected} selected scenarios, found {len(statuses)}"
            )
        complete = all(status in ("passed", "skipped") for status in statuses)
        junit_failed = any(
            case.find("failure") is not None or case.find("error") is not None
            for case in cases
        )
        stages = destination / "stages.jsonl"
        stage_failed = stages.exists() and any(
            not json.loads(line)["success"] for line in stages.read_text().splitlines()
        )
        if returncode == 0 and complete and not junit_failed and not stage_failed:
            outcome["status"] = (
                "skipped" if all(s == "skipped" for s in statuses) else "passed"
            )
    except (OSError, ValueError, KeyError, TypeError, ET.ParseError) as error:
        outcome["error"] = str(error)
    return outcome
