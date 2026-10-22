"""Publish failures before slow diagnostic collection or worker teardown."""

import json
import os
import uuid
from pathlib import Path

from behave.model import Step

from ch_backup import logging
from tests.integration.modules.typing import ContextT


def record_stage_failure(stage: str, error: object = None) -> None:
    """Record only unsuccessful environment stages for final status checks."""
    destination = os.getenv("INTEGRATION_STAGE_FAILURES")
    if not destination:
        return
    failure = {"stage": stage}
    if error is not None:
        failure["error"] = str(error)
    with Path(destination).open("a", encoding="utf-8") as output:
        output.write(json.dumps(failure) + "\n")


def record_step_failure(context: ContextT, step: Step) -> None:
    """Use an atomic file so the coordinator can report a still-running feature."""
    write_failure(
        {
            "scenario": context.scenario.name,
            "step": f"{step.keyword} {step.name}",
            "filename": str(step.filename),
            "line": step.line,
        }
    )


def write_failure(failure: dict) -> None:
    """Publish the first error without letting diagnostic I/O mask it."""
    destination = os.getenv("INTEGRATION_FEATURE_FAILURE")
    if not destination:
        return
    path = Path(destination)
    try:
        if path.exists():
            return
        temporary = path.with_suffix(".tmp")
        temporary.write_text(json.dumps(failure), encoding="utf-8")
        temporary.replace(path)
    except OSError as error:
        logging.warning("Cannot publish integration failure: {}", error)


def annotate(title: str, filename: str | None = None, line: int | None = None) -> None:
    """Show a failure on the page of a GitHub Actions run, at its line when known."""
    if os.getenv("GITHUB_ACTIONS") != "true":
        return
    location = (
        f" file={_command_value(filename, prop=True)},line={int(line)}"
        if filename and line
        else ""
    )
    print(f"::error{location}::{_command_value(title)}", flush=True)


def _command_value(text: str, *, prop: bool = False) -> str:
    """Values of workflow commands must not end the command or start another one."""
    text = text.replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")
    return text.replace(":", "%3A").replace(",", "%2C") if prop else text


def print_untrusted(text: str) -> None:
    """Print output of tests: on GitHub Actions its lines cannot run workflow commands."""
    github = os.getenv("GITHUB_ACTIONS") == "true"
    token = uuid.uuid4().hex
    if github:
        print(f"::stop-commands::{token}", flush=True)
    print(text, flush=True)
    if github:
        print(f"::{token}::", flush=True)


def print_failure(
    title: str, details: str, *, annotate_failure: bool = True, fold: bool = False
) -> None:
    """Keep the complete traceback available and emit a concise Actions annotation."""
    if annotate_failure:
        annotate(title)
    heading = "\n".join("ERROR: " + line for line in title.splitlines())
    fold = fold and os.getenv("GITHUB_ACTIONS") == "true"
    # A folded group keeps a long log out of the way; its title is the heading.
    print(f"::group::{heading}" if fold else heading, flush=True)
    print_untrusted("\n".join("  " + line for line in details.splitlines()))
    if fold:
        print("::endgroup::", flush=True)


def print_process_failure(
    title: str, path: Path, *, annotate_failure: bool = True
) -> None:
    """Show enough process output to diagnose setup, hooks and missing reports."""
    try:
        with path.open("rb") as source:
            source.seek(max(0, path.stat().st_size - 65536))
            tail = source.read().decode("utf-8", errors="replace")
        details = "\n".join(tail.splitlines()[-120:])
    except OSError as error:
        details = f"Cannot read process log: {error}"
    print_failure(
        title,
        f"Log: {path}\nLast output:\n{details}",
        annotate_failure=annotate_failure,
        fold=True,
    )
