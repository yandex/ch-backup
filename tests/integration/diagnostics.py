"""Publish failures before slow diagnostic collection or worker teardown."""

import json
import os
import traceback
from pathlib import Path

from behave.model import Step

from tests.integration.modules.typing import ContextT


def record_step_failure(context: ContextT, step: Step) -> None:
    """Use an atomic file so the coordinator can report a still-running feature."""
    destination = os.getenv("INTEGRATION_FEATURE_FAILURE")
    if not destination:
        return
    path = Path(destination)
    temporary = path.with_suffix(".tmp")
    error = step.error_message or str(step.exception)
    if step.exception:
        error += "\n" + "".join(
            traceback.format_exception(
                type(step.exception), step.exception, step.exception.__traceback__
            )
        )
    temporary.write_text(
        json.dumps(
            {
                "scenario": context.scenario.name,
                "step": f"{step.keyword} {step.name}",
                "filename": str(step.filename),
                "line": step.line,
                "error": error,
            }
        ),
        encoding="utf-8",
    )
    temporary.replace(path)


def print_failure(title: str, details: str) -> None:
    """Keep the complete traceback visible and emit a concise Actions annotation."""
    if os.getenv("GITHUB_ACTIONS") == "true":
        message = title.replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")
        print(f"::error::{message}", flush=True)
    # Prefix untrusted process output so it cannot emit Actions workflow commands.
    heading = "\n".join("ERROR: " + line for line in title.splitlines())
    print(
        heading + "\n" + "\n".join("  " + line for line in details.splitlines()),
        flush=True,
    )


def print_process_failure(title: str, path: Path) -> None:
    """Show enough process output to diagnose setup, hooks and missing reports."""
    try:
        with path.open("rb") as source:
            source.seek(max(0, path.stat().st_size - 65536))
            tail = source.read().decode("utf-8", errors="replace")
        details = "\n".join(tail.splitlines()[-120:])
    except OSError as error:
        details = f"Cannot read process log: {error}"
    print_failure(title, f"Log: {path}\nLast output:\n{details}")
