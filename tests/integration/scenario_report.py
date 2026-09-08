"""Durable scenario snapshots for parallel Behave workers."""

import json
import os
import traceback
from functools import partial
from pathlib import Path
from typing import Any

from behave.formatter.base import Formatter, StreamOpener
from behave.runner import Runner

from ch_backup import logging
from tests.integration.diagnostics import write_failure


def exception_text(statement: Any) -> str:
    """Prefer one complete traceback over an assertion's abbreviated message."""
    error = getattr(statement, "exception", None)
    if error is not None:
        return "".join(
            traceback.format_exception(type(error), error, error.__traceback__)
        ).rstrip()
    return getattr(statement, "error_message", None) or ""


def step_details(step: Any, background: bool, attempted: bool) -> dict:
    return {
        "name": f"{step.keyword} {step.name}",
        "filename": str(step.filename),
        "line": step.line,
        "status": step.status.name,
        "duration": step.duration,
        "background": background,
        "attempted": attempted,
        "text": step.text,
        "table": (
            [step.table.headings, *[list(row.cells) for row in step.table.rows]]
            if step.table is not None
            else None
        ),
        "error": exception_text(step),
    }


class ScenarioFormatter(Formatter):
    """Observe model events independently of the user's output formatters."""

    name = "parallel-scenario"
    description = "Complete failed scenarios for the parallel coordinator"

    def __init__(self, runner):
        super().__init__(StreamOpener(), runner.config)
        self.runner = runner
        self.current = None
        self.hook_errors: list[dict] = []
        self.active_step = None
        self.attempted: set[int] = set()
        self.path = Path(os.environ["INTEGRATION_FEATURE_FAILURE"]).with_name(
            "scenario.json"
        )

    def scenario(self, scenario):
        self.finish()
        self.current = scenario
        self.hook_errors = []
        self.active_step = None
        self.attempted.clear()
        self.save()

    def result(self, step):
        self.attempted.add(id(step))
        if step.status.has_failed():
            self.notify(step)
        self.active_step = None
        self.save()

    def notify(self, step: Any = None, hook: str = "", error: str = "") -> None:
        if self.current is None:
            return
        if not hook and step is None:
            return
        write_failure(
            {
                "scenario": self.current.name,
                "step": (
                    f"{step.keyword} {step.name}"
                    if step is not None
                    else f"Hook {hook}"
                ),
                "filename": str(
                    step.filename if step is not None else self.current.filename
                ),
                "line": step.line if step is not None else self.current.line,
                "error": error or exception_text(self.current if hook else step),
            }
        )

    def save(self, completed: bool = False) -> None:
        """Atomically retain useful context even if the process exits abruptly."""
        if self.current is None:
            return
        try:
            scenario = self.current
            # Behave stores the expanded Example row on the generated Scenario.
            example = getattr(scenario, "_row", None)
            background_steps = list(scenario.background_steps)
            captured = scenario.captured.make_report()
            if not completed:
                live = self.runner.capture_controller.make_captured()
                if live.has_output():
                    captured += "\n" + live.make_report()
            report = {
                "feature": scenario.feature.name,
                "scenario": scenario.name,
                "filename": str(scenario.filename),
                "line": scenario.line,
                "tags": list(scenario.effective_tags),
                "example": dict(example.as_dict()) if example is not None else {},
                "completed": completed,
                "status": scenario.status.name,
                "steps": [
                    step_details(step, True, id(step) in self.attempted)
                    for step in background_steps
                ]
                + [
                    step_details(step, False, id(step) in self.attempted)
                    for step in scenario.steps
                ],
                "active_step": (
                    f"{self.active_step.keyword} {self.active_step.name}"
                    if self.active_step is not None
                    else None
                ),
                "hook_errors": self.hook_errors,
                "error": exception_text(scenario),
                "captured": captured,
            }
            temporary = self.path.with_suffix(".tmp")
            temporary.write_text(json.dumps(report), encoding="utf-8")
            temporary.replace(self.path)
        except Exception as error:  # Diagnostics must not replace a test failure.
            logging.warning("Cannot record scenario diagnostics: {}", error)

    def finish(self) -> None:
        self.save(completed=True)

    def eof(self):
        self.finish()

    def close(self):
        # eof() finalizes normal runs; preserve the last snapshot after an abort.
        pass


class ReportingRunner(Runner):
    """Attach diagnostics without changing formatter/output argument pairing."""

    scenario_formatter = None

    def load_hooks(self, filename=None):
        super().load_hooks(filename)
        hooks = self.hooks
        self.hooks: dict = {
            name: partial(self._observed_hook, name, hook)
            for name, hook in hooks.items()
        }

    def _observed_hook(self, name, hook, *args):
        try:
            return hook(*args)
        except (Exception, KeyboardInterrupt, SystemExit):
            reporter = self.scenario_formatter
            if (
                reporter is not None
                and reporter.current is not None
                and name
                in (
                    "before_scenario",
                    "after_scenario",
                    "before_step",
                    "after_step",
                    "before_tag",
                    "after_tag",
                )
            ):
                error = traceback.format_exc().rstrip()
                reporter.hook_errors.append({"hook": name, "error": error})
                reporter.notify(hook=name, error=error)
                reporter.save()
            raise

    def run_model(self, features=None):
        self.scenario_formatter = ScenarioFormatter(self)
        self.formatters.append(self.scenario_formatter)
        return super().run_model(features)

    def run_hook(self, hook_name, *args):
        reporter = self.scenario_formatter
        if reporter is not None and reporter.current is not None:
            if hook_name == "before_step":
                reporter.active_step = args[0]
                reporter.attempted.add(id(args[0]))
                reporter.save()
            elif hook_name == "after_step" and args[0].status.has_failed():
                # Before the repository's after_step starts slow log collection.
                reporter.notify(args[0])
                reporter.save()
        return super().run_hook(hook_name, *args)


def render_scenario(report: dict) -> str:
    """Render every step and its data, including steps not reached after failure."""
    lines = [
        f"Feature: {report['feature']}",
        f"Scenario: {report['scenario']}  # {report['filename']}:{report['line']}",
    ]
    if report["tags"]:
        lines.append("Tags: " + " ".join("@" + tag for tag in report["tags"]))
    if report["example"]:
        lines.append("Example: " + json.dumps(report["example"], ensure_ascii=False))
    lines.append("Scenario status: " + report["status"])
    if not report["completed"]:
        lines.append("INCOMPLETE: worker exited before the scenario report finished")
        if report["active_step"]:
            lines.append("Last active step: " + report["active_step"])
    for step in report["steps"]:
        marker = step_marker(step, report)
        background = " [Background]" if step["background"] else ""
        lines.append(
            f"{marker}{background} {step['name']} ({step['duration']:.3f}s)"
            f"  # {step['filename']}:{step['line']}"
        )
        if step["text"] is not None:
            lines.extend(
                ['  """', *["  " + line for line in step["text"].splitlines()], '  """']
            )
        if step["table"] is not None:
            lines.extend("  | " + " | ".join(row) + " |" for row in step["table"])
        if step["error"] and not any(
            step["error"] in error["error"] for error in report["hook_errors"]
        ):
            lines.append(step["error"])
    for error in report["hook_errors"]:
        lines.extend([f">>> FAILED Hook {error['hook']}", error["error"]])
    if report["error"] and not report["hook_errors"]:
        lines.append(report["error"])
    if report["captured"]:
        lines.extend(["Scenario output:", report["captured"]])
    return "\n".join(lines) + "\n"


def step_marker(step: dict, report: dict) -> str:
    status = step["status"]
    if not step["attempted"]:
        return "NOT RUN"
    if not report["completed"] and step["name"] == report["active_step"]:
        return "INTERRUPTED"
    return {
        "untested": "NOT RUN",
        "skipped": "SKIPPED",
        "passed": "PASSED",
        "hook_error": "HOOK ERROR",
    }.get(status, ">>> FAILED (" + status + ")")
