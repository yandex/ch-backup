"""Shared copy fault injection and report assertions."""

import json
import shlex

from behave import then, when

from tests.integration.modules.docker import get_container
from tests.integration.modules.steps import get_step_data


@when(
    'we try to execute ch-backup on {node:w} with publication of "{backup_name}" failing'
)
def step_fail_backup_publication(context, node, backup_name):
    arguments = [
        "python3",
        "-m",
        "tests.integration.modules.fault_injection",
        "fail-backup-publication",
        backup_name,
        *shlex.split(context.text),
    ]
    context.command = shlex.join(arguments)
    result = get_container(context, node).exec_run(
        arguments, user="root", workdir="/code"
    )
    context.response = result.output.decode().strip()
    context.exit_code = result.exit_code


@then('failed copy report "{path}" on {node:w} has results')
def step_failed_copy_report(context, path, node):
    result = get_container(context, node).exec_run(["cat", path], user="root")
    assert result.exit_code == 0, result.output
    report = json.loads(result.output)
    assert report["schema_version"] == 1, report
    assert report["error"], report
    expected = get_step_data(context)
    backups = {backup["name"]: backup for backup in report["backups"]}
    assert len(backups) == len(report["backups"]), "Duplicate backups in report"
    assert set(backups) == set(expected), backups
    for name, properties in expected.items():
        backup = backups[name]
        assert backup["status"] == properties["status"], backup
        if backup["status"] == "copied":
            assert backup["objects_total"] == backup["objects_copied"], backup
            assert backup["objects_failed"] == 0, backup
        if "objects_total" in properties:
            assert backup["objects_total"] == properties["objects_total"], backup
        if properties.get("copied_objects") or properties.get("partial_copy"):
            assert backup["objects_copied"] > 0, backup
        if properties.get("partial_copy"):
            assert backup["objects_failed"] > 0, backup
            assert backup["objects_total"] == (
                backup["objects_copied"] + backup["objects_failed"]
            ), backup
        _assert_copy_errors(backup["errors"], properties)


def _assert_copy_errors(errors, expected):
    if "error_count" in expected:
        assert len(errors) == expected["error_count"], errors
    if "error_stage" in expected:
        assert errors, "Expected errors in copy report"
        assert all(
            error["stage"] == expected["error_stage"] for error in errors
        ), errors
    if expected.get("error_sources"):
        assert errors and all(error.get("source") for error in errors), errors
    if "error_sources_contain" in expected:
        sources = expected["error_sources_contain"]
        assert len(errors) == len(sources), errors
        assert all(
            source in error["source"] for error, source in zip(errors, sources)
        ), errors
