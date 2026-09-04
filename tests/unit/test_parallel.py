"""Feature scheduling, report integrity and workspace isolation regressions."""

import importlib
import json
import signal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from tests.integration.diagnostics import (
    print_failure,
    print_process_failure,
    record_step_failure,
)
from tests.integration.feature_queue import (
    Feature,
    FeatureQueue,
    load_features,
    load_timings,
)
from tests.integration.parallel import ParallelRun, RunningFeature, Worker
from tests.integration.parallel_runtime import (
    cleanup_environment,
    image_inventory,
    read_outcome,
    snapshot,
)
from tests.integration.profiling import ENVIRONMENT_LABEL, write_feature_profiles


def feature(name, weight=1, tags=()):
    return Feature(name, 1, frozenset(tags), weight)


def test_step_failure_is_reported_before_process_exit_and_closes_admission(
    tmp_path, monkeypatch, capsys
):
    selected = feature("tests/one.feature")
    run = ParallelRun(tmp_path, [selected, feature("tests/two.feature")], 2, [])
    run.results.mkdir(parents=True)
    output = run.results / "one"
    output.mkdir()
    monkeypatch.setenv("INTEGRATION_FEATURE_FAILURE", str(output / "failure.json"))
    record_step_failure(
        SimpleNamespace(scenario=SimpleNamespace(name="restore after restart")),
        SimpleNamespace(
            keyword="When",
            name="we query ClickHouse",
            filename="tests/one.feature",
            line=12,
            error_message="Traceback:\nConnectionError: connection closed",
            exception=None,
        ),
    )
    process = MagicMock()
    process.poll.return_value = None
    running = RunningFeature(
        Worker(tmp_path, "worker-1"), selected, process, output, MagicMock(), 0
    )
    assert run.queue.take() == selected
    run._report_failure(running)  # pylint: disable=protected-access
    printed = capsys.readouterr().out
    assert "tests/one.feature:12" in printed
    assert "restore after restart" in printed
    assert "ConnectionError: connection closed" in printed
    assert run.queue.take() is None
    assert run.report["features"][selected.path]["failure"]["line"] == 12
    assert process.poll() is None
    run._report_failure(running)  # pylint: disable=protected-access
    assert capsys.readouterr().out == ""


def test_error_output_cannot_inject_actions_commands(monkeypatch, capsys):
    monkeypatch.setenv("GITHUB_ACTIONS", "true")
    print_failure(
        "failed\n::warning::fake", "::error::from subprocess\nValueError: bad"
    )
    output = capsys.readouterr().out
    assert output.startswith("::error::failed%0A::warning::fake\n")
    assert "\n  ::error::from subprocess" in output
    assert "\n::warning::fake" not in output


@pytest.mark.parametrize("exists", [False, True])
def test_process_failure_reports_available_log_tail(tmp_path, capsys, exists):
    path = tmp_path / "setup.log"
    if exists:
        path.write_text("earlier output\n" * 10000 + "Docker build failed: no space\n")
    print_process_failure("worker-2: setup exited 1", path)
    output = capsys.readouterr().out
    assert "worker-2: setup exited 1" in output
    assert (
        "Docker build failed: no space" if exists else "Cannot read process log"
    ) in output
    assert len(output) < 5000


@pytest.mark.parametrize("jobs", [1, 2, 3, 4])
def test_queue_visits_each_feature_once_within_budget(jobs):
    features = [
        feature("heavy1", 9, ["parallel_heavy"]),
        feature("heavy2", 8, ["parallel_heavy"]),
        feature("normal1", 7),
        feature("exclusive", 6, ["parallel_exclusive"]),
        feature("normal2", 5),
    ]
    queue = FeatureQueue(features, jobs)
    completed = []
    while queue.pending or queue.active:
        while queue.take() is not None:
            assert sum(f.slots(jobs) for f in queue.active.values()) <= jobs
            if any("parallel_exclusive" in f.tags for f in queue.active.values()):
                assert len(queue.active) == 1
        assert queue.active, "Scheduler deadlocked with pending features"
        current = next(iter(queue.active.values()))
        completed.append(current.path)
        queue.finish(current, True)
    assert sorted(completed) == sorted(f.path for f in features)


def test_heavy_backfills_with_light_but_not_another_heavy():
    heavy = feature("heavy", 4, ["parallel_heavy"])
    other = feature("other-heavy", 3, ["parallel_heavy"])
    light = feature("light", 2)
    queue = FeatureQueue([heavy, other, light], 3)
    assert queue.take() == heavy
    assert queue.take() == light
    assert queue.take() is None
    queue.finish(heavy, True)
    assert queue.take() == other


def test_exclusive_drains_active_work_before_later_features():
    first = feature("first", 3)
    exclusive = feature("exclusive", 2, ["parallel_exclusive"])
    queue = FeatureQueue([first, exclusive, feature("later")], 3)
    assert queue.take() == first
    assert queue.take() is None
    queue.finish(first, True)
    assert queue.take() == exclusive
    assert queue.take() is None


def test_failure_closes_queue_without_cancelling_active_feature():
    first, second, third = [feature(str(i)) for i in range(3)]
    queue = FeatureQueue([first, second, third], 2)
    assert queue.take() == first
    assert queue.take() == second
    queue.finish(first, False)
    assert queue.take() is None
    assert list(queue.active.values()) == [second]
    queue.finish(second, True)
    assert queue.take() is None
    assert queue.pending == [third]


@pytest.fixture(name="feature_root")
def fixture_feature_root(tmp_path):
    directory = tmp_path / "tests/integration"
    directory.mkdir(parents=True)
    (directory / "one.feature").write_text(
        "@parallel_heavy\nFeature: First\n"
        "  Scenario Outline: Example\n    Given value <value>\n"
        "    Examples:\n      | value |\n      | 1 |\n      | 2 |\n"
    )
    (directory / "two.feature").write_text(
        "Feature: Second\n  Scenario: Plain\n    Given value 3\n"
    )
    featureset = directory / "test.featureset"
    featureset.write_text("# canonical\n\none.feature\ntwo.feature\n")
    return tmp_path, featureset


def test_selection_expands_examples_and_keeps_canonical_order(feature_root):
    root, featureset = feature_root
    features = load_features(root, featureset, [])
    assert [f.scenarios for f in features] == [2, 1]
    assert features[0].slots(3) == 2
    assert len(load_features(root, featureset, ["-i", "two"])) == 1
    assert len(load_features(root, featureset, ["-n", "Plain"])) == 1
    assert len(load_features(root, featureset, ["-t", "parallel_heavy"])) == 1


def test_new_feature_needs_only_canonical_list_entry(feature_root):
    root, featureset = feature_root
    new = featureset.parent / "three.feature"
    new.write_text("Feature: Third\n  Scenario: New\n    Given a step\n")
    assert len(load_features(root, featureset, [])) == 2
    with featureset.open("a") as output:
        output.write("three.feature\n")
    assert len(load_features(root, featureset, [])) == 3


@pytest.mark.parametrize(
    "entry", ["one.feature", "missing.feature", "../../../outside.feature"]
)
def test_invalid_features_rejected_even_when_filtered_out(feature_root, entry):
    root, featureset = feature_root
    with featureset.open("a") as output:
        output.write(entry + "\n")
    with pytest.raises(ValueError):
        load_features(root, featureset, ["-i", "two"])


def test_no_selection_is_an_error(feature_root):
    root, featureset = feature_root
    with pytest.raises(ValueError, match="No features"):
        load_features(root, featureset, ["-n", "does-not-exist"])


def test_timings_ignore_failed_and_skipped_runs_and_scale_unknown_features(
    feature_root,
):
    root, featureset = feature_root
    timings = root / "summary.json"
    timings.write_text(
        json.dumps(
            {
                "features": {
                    "tests/integration/one.feature": {
                        "status": "passed",
                        "wall_seconds": 200,
                    },
                    "tests/integration/two.feature": {
                        "status": "failed",
                        "wall_seconds": 2,
                    },
                }
            }
        )
    )
    assert [f.weight for f in load_features(root, featureset, [], timings)] == [
        200,
        100,
    ]
    assert len(load_timings(timings)) == 1


@pytest.mark.parametrize("duration", [-1, 0, "bad", float("nan"), float("inf")])
def test_invalid_timings_are_rejected(tmp_path, duration):
    timings = tmp_path / "summary.json"
    timings.write_text(
        json.dumps({"features": {"f": {"status": "passed", "wall_seconds": duration}}})
    )
    with pytest.raises(ValueError):
        load_timings(timings)


def test_workspaces_have_independent_mutable_inputs(tmp_path):
    root = tmp_path / "source"
    for name in ("ch_backup", "tests", "images", ".venv", ".git", "staging", "dist"):
        (root / name).mkdir(parents=True)
    for name in (
        "pyproject.toml",
        "uv.lock",
        "README.md",
        ".python-version",
        "Makefile",
    ):
        (root / name).write_text(name)
    (root / "ch_backup/version.txt").write_text("1.0")
    (root / "images/config").write_text("original")
    (root / "dist/ch_backup-1.0-py3-none-any.whl").write_text("current wheel")
    (root / "dist/ch_backup-0.9-py3-none-any.whl").write_text("stale wheel")
    (root / ".session_conf.sav").write_text("old state")
    first, second = tmp_path / "first", tmp_path / "second"
    snapshot(root, first)
    snapshot(root, second)
    (first / "images/config").write_text("changed")
    assert (second / "images/config").read_text() == "original"
    assert (root / "images/config").read_text() == "original"
    for name in (".git", ".venv", "staging", ".session_conf.sav"):
        assert not (first / name).exists()
    assert len(list((first / "dist").glob("*.whl"))) == 1


@pytest.fixture(name="reports")
def fixture_reports(tmp_path):
    (tmp_path / "junit").mkdir()
    (tmp_path / "junit/results.xml").write_text(
        '<testsuite><testcase name="one"/></testsuite>'
    )
    (tmp_path / "outcome.json").write_text(
        json.dumps({"scenarios": [{"status": "passed"}]})
    )
    return tmp_path


def test_reports_require_successful_exit_and_complete_evidence(reports):
    assert read_outcome(reports, 0, 1)["status"] == "passed"
    assert read_outcome(reports, 1, 1)["status"] == "failed"
    assert read_outcome(reports, 0, 2)["status"] == "failed"
    (reports / "junit/results.xml").unlink()
    assert read_outcome(reports, 0, 1)["status"] == "failed"


def test_missing_outcome_is_failure(reports):
    (reports / "outcome.json").unlink()
    assert read_outcome(reports, 0, 1)["status"] == "failed"


@pytest.mark.parametrize(
    "status,expected",
    [
        ("skipped", "skipped"),
        ("untested", "failed"),
        ("failed", "failed"),
        ("hook_error", "failed"),
    ],
)
def test_scenario_outcomes(reports, status, expected):
    (reports / "outcome.json").write_text(
        json.dumps({"scenarios": [{"status": status}]})
    )
    assert read_outcome(reports, 0, 1)["status"] == expected


def test_ignored_cleanup_failure_is_not_success(reports):
    (reports / "stages.jsonl").write_text(
        json.dumps({"stage": "stop", "success": False}) + "\n"
    )
    assert read_outcome(reports, 0, 1)["status"] == "failed"


def test_junit_failure_overrides_successful_process(reports):
    (reports / "junit/results.xml").write_text(
        "<testsuite><testcase><failure/></testcase></testsuite>"
    )
    assert read_outcome(reports, 0, 1)["status"] == "failed"


def test_repository_featureset_is_complete_and_deterministic():
    root = Path(__file__).resolve().parents[2]
    featureset = root / "tests/integration/ch_backup.featureset"
    features = load_features(root, featureset, [])
    assert len({f.path for f in features}) == len(features)
    assert features == load_features(root, featureset, [])
    assert sum(f.scenarios for f in features) > len(features)


def test_cleanup_uses_exact_ownership_and_does_not_remove_misnamed_network(tmp_path):
    client = MagicMock()
    client.containers.list.return_value = []
    network = MagicMock()
    network.name = "other-environment"
    client.networks.list.return_value = [network]
    with patch(
        "tests.integration.parallel_runtime.docker.from_env", return_value=client
    ):
        errors = cleanup_environment("mine", tmp_path)
    client.containers.list.assert_called_once_with(
        all=True, filters={"label": f"{ENVIRONMENT_LABEL}=mine"}
    )
    client.networks.list.assert_called_once_with(
        filters={"label": f"{ENVIRONMENT_LABEL}=mine"}
    )
    network.remove.assert_not_called()
    client.close.assert_called_once()
    assert errors


def test_inventory_closes_docker_client_without_context_manager(tmp_path):
    client = MagicMock(spec=["images", "close"])
    (tmp_path / "staging").mkdir()
    (tmp_path / "staging/docker-compose.yml").write_text(
        "services:\n  service:\n    image: service:mine\n"
    )
    client.images.get.return_value.id = "sha256:123"
    with patch(
        "tests.integration.parallel_runtime.docker.from_env", return_value=client
    ):
        assert image_inventory(tmp_path) == {"service:mine": "sha256:123"}
    client.close.assert_called_once()


def test_feature_profile_excludes_other_workers_and_tracks_descendants(tmp_path):
    report = {
        "workers": {"w1": {"environment": "env1"}},
        "features": {
            "one.feature": {
                "started_at": 10,
                "finished_at": 20,
                "pid": 1,
                "worker": "w1",
            },
            "not-run.feature": {"status": "not_run"},
        },
    }
    sample = {
        "time": 15,
        "memory": {"available": 100},
        "containers": [{"environment": "env1"}, {"environment": "env2"}],
        "processes": [
            {"pid": 3, "ppid": 2},
            {"pid": 2, "ppid": 1},
            {"pid": 1, "ppid": 10},
            {"pid": 4, "ppid": 10},
        ],
    }
    (tmp_path / "resources.jsonl").write_text(json.dumps(sample) + "\n")
    (tmp_path / "features/one").mkdir(parents=True)
    write_feature_profiles(tmp_path, report)
    result = json.loads((tmp_path / "features/one/resources.jsonl").read_text())
    assert result["containers"] == [{"environment": "env1"}]
    assert {p["pid"] for p in result["processes"]} == {1, 2, 3}
    assert result["memory"] == {"available": 100}


@pytest.mark.parametrize("cancel", [False, True])
def test_setup_failure_and_cancellation_leave_failed_report_and_cleanup(
    tmp_path, monkeypatch, cancel
):
    run = ParallelRun(tmp_path, [feature("one.feature"), feature("two.feature")], 2, [])

    def prepare(_worker):
        if cancel:
            run._signal(signal.SIGTERM, None)  # pylint: disable=protected-access
        else:
            raise RuntimeError("build failed")

    monkeypatch.setattr(run, "_prepare", prepare)
    monkeypatch.setattr("tests.integration.parallel.snapshot", lambda *_args: None)
    monkeypatch.setattr("tests.integration.parallel.ResourceSampler", MagicMock())
    cleanup = MagicMock(return_value=[])
    monkeypatch.setattr("tests.integration.parallel.cleanup_environment", cleanup)
    monkeypatch.setattr("tests.integration.parallel.image_inventory", lambda _path: {})
    monkeypatch.setattr(
        "tests.integration.parallel.remove_image_tags", lambda _images: []
    )
    previous = signal.getsignal(signal.SIGTERM)
    assert run.execute() != 0
    report = json.loads((run.results / "summary.json").read_text())
    assert report["status"] == ("cancelled" if cancel else "failed")
    assert all(f["status"] == "not_run" for f in report["features"].values())
    assert cleanup.call_count == 2
    assert signal.getsignal(signal.SIGTERM) == previous


def test_network_creation_uses_docker_ipam_and_rejects_unowned_network(monkeypatch):
    with patch("docker.from_env"):
        module = importlib.import_module("tests.integration.modules.docker")
    client = MagicMock()
    monkeypatch.setattr(module, "DOCKER_API", client)
    context = SimpleNamespace(conf={"network_name": "mine"})
    client.networks.get.side_effect = module.docker.errors.NotFound("not found")
    module.create_network(context)
    kwargs = client.networks.create.call_args.kwargs
    assert kwargs["labels"] == {ENVIRONMENT_LABEL: "mine"}
    assert "ipam" not in kwargs
    assert "com.docker.network.bridge.name" not in kwargs["options"]
    client.networks.get.side_effect = None
    client.networks.get.return_value.attrs = {"Labels": None}
    with pytest.raises(RuntimeError, match="another environment"):
        module.create_network(context)
    assert client.networks.create.call_count == 1
    module.shutdown_network(context)
    client.networks.get.return_value.remove.assert_not_called()


def test_network_cleanup_uses_exact_name(monkeypatch):
    with patch("docker.from_env"):
        module = importlib.import_module("tests.integration.modules.docker")
    client = MagicMock()
    monkeypatch.setattr(module, "DOCKER_API", client)
    client.networks.get.return_value.attrs = {"Labels": {ENVIRONMENT_LABEL: "mine"}}
    module.shutdown_network(SimpleNamespace(conf={"network_name": "mine"}))
    client.networks.get.assert_called_once_with("mine")
    client.networks.get.return_value.remove.assert_called_once()
    client.networks.list.assert_not_called()
