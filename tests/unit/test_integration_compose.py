import subprocess
from types import SimpleNamespace
from unittest import mock

import pytest

from tests.integration.modules import compose


@pytest.mark.parametrize(
    "logs_error",
    [
        None,
        subprocess.CalledProcessError(1, ["docker", "compose", "logs"]),
        OSError("missing executable"),
    ],
)
def test_startup_logs_preserve_original_error(logs_error, tmp_path, monkeypatch):
    failure_file = tmp_path / "stage-failures.jsonl"
    monkeypatch.setenv("INTEGRATION_STAGE_FAILURES", str(failure_file))
    context = SimpleNamespace(conf={"network_name": "startup-test"})
    startup_error = subprocess.CalledProcessError(1, ["docker", "compose", "up"])

    with mock.patch.object(
        compose, "_call_compose", side_effect=[startup_error, logs_error]
    ) as call_compose:
        with pytest.raises(subprocess.CalledProcessError) as raised:
            compose.startup_containers(context)

    assert raised.value is startup_error
    assert call_compose.call_args_list == [
        mock.call(
            context.conf, project_name="startup-test", command="up -d --timeout 30"
        ),
        mock.call(
            context.conf,
            project_name="startup-test",
            command="logs --no-color --timestamps",
        ),
    ]
    assert "docker', 'compose', 'up" in failure_file.read_text()
