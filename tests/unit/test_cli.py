"""
Cli unit tests.
"""

from ch_backup import cli
from tests.unit.utils import parametrize


@parametrize(
    {
        "id": "empty config values",
        "args": {
            "values": [],
            "expected": {},
        },
    },
    {
        "id": "single config values",
        "args": {
            "values": [
                ("backup", {"path_root": "/root"}),
            ],
            "expected": {
                "backup": {
                    "path_root": "/root",
                }
            },
        },
    },
    {
        "id": "plain config values",
        "args": {
            "values": [
                ("backup.path_root", "/root"),
                ("backup.deduplication_age_limit.days", 123),
                ("backup.keep_freezed_data_on_failure", True),
            ],
            "expected": {
                "backup": {
                    "path_root": "/root",
                    "deduplication_age_limit": {
                        "days": 123,
                    },
                    "keep_freezed_data_on_failure": True,
                }
            },
        },
    },
    {
        "id": "complex config values",
        "args": {
            "values": [
                (
                    "backup",
                    {
                        "path_root": None,
                        "deduplication_age_limit": {
                            "days": 7,
                        },
                    },
                ),
                ("backup.path_root", "/root"),
                ("backup.deduplication_age_limit", {"days": 8}),
                ("backup.keep_freezed_data_on_failure", True),
            ],
            "expected": {
                "backup": {
                    "path_root": "/root",
                    "deduplication_age_limit": {
                        "days": 8,
                    },
                    "keep_freezed_data_on_failure": True,
                }
            },
        },
    },
    {
        "id": "overrode config values",
        "args": {
            "values": [
                ("backup", "not_used_setting"),
                ("backup", "actual_setting"),
            ],
            "expected": {"backup": "actual_setting"},
        },
    },
    {
        "id": "not uniform config values",
        "args": {
            "values": [
                (
                    "backup",
                    {
                        "path_root": "/root",
                        "deduplication_age_limit": {
                            "days": 7,
                        },
                    },
                ),
                ("backup", "actual_setting"),
            ],
            "expected": {
                "backup": "actual_setting",
            },
        },
    },
)
def test_build_cli_cfg_from_config_parameters(values, expected):
    # pylint: disable=protected-access
    assert cli._build_cli_cfg_from_config_parameters(values) == expected
