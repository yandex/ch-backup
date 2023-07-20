"""
Module for working with Clickhouse configuration file.
"""

from typing import Any, Dict

import xmltodict
from ch_backup.config import Config


class ClickhouseConfig:
    """
    Class for working with Clickhouse configuration file.
    """

    def __init__(self, config: Config) -> None:
        self._ch_config: Dict[str, Any] = {}
        self._ch_config_path = config["clickhouse"]["preprocessed_config_path"]

    def load(self) -> None:
        """
        Loads clickhouse configuration file.
        """
        with open(self._ch_config_path, "r", encoding="utf-8") as file:
            config = xmltodict.parse(file.read())
            self._ch_config = config.get("clickhouse", config.get("yandex"))

    @property
    def config(self) -> dict:
        """
        Returns CH config, loads it if necessary.
        """
        if not self._ch_config:
            self.load()

        return self._ch_config
