"""
Zookeeper metadata cleaner for clickhouse.
"""

import copy
import os
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Dict, List, Optional

from ch_backup import logging
from ch_backup.clickhouse.client import ClickhouseError
from ch_backup.clickhouse.control import ClickhouseCTL
from ch_backup.clickhouse.models import Database, Table
from ch_backup.exceptions import ConfigurationError
from ch_backup.util import (
    get_database_zookeeper_paths,
    get_table_zookeeper_paths,
    replace_macros,
)


def select_replica_drop(replica_name: Optional[str], macros: Dict) -> str:
    """
    Select replica to drop from zookeeper.
    """
    selected_replica = replica_name
    if not selected_replica:
        selected_replica = macros.get("replica", None)

    if not selected_replica:
        raise ConfigurationError(
            "Can't get the replica name. Please, specify it through macros or replica_name knob."
        )
    return selected_replica


class MetadataCleaner:
    """
    Class for cleaning up replica metadata from zookeeper.
    """

    def __init__(
        self, ch_ctl: ClickhouseCTL, replica_to_drop: str, max_workers: int
    ) -> None:
        self._ch_ctl = ch_ctl
        self._macros = self._ch_ctl.get_macros()
        self._replica_to_drop = replica_to_drop or self._macros.get("replica")
        if not self._replica_to_drop:
            raise ConfigurationError(
                "Can't get the replica name. Please, specify it through macros or replica_name knob."
            )
        self._exec_pool = ThreadPoolExecutor(max_workers)

    def clean_tables_metadata(self, replicated_tables: List[Table]) -> None:
        """
        Remove replica tables metadata from zookeeper.
        """
        replicated_table_paths = get_table_zookeeper_paths(replicated_tables)
        tasks: Dict[str, Future] = {}
        for table, table_path in replicated_table_paths:
            table_macros = copy.copy(self._macros)
            macros_to_override = dict(
                database=table.database, table=table.name, uuid=table.uuid
            )
            table_macros.update(macros_to_override)

            path_resolved = os.path.abspath(replace_macros(table_path, table_macros))

            logging.debug(
                "Scheduling drop replica {} from table {} metadata from zookeeper {}.",
                self._replica_to_drop,
                f"{table.database}.{table.database}",
                path_resolved,
            )
            future = self._exec_pool.submit(
                self._ch_ctl.system_drop_replica, self._replica_to_drop, path_resolved  # type: ignore
            )
            tasks[f"{table.database}.{table.name}"] = future

        for full_table_name, future in tasks.items():
            try:
                future.result()
                logging.debug(
                    "Successful zk metadata cleanup for table {}", full_table_name
                )
            except ClickhouseError as ch_error:
                if "does not look like a table path" in str(ch_error):
                    logging.warning(
                        "System drop replica failed with: {}\n Will ignore it, probably different configuration for zookeeper or tables schema.",
                        repr(ch_error),
                    )
                else:
                    raise

    def clean_database_metadata(self, replicated_databases: List[Database]) -> None:
        """
        Remove replica database metadata from zookeeper.
        """
        if not self._ch_ctl.ch_version_ge("23.3"):
            logging.warning(
                "Ch version is too old, will skip replicated database cleanup."
            )
            return

        replicated_databases_paths = get_database_zookeeper_paths(replicated_databases)
        tasks: Dict[str, Future] = {}

        for database, database_path, shard in replicated_databases_paths:
            db_macros = copy.copy(self._macros)

            macros_to_override = dict(database=database.name, uuid=database.uuid)
            db_macros.update(macros_to_override)

            path_resolved = os.path.abspath(replace_macros(database_path, db_macros))
            full_replica_name = (
                f"{replace_macros(shard, db_macros)}|{self._replica_to_drop}"
            )

            logging.debug(
                "Scheduling replica {} from database {} metadata from zookeeper {}.",
                full_replica_name,
                database.name,
                path_resolved,
            )
            future = self._exec_pool.submit(
                self._ch_ctl.system_drop_database_replica, full_replica_name, path_resolved  # type: ignore
            )
            tasks[f"{database.name}"] = future

        for database_name, future in tasks.items():
            try:
                future.result()
                logging.debug(
                    "Successful zk metadata cleanup for database {}", database_name
                )
            except ClickhouseError as ch_error:
                if "does not look like a path of Replicated database" in str(
                    ch_error
                ) or "node doesn't exist" in str(ch_error):
                    logging.warning(
                        "System drop database replica failed with: {}\n Will ignore it, probably different configuration for zookeeper or database schema.",
                        repr(ch_error),
                    )
                else:
                    raise
