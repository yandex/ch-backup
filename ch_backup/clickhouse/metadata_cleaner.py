"""
Zookeeper metadata cleaner for clickhouse.
"""

import copy
import os
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Dict, List, Optional

from kazoo.exceptions import NoNodeError

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
from ch_backup.zookeeper.zookeeper import ZookeeperCTL


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
    Class for cleaning up table metadata from zookeeper.
    """

    def __init__(
        self,
        ch_ctl: ClickhouseCTL,
        zk_ctl: ZookeeperCTL,
        replica_to_drop: Optional[str],
        max_workers: int,
    ) -> None:
        self._ch_ctl = ch_ctl
        self._zk_ctl = zk_ctl
        self._macros = self._ch_ctl.get_macros()
        self._replica_to_drop = replica_to_drop
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
            full_table_name = f"{table.database}.{table.name}"
            replicas_to_drop = (
                [self._replica_to_drop]
                if self._replica_to_drop
                else self._list_replicas(path_resolved)
            )

            with self._zk_ctl.zk_client as zk_client:
                for replica in replicas_to_drop:
                    # Both paths are already abs.
                    full_table_zk_path = (
                        self._zk_ctl.zk_root_path  # type: ignore
                        + path_resolved
                        + "/replicas/"
                        + replica
                    )
                    if not zk_client.exists(full_table_zk_path):
                        logging.debug(
                            "There are no nodes for the replicated table {} with zk path {}",
                            full_table_name,
                            full_table_zk_path,
                        )
                        return

                    # We are sure that we want to  drop the table from zk.
                    # To force it we will remove it active flag.
                    active_flag_path = os.path.join(full_table_zk_path, "is_active")
                    try:
                        zk_client.delete(active_flag_path)
                    except NoNodeError:
                        pass

                    logging.debug(
                        "Scheduling metadata cleanup for table {}, replica to clean: {}",
                        full_table_name,
                        replica,
                    )
                    tasks[full_table_name] = self._exec_pool.submit(
                        self._ch_ctl.system_drop_replica,
                        replica,
                        path_resolved,
                    )

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

    def _list_replicas(self, zk_path: str) -> List[str]:
        replicas_path = (
            self._zk_ctl.zk_root_path + zk_path + "/replicas/"  # type: ignore
        )
        with self._zk_ctl.zk_client as zk_client:
            try:
                return zk_client.get_children(replicas_path)
            except NoNodeError:
                return []

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

            # replicas from _list_replicas look like 'shard|replica'
            # _replica_to_drop looks like 'replica'
            replicas_to_drop = (
                [f"{replace_macros(shard, db_macros)}|{self._replica_to_drop}"]
                if self._replica_to_drop
                else self._list_replicas(path_resolved)
            )

            for replica in replicas_to_drop:
                with self._zk_ctl.zk_client as zk_client:
                    # Both paths are already abs.
                    full_database_zk_path = (
                        self._zk_ctl.zk_root_path
                        + path_resolved
                        + "/replicas/"
                        + replica
                    )

                    if not zk_client.exists(full_database_zk_path):
                        logging.debug(
                            "There are no nodes for the replicated database {} with zk path {}",
                            database.name,
                            full_database_zk_path,
                        )
                        continue

                    active_flag_path = os.path.join(full_database_zk_path, "active")
                    try:
                        zk_client.delete(active_flag_path)
                    except NoNodeError:
                        pass

                logging.debug(
                    "Scheduling replica {} from database {} metadata from zookeeper {}.",
                    replica,
                    database.name,
                    path_resolved,
                )
                future = self._exec_pool.submit(
                    self._ch_ctl.system_drop_database_replica, replica, path_resolved  # type: ignore
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
