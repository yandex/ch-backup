"""
ClickHouse client.
"""

import logging
from copy import copy
from datetime import datetime
from typing import Any, Sequence, Tuple, Union
from urllib.parse import urljoin

from pkg_resources import parse_version
from requests import HTTPError, Session

from . import docker
from .typing import ContextT
from .utils import generate_random_string, normalize_create_query

DB_COUNT = 2
TABLE_COUNT = 2
ROWS_COUNT = 3

ACCESS_TYPES = [
    ("users", "USER"),
    ("roles", "ROLE"),
    ("quotas", "QUOTA"),
    ("row_policies", "ROW POLICY"),
    ("settings_profiles", "SETTINGS PROFILE"),
]
ACCESS_LIST_QUERY = """
SELECT name
FROM system.{type} WHERE storage IN ('disk', 'local directory', 'local_directory', 'replicated')
FORMAT JSON
"""


class ClickhouseClient:
    """
    ClickHouse Client.
    """

    def __init__(
        self, context: ContextT, node_name: str, user: str = None, password: str = None
    ) -> None:
        protocol = "http"
        port = context.conf["services"]["clickhouse"]["expose"][protocol]
        host, exposed_port = docker.get_exposed_port(
            docker.get_container(context, node_name), port
        )

        self._session = Session()
        self._url = f"{protocol}://{host}:{exposed_port}"
        self._timeout = 30
        self._user = user
        self._password = password
        self._settings = context.clickhouse_settings

    def ping(self) -> None:
        """
        Ping ClickHouse server.
        """
        self._query("GET", url="ping")

    def execute(self, query: str) -> None:
        """
        Execute arbitrary query.
        """
        self._query("POST", query=query)

    def get_response(self, query: str) -> str:
        """
        Execute arbitrary query and return result
        """
        return str(self._query("POST", query=query))

    def get_version(self) -> str:
        """
        Get ClickHouse version.
        """
        return self._query("GET", "SELECT version()")

    def ch_version_ge(self, comparing_version: str) -> bool:
        """
        Returns True if ClickHouse version >= comparing_version.
        """
        return parse_version(self.get_version()) >= parse_version(comparing_version)  # type: ignore

    def init_schema(self) -> None:
        """
        Create test schema.
        """
        for db_num in range(1, DB_COUNT + 1):
            db_name = f"test_db_{db_num:02d}"
            self._query("POST", f"CREATE DATABASE IF NOT EXISTS {db_name}")
            for table_num in range(1, TABLE_COUNT + 1):
                table_name = f"test_table_{table_num:02d}"
                query = f"""
                    CREATE TABLE IF NOT EXISTS `{db_name}`.`{table_name}` (
                        date Date,
                        datetime DateTime,
                        int_num UInt32,
                        str String
                    )
                    ENGINE MergeTree
                    PARTITION BY date
                    ORDER BY int_num
                    """
                self._query("POST", query)

    def init_data(self, mark: str) -> None:
        """
        Fill test schema with data
        """
        for db_num in range(1, DB_COUNT + 1):
            db_name = self._get_test_db_name(db_num)
            for table_num in range(1, TABLE_COUNT + 1):
                rows = []
                table_name = self._get_test_table_name(table_num)
                for row_num in range(1, ROWS_COUNT + 1):
                    rows.append(
                        ", ".join(self._gen_record(row_num=row_num, str_prefix=mark))
                    )

                self._query(
                    "POST",
                    f"INSERT INTO {db_name}.{table_name} FORMAT CSV",
                    data="\n".join(rows),
                )

    def get_all_user_data(self) -> Tuple[int, dict]:
        """
        Retrieve all user data.
        """
        user_data = {}
        rows_count = 0
        for db_name, table_name, columns in self._get_tables_for_data_comparisson():
            query = f"""
                SELECT *
                FROM `{db_name}`.`{table_name}`
                ORDER BY {','.join(map(lambda column: f"`{column}`", columns))}
                FORMAT JSONCompact
                """
            table_data = self._query("POST", data=query.encode("utf-8"))
            user_data[".".join([db_name, table_name])] = table_data["data"]
            rows_count += table_data["rows"]
        return rows_count, user_data

    def get_table_schemas(self) -> dict:
        """
        Retrieve DDL for user schemas.
        """
        query = """
            SELECT
                database,
                name,
                create_table_query
            FROM system.tables
            WHERE database NOT IN ('system', '_temporary_and_external_tables',
                                   'information_schema', 'INFORMATION_SCHEMA')
            FORMAT JSON
            """
        tables = self._query("GET", query)["data"]
        return {
            (table["database"], table["name"]): normalize_create_query(
                table["create_table_query"]
            )
            for table in tables
        }

    def get_all_user_databases(self) -> Sequence[str]:
        """
        Get user databases.
        """
        query = """
            SELECT name
            FROM system.databases
            WHERE name NOT IN ('system', '_temporary_and_external_tables',
                               'information_schema', 'INFORMATION_SCHEMA')
            FORMAT JSONCompact
            """

        databases = self._query("GET", query)["data"]
        return [db[0] for db in databases]

    def get_all_access_objects(self) -> set:
        """
        Retrieve DDL for users, roles, etc
        """
        result = set()
        for table_name, uppercase_name in ACCESS_TYPES:
            ch_resp = self._query(
                "GET", query=ACCESS_LIST_QUERY.format(type=table_name)
            )
            for row in ch_resp.get("data", []):
                result.add(
                    self._query(
                        "GET", query=f"SHOW CREATE {uppercase_name} {row['name']}"
                    )
                )

        return result

    def drop_all_access_objects(self) -> None:
        """
        Drop all access entities.
        """
        for table_name, uppercase_name in ACCESS_TYPES:
            ch_resp = self._query(
                "GET", query=ACCESS_LIST_QUERY.format(type=table_name)
            )
            for row in ch_resp.get("data", []):
                self._query("POST", f'DROP {uppercase_name} `{row["name"]}`')

    def drop_all_udf(self) -> None:
        """
        Drop all UDF.
        """
        if not self.ch_version_ge("21.11"):
            return
        all_functions_query = "SELECT name FROM system.functions WHERE origin == 'SQLUserDefined' FORMAT JSON"
        ch_resp = self._query("GET", query=all_functions_query)
        for row in ch_resp.get("data", []):
            self._query("POST", f'DROP FUNCTION `{row["name"]}`')

    def drop_database(self, db_name: str) -> None:
        """
        Drop database.
        """
        self._query("POST", f"DROP DATABASE `{db_name}`")

    def drop_test_table(self, db_num: int, table_num: int) -> None:
        """
        Drop test table.
        """
        db_name = self._get_test_db_name(db_num)
        table_name = self._get_test_table_name(table_num)

        query = f"DROP TABLE `{db_name}`.`{table_name}` NO DELAY"
        self._query("POST", query)

    def _get_tables_for_data_comparisson(self) -> dict:
        query = """
            SELECT
                database,
                table,
                groupArray(c.name) "columns"
            FROM system.tables t
            JOIN system.columns c ON (t.database = c.database AND t.name = c.table)
            WHERE database NOT IN ('system', '_temporary_and_external_tables',
                                   'information_schema', 'INFORMATION_SCHEMA')
              AND t.engine NOT IN ('View', 'MaterializedView', 'Distributed')
            GROUP BY database, table
            ORDER BY database, table
            FORMAT JSONCompact
            """
        return self._query("GET", query)["data"]

    def _query(
        self,
        method: str,
        query: str = None,
        url: str = None,
        data: Union[bytes, str] = None,
    ) -> Any:
        if url:
            url = urljoin(self._url, url)
        else:
            url = self._url

        if isinstance(data, str):
            data = data.encode()

        params = copy(self._settings)
        if query:
            params["query"] = query
        if self._user:
            params["user"] = self._user
        if self._password:
            params["password"] = self._password

        try:
            logging.debug("Executing ClickHouse query: %s", query)
            response = self._session.request(
                method, url, params=params, data=data, timeout=self._timeout
            )

            response.raise_for_status()
        except HTTPError as e:
            logging.critical("Error while performing request: %s", e.response.text)
            raise

        try:
            return response.json()
        except ValueError:
            return str.strip(response.text)

    @staticmethod
    def _get_test_db_name(db_num: int) -> str:
        """
        Get test database name
        """
        return f"test_db_{db_num:02d}"

    @staticmethod
    def _get_test_table_name(table_num: int) -> str:
        """
        Get test table name
        """
        return f"test_table_{table_num:02d}"

    @staticmethod
    def _gen_record(row_num=0, str_len=5, str_prefix=None):
        """
        Generate test record.
        """
        if str_prefix is None:
            str_prefix = ""
        else:
            str_prefix = f"{str_prefix}_"

        rand_str = generate_random_string(str_len)

        dt_now = datetime.utcnow()
        row = (
            dt_now.strftime("%Y-%m-%d"),
            dt_now.strftime("%Y-%m-%d %H:%M:%S"),
            str(row_num),
            f"{str_prefix}{rand_str}",
        )

        return row
