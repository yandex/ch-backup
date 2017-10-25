"""
Utilities for dealing with ch_backup
"""

import json
import logging
from datetime import datetime, timedelta

import requests

from . import crypto, docker, utils

DB_COUNT = 2
TABLE_COUNT = 2
ROWS_COUNT = 3

CH_BACKUP_CLI_PATH = '/usr/local/bin/ch-backup'
CH_BACKUP_CONF_PATH = '/config/ch-backup.conf'

GET_ALL_USER_TABLES_SQL = utils.strip_query("""
    SELECT database, name as table
    FROM system.tables
    WHERE database NOT IN ('system')
    ORDER BY metadata_modification_time, database, table
    FORMAT JSONCompact
""")

GET_TEST_TABLE_DATA_SQL = utils.strip_query("""
    SELECT *
    FROM {db_name}.{table_name}
    ORDER BY datetime, int_num
    FORMAT JSONCompact
""")

TEST_TABLE_SCHEMA = utils.strip_query("""
    (date Date, datetime DateTime, int_num UInt32, str String)
    engine = MergeTree(date, int_num, 8192)
""")


class ClickhouseClient(object):  # pylint: disable=too-few-public-methods
    """
    Simple clickhouse client
    """

    def __init__(self, config):
        self._config = config
        self._timeout = 5
        self._query_url = '{proto}://{host}:{port}/?query={query}'. \
            format(
                proto=config.get('proto', 'http'),
                host=config.get('host', 'localhost'),
                port=config.get('port', 8123),
                query='{query}')

    def query(self, query_str, post_data=None, timeout=None):
        """
        Perform query to configured clickhouse endpoint
        """

        if timeout is None:
            timeout = self._timeout
        query_url = self._query_url.format(query=query_str)

        http_response = requests.post(
            query_url, data=post_data, timeout=timeout)

        try:
            http_response.raise_for_status()
        except requests.HTTPError:
            logging.critical('Error while performing request: %s',
                             http_response.text)
            raise

        try:
            return http_response.json()
        except ValueError:
            return {}


def get_ch_client(context, node_name, proto=None):
    """
    Get clickhouse client for sending requests to clickhouse.
    """

    if proto is None:
        proto = 'http'

    host, port = docker.get_exposed_port(
        docker.get_container(context, node_name),
        context.conf['projects']['clickhouse']['expose'][proto])
    return ClickhouseClient({'proto': proto, 'host': host, 'port': port})


def get_base_url(context, node_name, proto=None):
    """
    Get clickhouse url for sending requests to clickhouse.
    """

    if proto is None:
        proto = 'http'

    host, port = docker.get_exposed_port(
        docker.get_container(context, node_name),
        context.conf['projects']['clickhouse']['expose'][proto])

    return '{proto}://{host}:{port}'.format(proto=proto, host=host, port=port)


def init_schema(ch_client):
    """
    Create test schema
    """
    for db_num in range(1, DB_COUNT + 1):
        db_name = 'test_db_{db_num:02d}'.format(db_num=db_num)
        ch_client.query('CREATE DATABASE IF NOT EXISTS {db_name}'
                        .format(db_name=db_name))
        for table_num in range(1, TABLE_COUNT + 1):
            table_name = 'test_table_{table_num:02d}'.\
                format(table_num=table_num)
            ch_client.query('CREATE TABLE IF NOT EXISTS '
                            '{db_name}.{table_name} {table_schema}'.format(
                                db_name=db_name,
                                table_name=table_name,
                                table_schema=TEST_TABLE_SCHEMA))


def fill_with_data(ch_client, mark=None):
    """
    Fill test schema with data
    """

    if mark is None:
        mark = ''
    for db_num in range(1, DB_COUNT + 1):
        db_name = 'test_db_{db_num:02d}'.format(db_num=db_num)
        for table_num in range(1, TABLE_COUNT + 1):
            rows = []
            table_name = 'test_table_{table_num:02d}'.\
                format(table_num=table_num)
            for row_num in range(1, ROWS_COUNT + 1):
                rows.append(', '.join(
                    gen_record(row_num=row_num, str_prefix=mark)))

            ch_client.query(
                'INSERT INTO {db_name}.{table_name} FORMAT CSV'.format(
                    db_name=db_name, table_name=table_name),
                post_data='\n'.join(rows))


def gen_record(row_num=0, day_diff=None, str_len=5, str_prefix=None):
    """
    Generate test record
    """

    if day_diff is None:
        day_diff = {'days': 0}
    if str_prefix is None:
        str_prefix = ''
    else:
        str_prefix = '{prefix}_'.format(prefix=str_prefix)

    rand_str = crypto.gen_plain_random_string(str_len)

    dt_now = datetime.now() - timedelta(**day_diff)
    row = (dt_now.strftime('%Y-%m-%d'), dt_now.strftime('%Y-%m-%d %H:%M:%S'),
           str(row_num), '{prefix}{rand_str}'.format(
               prefix=str_prefix, rand_str=rand_str))

    return row


def make_backup(ch_instance, cli_path=None, conf_path=None):
    """
    Call ch-backup cli to run backup
    """

    if cli_path is None:
        cli_path = CH_BACKUP_CLI_PATH
    if conf_path is None:
        conf_path = CH_BACKUP_CONF_PATH

    output = ch_instance.exec_run('{cli_path} -c {conf_path} backup'.format(
        cli_path=cli_path, conf_path=conf_path))
    return output.decode()


def restore_backup_num(ch_instance, backup_num):
    """
    Call ch-backup cli to run restore backup by serial number
    """
    backup_entries = get_backup_entries(ch_instance)
    restore_backup_entry(ch_instance, backup_entries[backup_num])


def get_backup_entries(ch_instance, cli_path=None, conf_path=None):
    """
    Call ch-backup cli to retrieve existing backup entries
    """

    if cli_path is None:
        cli_path = CH_BACKUP_CLI_PATH
    if conf_path is None:
        conf_path = CH_BACKUP_CONF_PATH
    output = ch_instance.exec_run('{cli_path} -c {conf_path} list'.format(
        cli_path=cli_path, conf_path=conf_path))
    raw_entries = output.decode().split('\n')
    return list(filter(None, raw_entries))


def restore_backup_entry(ch_instance,
                         backup_entry,
                         cli_path=None,
                         conf_path=None):
    """
    Call ch-backup cli to run restore backup entry
    """

    if cli_path is None:
        cli_path = CH_BACKUP_CLI_PATH
    if conf_path is None:
        conf_path = CH_BACKUP_CONF_PATH
    ch_instance.exec_run('{cli_path} -c {conf_path} -p {backup_entry} restore'
                         .format(
                             cli_path=cli_path,
                             conf_path=conf_path,
                             backup_entry=backup_entry))


def get_all_user_data(ch_client):
    """
    Retrieve all user data
    """

    dbs_tables = ch_client.query(GET_ALL_USER_TABLES_SQL)['data']
    user_data = {}
    rows_count = 0
    for db_table in dbs_tables:
        db_name, table_name = db_table
        query_sql = GET_TEST_TABLE_DATA_SQL.format(
            db_name=db_name, table_name=table_name)
        table_data = ch_client.query(query_sql)
        user_data['.'.join([db_name, table_name])] = table_data['data']
        rows_count += table_data['rows']
    return rows_count, user_data


def count_deduplicated_parts(context,
                             node_name,
                             entry_num,
                             cli_path=None,
                             conf_path=None):
    """
    Count deduplicated parts in backup struct for given node and backup number
    """

    if cli_path is None:
        cli_path = CH_BACKUP_CLI_PATH
    if conf_path is None:
        conf_path = CH_BACKUP_CONF_PATH

    ch_instance = docker.get_container(context, node_name)
    backup_entry = get_backup_entries(ch_instance)[entry_num]
    backup_json = ch_instance.exec_run(
        '{cli_path} -c {conf_path} -p {backup_entry} show'.format(
            cli_path=cli_path, conf_path=conf_path, backup_entry=backup_entry))
    backup_meta = json.loads(backup_json.decode())

    links_count = 0
    for _, db_contents in backup_meta['databases'].items():
        for _, table_contents in db_contents['parts_paths'].items():
            for _, part_contents in table_contents.items():
                if part_contents['link']:
                    links_count += 1

    return links_count
