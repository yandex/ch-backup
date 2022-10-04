"""
util module defines various auxiliary functions
"""

import grp
import os
import pwd
import re
import shutil
import time
from datetime import datetime, timedelta, timezone
from typing import Callable, Iterable, Tuple, Union

import humanfriendly
import tenacity

from ch_backup import logging
from ch_backup.exceptions import ClickhouseBackupError

LOCAL_TZ = timezone(timedelta(seconds=-1 * (time.altzone if time.daylight else time.timezone)))


def chown_dir_contents(user: str, group: str, dir_path: str) -> None:
    """
    Recursively change directory user/group
    """
    for path in os.listdir(dir_path):
        shutil.chown(os.path.join(dir_path, path), user, group)


def setup_environment(config: dict) -> None:
    """
    Set environment variables
    """
    try:
        env_value = ':'.join(config['ca_bundle'])
        os.environ['REQUESTS_CA_BUNDLE'] = env_value
    except KeyError:
        pass


def demote_group(new_group: str) -> None:
    """
    Perform group change
    """
    new_gid = grp.getgrnam(new_group).gr_gid
    os.setgid(new_gid)


def demote_user(new_user: str) -> None:
    """
    Perform user change
    """
    new_uid = pwd.getpwnam(new_user).pw_uid
    os.setuid(new_uid)


def escape(string: str) -> str:
    """
    Escaping special character '`'
    """
    return r'\`'.join(string.split('`'))


def demote_user_group(new_user: str, new_group: str) -> None:
    """
    Perform user and group change
    """
    demote_group(new_group)
    demote_user(new_user)


def drop_privileges(config: dict) -> bool:
    """
    Demote user/group if needed
    """

    try:
        if config['drop_privileges']:
            demote_user_group(config['user'], config['group'])
            return True
    except KeyError:
        pass

    return False


def strip_query(query_text: str) -> str:
    """
    Remove query without newlines and duplicate whitespaces.
    """
    return re.sub(r'\s{2,}', ' ', query_text.replace('\n', ' ')).strip()


def now() -> datetime:
    """
    Return local datetime with timezone information.
    """
    return datetime.now(LOCAL_TZ)


def utcnow() -> datetime:
    """
    Return UTC datetime with timezone information.
    """
    return datetime.now(timezone.utc)


def retry(exception_types: Union[type, tuple] = Exception,
          max_attempts: int = 5,
          max_interval: float = 5,
          retry_if: tenacity.retry_base = tenacity.retry_always) -> Callable:
    """
    Function decorator that retries wrapped function on failures.
    """
    def _log_retry(retry_state):
        logging.debug("Retrying %s.%s in %.2fs, attempt: %s, reason: %r", retry_state.fn.__module__,
                      retry_state.fn.__qualname__, retry_state.next_action.sleep, retry_state.attempt_number,
                      retry_state.outcome.exception())

    return tenacity.retry(retry=tenacity.retry_all(tenacity.retry_if_exception_type(exception_types), retry_if),
                          wait=tenacity.wait_random_exponential(multiplier=0.5, max=max_interval),
                          stop=tenacity.stop_after_attempt(max_attempts),
                          reraise=True,
                          before_sleep=_log_retry)


def get_table_zookeeper_paths(tables: Iterable) -> Iterable[Tuple]:
    """
    Parse ZooKeeper path from create statement.
    """
    result = []
    for table in tables:
        match = re.search(R"""Replicated\S{0,20}MergeTree\(\'(?P<zk_path>[^']+)\',""", table.create_statement)
        if not match:
            raise ClickhouseBackupError(f'Couldn`t parse create statement for zk path: "{table}')
        result.append((table, match.group('zk_path')))
    return result


def get_database_zookeeper_paths(databases: Iterable[str]) -> Iterable[str]:
    """
    Parse ZooKeeper path from create statement.
    """
    result = []
    for db_sql in databases:
        match = re.search(R"""Replicated\(\'(?P<zk_path>[^']+)\', '(?P<shard>[^']+)', '(?P<replica>[^']+)'""", db_sql)
        if not match:
            continue
        result.append(f'{match.group("zk_path")}/replicas/{match.group("shard")}|{match.group("replica")}')
    return result


def compare_schema(schema_a: str, schema_b: str) -> bool:
    """
    Normalize table schema for comparison.
    `... ENGINE = Distributed('aaa', bbb, ccc, xxx) ...` may be in ver. before 19.16, 20.1
    `... ENGINE = Distributed('aaa', 'bbb', 'ccc', xxx) ...` in ver. 19.16+, 20.1+
    """
    def _normalize(schema: str) -> str:
        return re.sub(r"ENGINE = Distributed\('([^']+)', ('?)(\w+)\2, ('?)(\w+)\4(, .*)?\)",
                      r"ENGINE = Distributed('\1', '\3', '\5'\6)", schema).lower()

    return _normalize(schema_a) == _normalize(schema_b)


def format_size(value: int) -> str:
    """
    Format a value in bytes to human-friendly representation.
    """
    return humanfriendly.format_size(value, binary=True)
