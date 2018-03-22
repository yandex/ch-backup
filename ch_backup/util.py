"""
util module defines various auxiliary functions
"""

import grp
import logging
import os
import pwd
import re
import shutil
import time
from datetime import datetime, timedelta, timezone

LOCAL_TZ = timezone(
    timedelta(seconds=-1 * (time.altzone if time.daylight else time.timezone)))


def chown_dir_contents(user, group, dir_path):
    """
    Recursively change directory user/group
    """
    for path in os.listdir(dir_path):
        shutil.chown(os.path.join(dir_path, path), user, group)


def setup_logging(config):
    """
    Configure logging
    """
    root_level = getattr(logging, config['log_level_root'].upper(), None)

    log_file = config.get('log_file', None)
    logging.basicConfig(
        level=root_level, format=config['log_format'], filename=log_file)

    aux_level = getattr(logging, config['log_level_aux'].upper(), None)
    for module_logger in ('boto3', 'botocore', 's3transfer', 'urllib3'):
        logging.getLogger(module_logger).setLevel(aux_level)


def setup_environment(config):
    """
    Set environment variables
    """
    try:
        env_value = ':'.join(config['ca_bundle'])
        os.environ['REQUESTS_CA_BUNDLE'] = env_value
    except KeyError:
        pass


def demote_group(new_group):
    """
    Perform group change
    """
    new_gid = grp.getgrnam(new_group).gr_gid
    os.setgid(new_gid)


def demote_user(new_user):
    """
    Perform user change
    """
    new_uid = pwd.getpwnam(new_user).pw_uid
    os.setuid(new_uid)


def demote_user_group(new_user, new_group):
    """
    Perform user and group change
    """
    demote_group(new_group)
    demote_user(new_user)


def drop_privileges(config):
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


def strip_query(query_text):
    """
    Remove query without endlines and duplicate whitespaces
    """
    return re.sub(r'\s{2,}', ' ', query_text.replace('\n', ' ')).strip()


def now():
    """
    Return local datetime with timezone information.
    """
    return datetime.now(LOCAL_TZ)


def utcnow():
    """
    Return UTC datetime with timezone information.
    """
    return datetime.now(timezone.utc)
