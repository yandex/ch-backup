"""
argument parsing and validation logic
"""

import argparse
import re

DEFAULT_CONFIG_FILE = '/etc/yandex/ch-backup/ch-backup.conf'


def database(arg):
    """
    Database type for argument validation
    """
    db_re = re.compile(r'\w+')
    if not db_re.match(arg):
        raise ValueError('Bad database type')
    return str(arg)


def parse_args():
    """
    Parse command-line arguments
    """

    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('action', type=str, help='Command name')

    arg_parser.add_argument(
        '-d', '--databases', type=database, help='Database names')
    arg_parser.add_argument('-p', '--path', type=str, help='Backup entry path')

    arg_parser.add_argument(
        '-c',
        '--config',
        type=str,
        default=DEFAULT_CONFIG_FILE,
        help='Config file')

    return arg_parser.parse_args()
