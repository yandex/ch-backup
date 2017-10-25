"""
config module defines Config class and default values
"""

import copy
import logging

import yaml

DEFAULT_CONFIG = {
    'clickhouse': {
        'data_path': '/var/lib/clickhouse',
        'host': '127.0.0.1',
        'port': '8123',
        'timeout': '3',
        'user': 'clickhouse',
        'group': 'clickhouse',
    },
    'backup': {
        'exclude_dbs': ['system', 'default'],
        'path_root': 'ch_backup',
        'deduplicate_parts': True,
        'deduplication_age_limit': {
            'seconds': 300,
        },
    },
    's3': {
        'credentials': {
            'endpoint_url': None,
            'access_key_id': None,
            'secret_access_key': None,
            'bucket': None,
        },
        'disable_ssl_warnings': True,
    },
    'main': {
        'user': 'clickhouse',
        'group': 'clickhouse',
        'drop_privileges': True,
        'ca_bundle': [],
        'disable_ssl_warnings': False,
        'storage': None,
    },
    'logging': {
        'log_level_root': 'DEBUG',
        'log_level_aux': 'DEBUG',
        'log_format': '%(asctime)s [%(levelname)s] %(name)s:\t%(message)s',
    },
}


# pylint: disable=too-few-public-methods
class Config(object):
    """
    Config for all components
    """

    def __init__(self, config_file):
        self._conf = copy.deepcopy(DEFAULT_CONFIG)
        self._read_config(file_name=config_file)

    def _recursively_update(self, base_dict, update_dict):
        for key, value in update_dict.items():
            if isinstance(value, dict):
                if key not in base_dict:
                    base_dict[key] = {}
                self._recursively_update(base_dict[key], update_dict[key])
            else:
                base_dict[key] = value

    def _read_config(self, file_name):
        with open(file_name, 'r') as fileobj:
            try:
                custom_config = yaml.safe_load(fileobj)
                if custom_config:
                    self._recursively_update(self._conf, custom_config)
            except yaml.YAMLError as exc:
                logging.error('Unable to load config file(%s): %s', file_name,
                              exc)

    def __getitem__(self, item):
        try:
            return self._conf[item]
        except KeyError:
            logging.critical('Config item "%s" was not defined', item)
            raise
