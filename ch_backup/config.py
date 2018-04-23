"""
config module defines Config class and default values
"""

import copy
import logging
import socket

import yaml

DEFAULT_CONFIG = {
    'clickhouse': {
        'data_path': '/var/lib/clickhouse',
        'host': socket.gethostname(),
        'protocol': 'http',
        'port': None,
        'ca_path': None,
        'timeout': 3,
        'user': 'clickhouse',
        'group': 'clickhouse',
    },
    'backup': {
        'exclude_dbs': ['system', 'default'],
        'path_root': None,
        'deduplicate_parts': True,
        'deduplication_age_limit': {
            'days': 7,
        },
        'min_interval': {
            'minutes': 0,
        },
        'retain_time': {},
        'retain_count': None,
    },
    'storage': {
        'type': 's3',
        'credentials': {
            'endpoint_url': None,
            'access_key_id': None,
            'secret_access_key': None,
            'bucket': None,
        },
        'boto_config': {
            'addressing_style': 'auto',
            'region_name': 'us-east-1',
        },
        'disable_ssl_warnings': True,
        'chunk_size': 8 * 1024 * 1024,
        'buffer_size': 128 * 1024 * 1024,
    },
    'encryption': {
        'type': 'nacl',
        'chunk_size': 8 * 1024 * 1024,
        'buffer_size': 128 * 1024 * 1024,
        'key': None,
    },
    'filesystem': {
        'type': 'unlimited',
        'chunk_size': 1 * 1024 * 1024,
        'buffer_size': 128 * 1024 * 1024,
    },
    'multiprocessing': {
        'workers': 4,
    },
    'main': {
        'user': 'clickhouse',
        'group': 'clickhouse',
        'drop_privileges': True,
        'ca_bundle': [],
        'disable_ssl_warnings': False,
    },
    'logging': {
        'log_level_root': 'DEBUG',
        'log_level_aux': 'DEBUG',
        'log_format': '%(asctime)s [%(levelname)s] %(name)s:\t%(message)s',
    },
}


class Config:
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

    def __setitem__(self, item, value):
        try:
            self._conf[item] = value
        except KeyError:
            logging.critical('Config item "%s" was not defined', item)
            raise

    def get(self, key, default=None):
        """
        Returns value by key or default
        """

        return self._conf.get(key, default)
