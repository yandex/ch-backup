"""
Variables that influence testing behavior are defined here.
"""

import random

from tests.integration.modules.utils import generate_random_string, merge

try:
    from local_configuration import CONF_OVERRIDE
except ImportError:
    CONF_OVERRIDE = {}


def create():
    """
    Create test configuration (non-idempotent function).
    """
    # Docker network name. Also used as an instance and domain name.
    network_suffix = random.randint(0, 4096)
    network_name = f'test_net_{network_suffix}'

    s3 = {
        'container': 'minio01',
        'host': f'minio01.{network_name}',
        'bucket': 'ch-backup',
        'port': 9000,
        'endpoint': 'http://minio01:9000',
        'access_secret_key': generate_random_string(40),
        'access_key_id': generate_random_string(20),
        'proxy_resolver': {
            'uri': f'http://proxy-api01.{network_name}:8080',
            'proxy_port': 4080,
        },
        'boto_config': {
            'addressing_style': 'auto',
            'region_name': 'us-east-1',
        },
    }

    zk = {
        'uri': f'zookeeper01.{network_name}',
        'port': 2181,
    }

    config = {
        'images_dir': 'images',
        'staging_dir': 'staging',
        'network_name': network_name,
        's3': s3,
        'zk': zk,
        'ch_backup': {
            'encrypt_key': generate_random_string(32),
        },

        # A dict with all services that are going to interact in this
        # testing environment.
        'services': {
            'clickhouse': {
                'db': {
                    'user': 'reader',
                    'password': 'reader_password',
                },
                'expose': {
                    'http': 8123,
                    'clickhouse': 9000,
                    'ssh': 22,
                },
                'volumes': ['../:/code:rw'],
                'docker_instances': 2,
                'depends_on': ['minio', 'proxy', 'proxy-api', 'zookeeper'],
                'external_links': [f'{s3["host"]}:minio', f'{zk["uri"]}:zookeeper'],
                'args': {
                    'CLICKHOUSE_VERSION': '$CLICKHOUSE_VERSION',
                },
            },
            'minio': {
                'expose': {
                    'http': s3['port'],
                },
            },
            'proxy': {
                'container': 'proxy01',
                'expose': {
                    'tcp': 4080,
                },
            },
            'broken-proxy': {
                'expose': {
                    'http': 4080,
                },
            },
            'proxy-api': {
                'expose': {
                    'http': 8080,
                },
                'environment': [f'NETWORK={network_name}'],
            },
            'zookeeper': {
                'expose': {
                    'tcp': 2181,
                },
            },
        },
    }
    return merge(config, CONF_OVERRIDE)
