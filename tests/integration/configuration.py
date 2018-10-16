"""
Variables that influence testing behavior are defined here.
"""

import random

from tests.integration.helpers.crypto import generate_random_string
from tests.integration.helpers.utils import merge

try:
    from local_configuration import CONF_OVERRIDE
except ImportError:
    CONF_OVERRIDE = {}


def create():
    """
    Create test configuration (non-idempotent function).
    """
    # Docker network name. Also used as a project and domain name.
    network_name = 'test_net_{0}'.format(random.randint(0, 4096))

    s3 = {
        'container': 'minio01',
        'host': 'minio01.{0}'.format(network_name),
        'bucket': 'dbaas',
        'port': 9000,
        'endpoint': 'http://minio:9000',
        'access_secret_key': generate_random_string(40),
        'access_key_id': generate_random_string(20),
        'boto_config': {
            'addressing_style': 'auto',
            'region_name': 'us-east-1',
        },
    }

    config = {
        'staging_dir': 'staging',
        'docker_ip4_subnet': '10.%s.0/24',
        'docker_ip6_subnet': 'fd00:dead:beef:%s::/96',
        'network_name': network_name,
        's3': s3,
        'ch_backup': {
            'encrypt_key': generate_random_string(32),
        },

        # A dict with all projects that are going to interact in this
        # testing environment.
        'projects': {
            'clickhouse': {
                'build': '..',
                'db': {
                    'user': 'dbaas_reader',
                    'password': 'dbaas_reader_password',
                },
                'expose': {
                    'http': 8123,
                    'clickhouse': 9000,
                    'ssh': 22,
                },
                'volumes': ['../:/code:rw'],
                'docker_instances': 2,
                'external_links': ['{0}:minio'.format(s3['host'])],
                'args': {
                    'CLICKHOUSE_VERSION': '$CLICKHOUSE_VERSION',
                },
            },
            'minio': {
                'build': 'images/minio',
                'expose': {
                    'http': 9000,
                },
            },
        },
    }
    return merge(config, CONF_OVERRIDE)
