"""
Variables that influence testing behavior are defined here.
"""

import random

from tests.integration.helpers import crypto
from tests.integration.helpers.utils import merge

try:
    from local_configuration import CONF_OVERRIDE
except ImportError:
    CONF_OVERRIDE = {}


def get():
    """
    Get configuration (non-idempotent function)
    """
    # This "factor" is later used in the network name and port forwarding.
    port_factor = random.randint(0, 4096)
    # Docker network name. Also used as a project and domain name.
    net_name = 'test_net_{num}'.format(num=port_factor)

    dynamic_config = _generate_dynamic_config(net_name)

    config = {
        # Common conf options.
        # See below for dynamic stuff (keys, certs, etc)
        'dynamic': dynamic_config,
        # Controls whether to perform cleanup after tests execution or not.
        'cleanup': True,
        # Code checkout
        # Where does all the fun happens.
        # Assumption is that it can be safely rm-rf`ed later.
        'staging_dir': 'staging',
        # Docker-related
        'docker_ip4_subnet': '10.%s.0/24',
        'docker_ip6_subnet': 'fd00:dead:beef:%s::/96',
        # See above.
        'port_factor': port_factor,
        # Docker network name. Also doubles as a project and domain name.
        'network_name': net_name,

        # A dict with all projects that are going to interact in this
        # testing environment.
        'projects': {
            'clickhouse': {
                'build':
                    '..',
                'db': {
                    'user': 'dbaas_reader',
                    'password': 'dbaas_reader_password',
                },
                'expose': {
                    'http': 8123,
                    'clickhouse': 9000,
                    'ssh': 22,
                },
                'docker_instances':
                    2,
                'external_links': [
                    '%s:%s' % (dynamic_config['s3']['host'],
                               dynamic_config['s3']['fake_host']),
                ],
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


def _generate_dynamic_config(net_name):
    """
    Generates dynamic stuff like keys, uuids and other.
    """
    keys = {
        'internal_api': crypto.gen_keypair(),
        'client': crypto.gen_keypair(),
    }
    # https://pynacl.readthedocs.io/en/latest/public/#nacl-public-box
    # CryptoBox is a subclass of Box, but returning a string instead.
    api_to_client_box = crypto.CryptoBox(
        keys['internal_api']['secret_obj'],
        keys['client']['public_obj'],
    )
    s3_credentials = {
        'access_secret_key': crypto.gen_plain_random_string(40),
        'access_key_id': crypto.gen_plain_random_string(20),
    }
    config = {
        's3': {
            'host':
                'minio01.{domain}'.format(domain=net_name),
            'fake_host':
                'minio',
            'bucket':
                'dbaas',
            'port':
                9000,
            'endpoint':
                'http://minio:9000',
            'access_secret_key':
                s3_credentials['access_secret_key'],
            'access_key_id':
                s3_credentials['access_key_id'],
            'enc_access_secret_key':
                api_to_client_box.encrypt_utf(
                    s3_credentials['access_secret_key']),
            'enc_access_key_id':
                api_to_client_box.encrypt_utf(s3_credentials['access_key_id']),
            'boto_config': {
                'addressing_style': 'auto',
                'region_name': 'us-east-1',
            },
        },
        'ch_backup': {
            'encrypt_key': crypto.gen_plain_random_string(32),
        },
    }

    return config
