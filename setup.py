#!/usr/bin/env python3
# encoding: utf-8
"""
setup.py for DBaaS ch-backup
"""

try:
    from setuptools import setup
except ImportError:
    from distutils import setup


REQUREMENTS = [
    'requests>=2.13.0',
    'retrying>=1.3.3',
    'boto3==1.4.7',
    'botocore==1.7.43',
    'PyYAML>=3.10',
    'PyNaCl==1.1.2',
]


setup(
    name='ch-backup',
    version='0.0.1',
    description='DBaaS Clickhouse backup',
    license='Yandex License',
    url='https://github.yandex-team.ru/mdb/ch-backup/',
    author='DBaaS team',
    author_email='mdb-admin@yandex-team.ru',
    maintainer='DBaaS team',
    maintainer_email='mdb-admin@yandex-team.ru',
    zip_safe=False,
    platforms=['Linux', 'BSD', 'MacOS'],
    packages=['ch_backup', 'ch_backup.storage'],
    package_dir={
        'ch_backup': 'ch_backup',
        'ch_backup.storage': 'ch_backup/storage',
        'ch_backup.encryption': 'ch_backup/encryption',
    },
    entry_points={'console_scripts': [
        'ch-backup = ch_backup.main:main',
    ]},
    install_requires=REQUREMENTS,
)
