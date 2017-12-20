#!/usr/bin/env python3
# encoding: utf-8
"""
setup.py for DBaaS ch-backup
"""

from setuptools import setup, find_packages


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
    description='DBaaS ClickHouse backup tool',
    license='Yandex License',
    url='https://github.yandex-team.ru/mdb/ch-backup/',
    author='DBaaS team',
    author_email='mdb-admin@yandex-team.ru',
    maintainer='DBaaS team',
    maintainer_email='mdb-admin@yandex-team.ru',
    zip_safe=False,
    platforms=['Linux', 'BSD', 'MacOS'],
    packages=find_packages(exclude=['tests*']),
    entry_points={'console_scripts': [
        'ch-backup = ch_backup.main:main',
    ]},
    install_requires=REQUREMENTS,
)
