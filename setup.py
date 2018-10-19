#!/usr/bin/env python3
# encoding: utf-8
"""
setup.py for DBaaS ch-backup
"""

from setuptools import setup, find_packages


REQUREMENTS = [
    'requests',
    'boto3',
    'botocore',
    'PyYAML',
    'PyNaCl',
    'click',
    'tabulate',
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
        'ch-backup = ch_backup:main',
    ]},
    install_requires=REQUREMENTS,
)
