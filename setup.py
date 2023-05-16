#!/usr/bin/env python3
# encoding: utf-8
"""
Package configuration for ch-backup.
"""

from setuptools import setup, find_packages

REQUIREMENTS = [
    'requests',
    'boto3',
    'botocore',
    'PyYAML',
    'PyNaCl',
    'click',
    'tabulate',
    'tenacity',
    'pypeln'
]


with open('ch_backup/version.txt') as f:
    VERSION = f.read().strip()


setup(
    name='ch-backup',
    version=VERSION,
    description='MDB ClickHouse backup tool',
    license='Yandex License',
    url='https://github.yandex-team.ru/mdb/ch-backup/',
    author='MDB team',
    author_email='mdb-admin@yandex-team.ru',
    maintainer='MDB team',
    maintainer_email='mdb-admin@yandex-team.ru',
    zip_safe=False,
    platforms=['Linux', 'BSD', 'MacOS'],
    packages=find_packages(exclude=['tests*']),
    package_data={
        '': ['version.txt'],
    },
    entry_points={'console_scripts': [
        'ch-backup = ch_backup:main',
    ]},
    install_requires=REQUIREMENTS,
)
