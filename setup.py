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
    description='Backup tool for ClickHouse DBMS.',
    license='MIT',
    url='https://github.com/yandex/ch-backup',
    author='Yandex LLC',
    author_email='opensource@yandex-team.ru',
    zip_safe=False,
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "Intended Audience :: System Administrators",
        "License :: OSI Approved :: MIT License",
        "Operating System :: MacOS",
        "Operating System :: POSIX :: BSD",
        "Operating System :: POSIX :: Linux",
        "Operating System :: Unix",
        "Environment :: Console",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Topic :: Database",
        "Typing :: Typed",
    ],
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
