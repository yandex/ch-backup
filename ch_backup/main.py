#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
main module defines ch_backup
"""

import logging

from ch_backup.args import parse_args
from ch_backup.backup import ClickhouseBackup
from ch_backup.clickhouse.control import ClickhouseCTL
from ch_backup.clickhouse.layout import ClickhouseBackupLayout
from ch_backup.config import Config
from ch_backup.pipeline import PipelineLoader
from ch_backup.util import (drop_privileges, pretty_print, setup_environment,
                            setup_logging)


def main():
    """
    Entry point
    """

    args = parse_args()

    config = Config(args.config)

    setup_logging(config['logging'])
    setup_environment(config['main'])

    if not drop_privileges(config['main']):
        logging.warning('Drop privileges was disabled in config file.')

    storage_loader = PipelineLoader(config)
    ch_ctl = ClickhouseCTL(config['clickhouse'])
    backup_layout = ClickhouseBackupLayout(storage_loader, ch_ctl,
                                           config['backup'])

    ch_backup = ClickhouseBackup(config['backup'], ch_ctl, backup_layout)

    cli_commands = ch_backup.commands

    if args.action not in cli_commands:
        logging.fatal('Unknown command: %s', args.action)
    else:
        logging.info('Running %s', args.action)
        pretty_print(cli_commands[args.action](
            databases=args.databases, path=args.path))

    logging.info('Exiting')


if __name__ == '__main__':
    main()
