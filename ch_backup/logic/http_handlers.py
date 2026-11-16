"""Backup and restore SQL-defined HTTP handlers through ClickHouse SQL."""

from ch_backup import logging
from ch_backup.backup_context import BackupContext
from ch_backup.exceptions import ClickhouseBackupError
from ch_backup.logic.backup_manager import BackupManager


class HTTPHandlersBackup(BackupManager):
    """Storage-independent backup of SQL-defined HTTP handlers."""

    def backup(self, context: BackupContext) -> None:
        """Save canonical CREATE statements, including their secrets."""
        if not context.ch_ctl.ch_version_ge("26.8"):
            return

        handlers = context.ch_ctl.get_http_handlers_create_queries()
        for name, statement in handlers.items():
            context.backup_layout.upload_http_handler(
                context.backup_meta.name, name, statement
            )
            context.backup_meta.add_http_handler(name)

    def restore(self, context: BackupContext) -> None:
        """Skip identical handlers and replace changed definitions."""
        names = context.backup_meta.get_http_handlers()
        if not names:
            return
        if not context.ch_ctl.ch_version_ge("26.8"):
            raise ClickhouseBackupError(
                "Restoring SQL-defined HTTP handlers requires ClickHouse 26.8 or newer"
            )

        existing = context.ch_ctl.get_http_handlers_create_queries(names)
        # Read every definition before changing the server. Drop changed handlers
        # before creating any: their URLs may have been swapped in the backup.
        statements = {
            name: context.backup_layout.get_http_handler_create_statement(
                context.backup_meta, name
            )
            for name in names
        }
        changed = {
            name: statement
            for name, statement in statements.items()
            if existing.get(name) != statement
        }
        for name in changed:
            if name in existing:
                logging.warning(
                    "Replacing HTTP handler {} with its backup definition", name
                )
                context.ch_ctl.drop_http_handler(name)
        for name, statement in changed.items():
            context.ch_ctl.restore_http_handler(statement)
            logging.debug("HTTP handler {} restored", name)
