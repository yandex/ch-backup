"""
Clickhouse backup logic for access entities.
"""

import os
import re
import shutil
from typing import Any, Dict, List, Sequence, Union

from kazoo.client import KazooClient
from kazoo.exceptions import NoNodeError

from ch_backup import logging
from ch_backup.backup.metadata import BackupStorageFormat
from ch_backup.backup_context import BackupContext
from ch_backup.logic.backup_manager import BackupManager
from ch_backup.util import (
    chown_dir_contents,
    copy_directory_content,
    temporary_directory,
)

CH_MARK_FILE = "need_rebuild_lists.mark"


class AccessBackup(BackupManager):
    """
    Access backup class
    """

    def backup(self, context: BackupContext) -> None:
        """
        Backup access control entities.
        """

        clickhouse_access_path = context.ch_ctl_conf["access_control_path"]
        backup_tmp_path = os.path.join(
            context.ch_ctl_conf["tmp_path"], context.backup_meta.name
        )
        user = context.ch_ctl_conf["user"]
        group = context.ch_ctl_conf["group"]

        os.makedirs(clickhouse_access_path, exist_ok=True)
        shutil.chown(clickhouse_access_path, user, group)

        with temporary_directory(backup_tmp_path, user, group):
            objects = context.ch_ctl.get_access_control_objects()
            context.backup_meta.set_access_control(objects)
            access_control = context.backup_meta.access_control

            if self._has_replicated_access(context):
                self._backup_replicated(
                    backup_tmp_path, access_control.acl_ids, context
                )
            self._backup_local(clickhouse_access_path, backup_tmp_path)

            assert (
                context.backup_meta.access_control.backup_format
                == BackupStorageFormat.TAR
            )
            chown_dir_contents(user, group, backup_tmp_path)

            acl_file_names = _get_access_control_files(access_control.acl_ids)
            context.backup_layout.upload_access_control_files(
                backup_tmp_path,
                context.backup_meta.name,
                acl_file_names,
            )

    def restore(self, context: BackupContext) -> None:
        """
        Restore access rights
        """
        access_control = context.backup_meta.access_control
        acl_ids, acl_meta = access_control.acl_ids, access_control.acl_meta

        if not acl_ids:
            logging.debug("No access control entities to restore.")
            return

        has_replicated_access = self._has_replicated_access(context)

        clickhouse_access_path = context.ch_ctl_conf["access_control_path"]
        restore_tmp_path = os.path.join(
            context.ch_ctl_conf["tmp_path"], context.backup_meta.name
        )
        user = context.ch_ctl_conf["user"]
        group = context.ch_ctl_conf["group"]

        if os.path.exists(clickhouse_access_path):
            shutil.rmtree(clickhouse_access_path)

        os.makedirs(clickhouse_access_path)
        shutil.chown(clickhouse_access_path, user, group)

        with temporary_directory(restore_tmp_path, user, group):
            self._download_access_control_list(context, restore_tmp_path, acl_ids)

            if has_replicated_access:
                self._restore_replicated(restore_tmp_path, acl_ids, acl_meta, context)
            else:
                self._restore_local(
                    restore_tmp_path, clickhouse_access_path, user, group
                )

    def fix_admin_user(self, context: BackupContext, dry_run: bool = True) -> None:
        """
        Check and fix potential duplicates of `admin` user in Keeper and local storage.
        """
        if not self._has_replicated_access(context):
            logging.info(
                "Cluster uses local storage for access entities, nothing to check."
            )
            return

        admin_user_id = context.ch_ctl.get_zookeeper_admin_id()
        admin_uuids = context.ch_ctl.get_zookeeper_admin_uuid()

        if admin_user_id not in admin_uuids:
            raise ValueError(
                f"Linked admin uuid {admin_user_id} not found, manual check required!"
            )

        if len(admin_uuids) == 1:
            logging.debug(
                f"Found only one admin user with {admin_user_id} id, nothing to fix."
            )
            return

        to_delete = []
        # check that all duplicates have the same password hash, settings and grants
        admin_create_str = self._clean_user_uuid(admin_uuids[admin_user_id])
        for uuid, create_str in admin_uuids.items():
            if uuid == admin_user_id:
                continue
            cleaned_create_str = self._clean_user_uuid(create_str)
            if admin_create_str == cleaned_create_str:
                to_delete.append(uuid)
            else:
                raise ValueError(
                    f"Duplicate {uuid} has differences, manual check required!"
                )

        # cleanup ZK paths and local files
        with context.zk_ctl.zk_client as zk_client:
            for uuid in to_delete:
                # cleanup ZK duplicate
                zk_path = _get_access_zk_path(context, f"/uuid/{uuid}")
                logging.debug(f"Removing zk path {zk_path}")
                try:
                    if dry_run:
                        logging.debug(
                            f"Skipped removing zk path {zk_path} due to dry-run"
                        )
                    else:
                        zk_client.delete(zk_path)
                except NoNodeError:
                    logging.debug(f"Node {zk_path} not found.")

                # cleanup SQL file
                file_path = os.path.join(
                    context.ch_ctl_conf["access_control_path"], f"{uuid}.sql"
                )
                logging.debug(f"Removing file {file_path}")
                try:
                    if dry_run:
                        logging.debug(
                            f"Skipped removing file {file_path} due to dry-run"
                        )
                    else:
                        os.remove(file_path)
                except FileNotFoundError:
                    logging.debug(f"File {file_path} not found.")

    def _download_access_control_list(
        self, context: BackupContext, restore_tmp_path: str, acl_ids: List[str]
    ) -> None:
        if context.backup_meta.access_control.backup_format == BackupStorageFormat.TAR:
            context.backup_layout.download_access_control(
                restore_tmp_path, context.backup_meta.name
            )
        else:
            for name in _get_access_control_files(acl_ids):
                context.backup_layout.download_access_control_file(
                    restore_tmp_path, context.backup_meta.name, name
                )

    def _clean_user_uuid(self, raw_str: str) -> str:
        return re.sub(r"EXCEPT ID\('(.+)'\)", "", raw_str)

    def _backup_local(self, clickhouse_access_path: str, backup_tmp_path: str) -> None:
        """
        Backup access entities from local storage to temporary folder.
        """

        logging.debug(
            "Backupping access entities from local storage to {}", backup_tmp_path
        )
        copy_directory_content(clickhouse_access_path, backup_tmp_path)

    def _backup_replicated(
        self, backup_tmp_path: str, acl_list: Sequence[str], context: BackupContext
    ) -> None:
        """
        Backup access entities from replicated storage (ZK/CK).
        """
        logging.debug(
            f"Backupping {len(acl_list)} access entities from replicated storage"
        )
        with context.zk_ctl.zk_client as zk_client:
            for uuid in acl_list:
                uuid_zk_path = _get_access_zk_path(context, f"/uuid/{uuid}")
                data, _ = zk_client.get(uuid_zk_path)
                _create_access_file(backup_tmp_path, f"{uuid}.sql", data.decode())

    def _restore_local(
        self,
        restore_tmp_path: str,
        clickhouse_access_path: str,
        user: str,
        group: str,
    ) -> None:
        """
        Restore access entities to local storage.
        """
        copy_directory_content(restore_tmp_path, clickhouse_access_path)
        self._mark_to_rebuild(clickhouse_access_path, user, group)

    def _restore_replicated(
        self,
        restore_tmp_path: str,
        acl_list: Sequence[str],
        acl_meta: Dict[str, Dict[str, Any]],
        context: BackupContext,
    ) -> None:
        """
        Restore access entities to replicated storage (ZK/CK).
        """
        logging.debug(
            f"Restoring {len(acl_list)} access entities to replicated storage"
        )
        if not acl_meta:
            logging.warning(
                "Can not restore access entities to replicated storage without meta information!"
            )
            return

        with context.zk_ctl.zk_client as zk_client:
            for i, uuid in enumerate(acl_list):
                meta_data = acl_meta[str(i)]
                name, obj_char = meta_data["name"], meta_data["char"]

                # restore object data
                file_path = os.path.join(restore_tmp_path, f"{uuid}.sql")
                with open(file_path, "r", encoding="utf-8") as file:
                    data = file.read()
                    uuid_zk_path = _get_access_zk_path(context, f"/uuid/{uuid}")
                    _zk_upsert_data(zk_client, uuid_zk_path, data)

                # restore object link
                uuid_zk_path = _get_access_zk_path(context, f"/{obj_char}/{name}")
                _zk_upsert_data(zk_client, uuid_zk_path, uuid)

    def _mark_to_rebuild(
        self, clickhouse_access_path: str, user: str, group: str
    ) -> None:
        """
        Creates special mark file to rebuild the lists.
        """
        mark_file = os.path.join(clickhouse_access_path, CH_MARK_FILE)
        with open(mark_file, "a", encoding="utf-8"):
            pass
        shutil.chown(mark_file, user, group)

    def _has_replicated_access(self, context: BackupContext) -> bool:
        return (
            context.ch_config.config.get("user_directories", {}).get("replicated")
            is not None
        )


def _get_access_control_files(objects: Sequence[str]) -> List[str]:
    """
    Return list of file to be backuped/restored.
    """

    return [f"{obj}.sql" for obj in objects]


def _get_access_zk_path(context: BackupContext, zk_path: str) -> str:
    paths = (
        context.zk_ctl.zk_root_path,
        context.ch_ctl_conf["zk_access_control_path"],
        zk_path,
    )
    return "/" + os.path.join(*map(lambda x: x.lstrip("/"), paths))


def _zk_upsert_data(zk: KazooClient, path: str, value: Union[str, bytes]) -> None:
    if isinstance(value, str):
        value = value.encode()

    logging.debug(f'Upserting zk access entity "{path}"')
    if zk.exists(path):
        zk.set(path, value)
    else:
        zk.create(path, value, makepath=True)


def _create_access_file(
    backup_tmp_path: str, file_name: str, file_content: str = ""
) -> str:
    file_path = os.path.join(backup_tmp_path, file_name)
    logging.debug(f'Creating "{file_path}" access entity file')
    with open(file_path, "w", encoding="utf-8") as file:
        file.write(file_content)

    return file_path
