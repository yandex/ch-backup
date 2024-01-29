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
from ch_backup.util import chown_dir_contents

CH_MARK_FILE = "need_rebuild_lists.mark"


class AccessBackup(BackupManager):
    """
    Access backup class
    """

    def backup(self, context: BackupContext) -> None:
        """
        Backup access control entities.
        """
        objects = context.ch_ctl.get_access_control_objects()
        context.backup_meta.set_access_control(objects)
        access_control = context.backup_meta.access_control

        if self._has_replicated_access(context):
            self._backup_replicated(access_control.acl_ids, context)
        self._backup_local(access_control.acl_ids, context)

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
        mark_to_rebuild = not has_replicated_access

        self._restore_local(acl_ids, context, mark_to_rebuild=mark_to_rebuild)
        if has_replicated_access:
            self._restore_replicated(acl_ids, acl_meta, context)

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
                file_path = _get_access_file_path(context, f"{uuid}.sql")
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

    def _clean_user_uuid(self, raw_str: str) -> str:
        return re.sub(r"EXCEPT ID\('(.+)'\)", "", raw_str)

    def _backup_local(self, acl_ids: Sequence[str], context: BackupContext) -> None:
        """
        Backup access entities from local storage.
        """
        assert (
            context.backup_meta.access_control.backup_format == BackupStorageFormat.TAR
        )

        logging.debug(f"Backupping {len(acl_ids)} access entities from local storage")
        acl_file_names = _get_access_control_files(acl_ids)
        context.backup_layout.upload_access_control_files(
            context.backup_meta.name, acl_file_names
        )

    def _backup_replicated(
        self, acl_list: Sequence[str], context: BackupContext
    ) -> None:
        """
        Backup access entities from replicated storage (ZK/CK).
        """
        _ensure_access_control_path(context)
        logging.debug(
            f"Backupping {len(acl_list)} access entities from replicated storage"
        )
        with context.zk_ctl.zk_client as zk_client:
            for uuid in acl_list:
                uuid_zk_path = _get_access_zk_path(context, f"/uuid/{uuid}")
                data, _ = zk_client.get(uuid_zk_path)
                _file_create(context, f"{uuid}.sql", data.decode())
        _chown_access_control_dir(context)

    def _restore_local(
        self,
        acl_list: Sequence[str],
        context: BackupContext,
        mark_to_rebuild: bool = True,
    ) -> None:
        """
        Restore access entities to local storage.
        """
        _ensure_access_control_path(context)
        logging.debug(f"Restoring {len(acl_list)} access entities to local storage")

        if context.backup_meta.access_control.backup_format == BackupStorageFormat.TAR:
            context.backup_layout.download_access_control(context.backup_meta.name)
        else:
            for name in _get_access_control_files(acl_list):
                context.backup_layout.download_access_control_file(
                    context.backup_meta.name, name
                )

        if mark_to_rebuild:
            self._mark_to_rebuild(context)

    def _restore_replicated(
        self,
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
                file_path = _get_access_file_path(context, f"{uuid}.sql")
                with open(file_path, "r", encoding="utf-8") as file:
                    data = file.read()
                    uuid_zk_path = _get_access_zk_path(context, f"/uuid/{uuid}")
                    _zk_upsert_data(zk_client, uuid_zk_path, data)

                # restore object link
                uuid_zk_path = _get_access_zk_path(context, f"/{obj_char}/{name}")
                _zk_upsert_data(zk_client, uuid_zk_path, uuid)

    def _mark_to_rebuild(self, context: BackupContext) -> None:
        """
        Creates special mark file to rebuild the lists.
        """
        mark_file = _get_access_file_path(context, CH_MARK_FILE)
        with open(mark_file, "a", encoding="utf-8"):
            pass
        _chown_path(context, mark_file)

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


def _get_access_file_path(context: BackupContext, file_name: str) -> str:
    return os.path.join(context.ch_ctl_conf["access_control_path"], file_name)


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


def _file_create(context: BackupContext, file_name: str, file_content: str = "") -> str:
    file_path = _get_access_file_path(context, file_name)
    logging.debug(f'Creating "{file_path}" access entity file')
    with open(file_path, "w", encoding="utf-8") as file:
        file.write(file_content)

    return file_path


def _ensure_access_control_path(context: BackupContext) -> None:
    acl_path = context.ch_ctl_conf["access_control_path"]
    os.makedirs(acl_path, exist_ok=True)
    _chown_path(context, acl_path)


def _chown_access_control_dir(context: BackupContext) -> None:
    ch_config = context.ch_ctl_conf
    chown_dir_contents(
        ch_config["user"], ch_config["group"], ch_config["access_control_path"]
    )


def _chown_path(context: BackupContext, path: str) -> None:
    ch_config = context.ch_ctl_conf
    shutil.chown(path, ch_config["user"], ch_config["group"])
