"""
Clickhouse backup logic for Cloud Storage
"""

from typing import Any, Dict, Optional

from ch_backup import logging
from ch_backup.storage.engine.s3 import S3StorageEngine


# pylint: disable=too-many-locals,too-many-nested-blocks,too-many-branches
def fix_s3_oplog(
    cloud_storage_config: Dict[str, Any],
    source_cluster_id: str,
    shard: str,
    cloud_storage_source_bucket: Optional[str] = None,
    cloud_storage_source_path: Optional[str] = None,
    dryrun: bool = False,
) -> None:
    """
    Fix S3 operations log.
    """
    if cloud_storage_source_bucket is None:
        cloud_storage_source_bucket = _get_bucket_name(cluster_id=source_cluster_id)
    if cloud_storage_source_path is None:
        cloud_storage_source_path = _get_shard_path(
            cluster_id=source_cluster_id, shard_id=shard
        )

    engine = S3StorageEngine(cloud_storage_config)
    client = engine.get_client()
    paginator = client.get_paginator("list_objects")
    list_object_kwargs = dict(
        Bucket=cloud_storage_source_bucket,
        Prefix=_get_operations_prefix(shard_path=cloud_storage_source_path),
    )

    delete_list: Dict[str, int] = {}

    collision_counter = 0

    for result in paginator.paginate(**list_object_kwargs):
        if result.get("Contents") is None:
            continue

        for file_data in result.get("Contents"):
            key = file_data.get("Key")

            if not key.endswith("-rename"):
                continue

            head = client.head_object(Bucket=cloud_storage_source_bucket, Key=key)
            metadata = head.get("Metadata")

            to_path = ""
            if "To_path" in metadata:
                to_path = metadata.get("To_path")

            if "delete_tmp_" not in to_path:
                continue

            if to_path not in delete_list:
                delete_list[to_path] = 1
                continue

            collision_counter += 1

            new_path = f"{to_path}_collision_{delete_list[to_path]}"
            delete_list[to_path] += 1

            logging.info("Collision for %s, new path %s", to_path, new_path)

            if dryrun:
                continue

            metadata["To_path"] = new_path
            metadata["To_path_original"] = to_path
            client.copy_object(
                Bucket=cloud_storage_source_bucket,
                Key=key,
                CopySource={
                    "Bucket": cloud_storage_source_bucket,
                    "Key": key,
                },
                Metadata=metadata,
                MetadataDirective="REPLACE",
            )

    logging.info(
        'Fix S3 OpLog: bucket "%s", path "%s"',
        cloud_storage_source_bucket,
        cloud_storage_source_path,
    )
    logging.info(
        f'Fix S3 OpLog: found {"and fixed " if not dryrun else ""}%d collisions',
        collision_counter,
    )


def _get_bucket_name(cluster_id: str) -> str:
    return f"cloud-storage-{cluster_id}"


def _get_shard_path(cluster_id: str, shard_id: str) -> str:
    return f"cloud_storage/{cluster_id}/{shard_id}"


def _get_operations_prefix(shard_path: str) -> str:
    return f"{shard_path}/operations/r"
