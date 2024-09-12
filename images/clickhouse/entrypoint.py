"""
Create zookeeper root node for ClickHouse before start
"""

import subprocess

from kazoo.client import KazooClient

if __name__ == "__main__":
    client = KazooClient("{{ conf.zk.uri }}:{{ conf.zk.port }}")
    client.start()

    # In our test by default ch nodes have different root paths in the zookeeper(`/clickhouse01/` and `/clickhouse02/`).
    # So they are not connected to each other.
    # If you need the same path for nodes, use `step_enable_shared_zookeeper_for_clickhouse` step to override configs. 
    client.ensure_path("/{{ instance_name }}")
    client.ensure_path("/{{ conf.zk.shared_node }}")

    client.stop()

    subprocess.Popen(["supervisord", "-c", "/etc/supervisor/supervisord.conf"]).wait()
