"""
Create zookeeper root node for ClickHouse before start
"""
import os
import subprocess

from kazoo.client import KazooClient

if __name__ == "__main__":
    if os.environ.get("ZOOKEEPER_ENABLED"):
        client = KazooClient("{{conf.zk.uri}}:{{conf.zk.port}}")
        client.start()
        client.ensure_path("/{{ instance_name }}")
        client.stop()

    subprocess.Popen(["supervisord", "-c", "/etc/supervisor/supervisord.conf"]).wait()
