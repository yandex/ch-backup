"""
Start supervisord
"""

import subprocess

if __name__ == "__main__":
    subprocess.Popen(["supervisord", "-c", "/etc/supervisor/supervisord.conf"]).wait()
