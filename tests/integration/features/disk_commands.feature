Feature: Disk commands

  Scenario: Disk commands preserve literal paths
    When we execute command on clickhouse01
    """
    cd /code && python3 -m tests.integration.modules.disks
    """
