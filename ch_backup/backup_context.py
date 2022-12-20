"""
Clickhouse backup context
"""
from ch_backup.backup.layout import BackupLayout
from ch_backup.backup.metadata import BackupMetadata
from ch_backup.backup.restore_context import RestoreContext
from ch_backup.clickhouse.control import ClickhouseCTL
from ch_backup.config import Config
from ch_backup.logic.lock_manager import LockManager
from ch_backup.zookeeper.zookeeper import ZookeeperCTL


class BackupContext:
    """
    Class context for clickhouse backup logic
    """

    # pylint: disable=too-many-instance-attributes

    _ch_ctl: ClickhouseCTL
    _backup_layout: BackupLayout
    _zk_ctl: ZookeeperCTL
    _backup_meta: BackupMetadata
    _restore_context: RestoreContext
    _locker: LockManager

    def __init__(self, config: Config) -> None:
        self._config_root = config
        self._lock_conf = config.get('lock')
        self._ch_ctl_conf = config.get('clickhouse')
        self._main_conf = config.get('main')
        self._config = config.get('backup')
        self._zk_config = config.get('zookeeper')

    @property
    def config_root(self) -> Config:
        """
        Getter config_root
        """
        return self._config_root

    @config_root.setter
    def config_root(self, config: Config) -> None:
        self._config = config

    @property
    def ch_ctl_conf(self) -> dict:
        """
        Getter ch_ctl_conf
        """
        return self._ch_ctl_conf

    @ch_ctl_conf.setter
    def ch_ctl_conf(self, ch_ctl_conf: dict) -> None:
        self._ch_ctl_conf = ch_ctl_conf

    @property
    def main_conf(self) -> dict:
        """
        Getter main_conf
        """
        return self._main_conf

    @main_conf.setter
    def main_conf(self, main_conf: dict) -> None:
        self._main_conf = main_conf

    @property
    def ch_ctl(self) -> ClickhouseCTL:
        """
        Getter ch_ctl
        """
        if not hasattr(self, '_ch_ctl'):
            self._ch_ctl = ClickhouseCTL(self._ch_ctl_conf, self._main_conf)
        return self._ch_ctl

    @ch_ctl.setter
    def ch_ctl(self, ch_ctl: ClickhouseCTL) -> None:
        self._ch_ctl = ch_ctl

    @property
    def backup_layout(self) -> BackupLayout:
        """
        Getter backup_layout
        """
        if not hasattr(self, '_backup_layout'):
            self._backup_layout = BackupLayout(self._config)
        return self._backup_layout

    @backup_layout.setter
    def backup_layout(self, backup_layout: BackupLayout) -> None:
        self._backup_layout = backup_layout

    @property
    def config(self) -> dict:
        """
        Getter config
        """
        return self._config

    @config.setter
    def config(self, config: dict) -> None:
        self._config = config

    @property
    def zk_config(self) -> dict:
        """
        Getter zk_config
        """
        return self._zk_config

    @zk_config.setter
    def zk_config(self, zk_config: dict) -> None:
        self._zk_config = zk_config

    @property
    def zk_ctl(self) -> ZookeeperCTL:
        """
        Getter zk_ctl
        """
        if not hasattr(self, '_zk_ctl'):
            self._zk_ctl = ZookeeperCTL(self._zk_config)
        return self._zk_ctl

    @zk_ctl.setter
    def zk_ctl(self, zk_ctl: ZookeeperCTL) -> None:
        self._zk_ctl = zk_ctl

    @property
    def restore_context(self) -> RestoreContext:
        """
        Getter restore_context
        """
        if not hasattr(self, '_restore_context'):
            self._restore_context = RestoreContext(self._config)
        return self._restore_context

    @restore_context.setter
    def restore_context(self, restore_context: RestoreContext) -> None:
        self._restore_context = restore_context

    @property
    def backup_meta(self) -> BackupMetadata:
        """
        Getter backup_meta
        """
        return self._backup_meta

    @backup_meta.setter
    def backup_meta(self, backup_meta: BackupMetadata) -> None:
        """
        Setter backup_meta
        """
        self._backup_meta = backup_meta

    @property
    def lock_conf(self) -> dict:
        """
        Getter lock_conf
        """
        return self._lock_conf

    @lock_conf.setter
    def lock_conf(self, lock_conf: dict) -> None:
        """
        Setter lock_conf
        """
        self._lock_conf = lock_conf

    @property
    def locker(self) -> LockManager:
        """
        Getter locker
        """
        if not hasattr(self, '_locker'):
            self._locker = LockManager(self.lock_conf, self.zk_ctl)
        return self._locker

    @locker.setter
    def locker(self, locker: LockManager) -> None:
        """
        Setter locker
        """
        self._locker = locker
