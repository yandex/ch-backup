"""Coordination of operations touching more than one storage."""

from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import pytest
from kazoo.exceptions import LockTimeout

from ch_backup.ch_backup import ClickhouseBackup
from ch_backup.config import Config
from ch_backup.logic.lock_manager import LockManager, storage_locks


def _config(path="/backup-lock", hosts=None):
    config = Config("/dev/null")
    config.merge(
        {
            "lock": {"zk_flock": True, "zk_flock_path": path, "exitcode": 1},
            "zookeeper": {"hosts": hosts or ["zk1:2181", "zk2:2181"]},
        }
    )
    return config


@pytest.mark.parametrize("reverse", [False, True])
def test_distinct_locks_have_stable_order_and_release_on_error(reverse):
    configs = [_config("/b"), _config("/a")]
    if reverse:
        configs.reverse()
    events = []

    @contextmanager
    def acquire(lock_conf, _zk_ctl):
        path = lock_conf["zk_flock_path"]
        events.append(("enter", path))
        try:
            yield
        finally:
            events.append(("exit", path))

    with patch("ch_backup.logic.lock_manager.ZookeeperCTL"):
        with patch("ch_backup.logic.lock_manager.LockManager") as manager:
            manager.side_effect = lambda conf, ctl: lambda **_: acquire(conf, ctl)
            with pytest.raises(RuntimeError, match="copy failed"):
                with storage_locks(configs, "COPY"):
                    events.append(("copy", ""))
                    raise RuntimeError("copy failed")
    assert events == [
        ("enter", "/a"),
        ("enter", "/b"),
        ("copy", ""),
        ("exit", "/b"),
        ("exit", "/a"),
    ]


def test_shared_zk_lock_is_acquired_once_even_with_distinct_file_locks():
    source = _config(hosts=["zk2:2181", "zk1:2181"])
    target = _config(hosts=["zk1", "zk2"])
    source["lock"].update(flock=True, flock_path="/tmp/source.lock")
    target["lock"].update(flock=True, flock_path="/tmp/target.lock")
    with patch("ch_backup.logic.lock_manager.ZookeeperCTL"):
        with patch("ch_backup.logic.lock_manager.LockManager") as manager:
            with storage_locks([source, target], "COPY"):
                assert manager.call_count == 3
                lock_configs = [call.args[0] for call in manager.call_args_list]
                assert sum(bool(c["zk_flock"]) for c in lock_configs) == 1
                assert sum(bool(c["flock"]) for c in lock_configs) == 2


def test_same_path_on_different_ensembles_is_not_deduplicated():
    with patch("ch_backup.logic.lock_manager.ZookeeperCTL"):
        with patch("ch_backup.logic.lock_manager.LockManager") as manager:
            with storage_locks(
                [_config(hosts=["zk1"]), _config(hosts=["zk2"])], "COPY"
            ):
                assert manager.call_count == 2


@pytest.mark.parametrize("target_path", ["/backup-lock/", "//backup-lock"])
def test_equivalent_lock_paths_and_chroots_are_acquired_once(target_path):
    source = _config("/root/backup-lock")
    target = _config(target_path, hosts="zk2,zk1/root")
    with patch("ch_backup.logic.lock_manager.ZookeeperCTL"):
        with patch("ch_backup.logic.lock_manager.LockManager") as manager:
            with storage_locks([source, target], "COPY"):
                assert manager.call_count == 1


def test_lock_timeout_releases_previously_acquired_locks_and_clients():
    clients = [MagicMock(), MagicMock()]
    locks = [client.__enter__.return_value.Lock.return_value for client in clients]
    locks[1].acquire.side_effect = LockTimeout()
    locks[1].contenders.return_value = []
    with patch("ch_backup.logic.lock_manager.ZookeeperCTL") as ctl:
        ctl.side_effect = [MagicMock(zk_client=client) for client in clients]
        with pytest.raises(SystemExit) as error:
            with storage_locks([_config("/a"), _config("/b")], "COPY"):
                pytest.fail("Copy must not run after a lock timeout")
    assert error.value.code == 1
    locks[0].release.assert_called_once()
    locks[1].release.assert_not_called()
    for client in clients:
        client.__exit__.assert_called_once()


def test_disabled_locks_need_no_zookeeper():
    with patch("ch_backup.logic.lock_manager.ZookeeperCTL") as ctl:
        with patch("ch_backup.logic.lock_manager.LockManager") as manager:
            with storage_locks([Config("/dev/null")], "COPY"):
                pass
    ctl.assert_not_called()
    manager.assert_not_called()


def test_missing_zookeeper_is_rejected_before_acquiring_locks():
    config = _config()
    config["zookeeper"]["hosts"] = []
    with patch("ch_backup.logic.lock_manager.LockManager") as manager:
        with pytest.raises(RuntimeError, match="zookeeper is not configured"):
            with storage_locks([config], "COPY"):
                pytest.fail("Missing ZooKeeper must prevent copying")
    manager.assert_not_called()


def test_file_lock_is_closed_if_zookeeper_acquisition_fails():
    config = _config()
    config["lock"]["flock"] = True
    manager = LockManager(config["lock"], None)
    lock_file = MagicMock()
    with patch("ch_backup.logic.lock_manager.os.path.exists", return_value=True):
        with patch("ch_backup.logic.lock_manager.open", return_value=lock_file):
            with patch("ch_backup.logic.lock_manager.flock"):
                with pytest.raises(RuntimeError, match="zookeeper is not configured"):
                    with manager(operation="COPY"):
                        pytest.fail("Missing ZooKeeper must prevent copying")
    lock_file.close.assert_called_once()


@pytest.mark.parametrize("failed", [False, True])
def test_copy_holds_source_and_target_locks_until_copier_returns(failed):
    source, target = _config("/source"), _config("/target")
    backup = ClickhouseBackup(source)
    backup.__dict__["_context"] = MagicMock()
    events = []

    @contextmanager
    def acquire(configs, operation):
        assert configs == [source, target]
        assert operation == "COPY"
        events.append("enter")
        try:
            yield
        finally:
            events.append("exit")

    def do_copy(names):
        assert names == ["b1"]
        assert events == ["enter"]
        events.append("copy")
        if failed:
            raise RuntimeError("copy failed")
        return ["b1"], []

    with patch("ch_backup.ch_backup.storage_locks", side_effect=acquire):
        with patch("ch_backup.ch_backup.BackupLayout"):
            with patch("ch_backup.ch_backup.BackupCopier") as copier:
                copier.return_value.copy.side_effect = do_copy
                if failed:
                    with pytest.raises(RuntimeError, match="copy failed"):
                        backup.copy(target, ["b1"])
                else:
                    assert backup.copy(target, ["b1"]) == (["b1"], [])
    assert events == ["enter", "copy", "exit"]
