#!/usr/bin/env python3
# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

import logging
from pathlib import Path
from unittest.mock import PropertyMock

import pytest
import yaml
from charms.data_platform_libs.v0.upgrade import ClusterNotReadyError, DependencyModel
from charms.zookeeper.v0.client import ZooKeeperManager
from kazoo.client import KazooClient
from ops.testing import Harness

from charm import ZooKeeperCharm
from cluster import ZooKeeperCluster
from config import ZooKeeperConfig
from literals import CHARM_KEY, DEPENDENCIES
from snap import ZooKeeperSnap
from upgrade import ZooKeeperDependencyModel, ZooKeeperUpgrade

logger = logging.getLogger(__name__)


@pytest.fixture(autouse=True)
def patched_client(mocker):
    mocker.patch.object(ZooKeeperManager, "get_leader", return_value="000.000.000")
    mocker.patch.object(KazooClient, "start")


CONFIG = str(yaml.safe_load(Path("./config.yaml").read_text()))
ACTIONS = str(yaml.safe_load(Path("./actions.yaml").read_text()))
METADATA = str(yaml.safe_load(Path("./metadata.yaml").read_text()))


@pytest.fixture
def harness():
    harness = Harness(ZooKeeperCharm, meta=METADATA, config=CONFIG, actions=ACTIONS)
    harness.add_relation("cluster", CHARM_KEY)
    harness.add_relation("restart", CHARM_KEY)
    harness.add_relation("upgrade", CHARM_KEY)
    harness._update_config({"init-limit": 5, "sync-limit": 2, "tick-time": 2000})
    harness.begin()
    with harness.hooks_disabled():
        harness.add_relation_unit(harness.charm.peer_relation.id, f"{CHARM_KEY}/0")
        harness.update_relation_data(
            harness.charm.peer_relation.id, f"{CHARM_KEY}/0", {"hostname": "000.000.000"}
        )

    return harness


def test_pre_upgrade_check_raises_not_all_members_broadcasting(harness, mocker):
    mocker.patch.object(
        ZooKeeperManager, "members_broadcasting", new_callable=PropertyMock, return_value=False
    )

    with pytest.raises(ClusterNotReadyError):
        harness.charm.upgrade.pre_upgrade_check()


def test_pre_upgrade_check_raises_not_all_units_members(harness, mocker):
    mocker.patch.object(
        ZooKeeperManager, "members_broadcasting", new_callable=PropertyMock, return_value=True
    )
    mocker.patch.object(
        ZooKeeperManager, "server_members", new_callable=PropertyMock, return_value=[0, 1]
    )

    with pytest.raises(ClusterNotReadyError):
        harness.charm.upgrade.pre_upgrade_check()


def test_pre_upgrade_check_raises_members_syncing(harness, mocker):
    mocker.patch.object(
        ZooKeeperManager, "members_broadcasting", new_callable=PropertyMock, return_value=True
    )
    mocker.patch.object(
        ZooKeeperManager, "server_members", new_callable=PropertyMock, return_value=[0]
    )
    mocker.patch.object(
        ZooKeeperManager, "members_syncing", new_callable=PropertyMock, return_value=True
    )

    with pytest.raises(ClusterNotReadyError):
        harness.charm.upgrade.pre_upgrade_check()


def test_pre_upgrade_check_raises_not_stable(harness, mocker):
    mocker.patch.object(
        ZooKeeperManager, "members_broadcasting", new_callable=PropertyMock, return_value=True
    )
    mocker.patch.object(
        ZooKeeperManager, "server_members", new_callable=PropertyMock, return_value=[0]
    )
    mocker.patch.object(
        ZooKeeperManager, "members_syncing", new_callable=PropertyMock, return_value=False
    )
    mocker.patch.object(ZooKeeperCluster, "stable", new_callable=PropertyMock, return_value=False)

    with pytest.raises(ClusterNotReadyError):
        harness.charm.upgrade.pre_upgrade_check()


def test_pre_upgrade_check_raises_leader_not_found(harness, mocker):
    mocker.patch.object(
        ZooKeeperManager, "members_broadcasting", new_callable=PropertyMock, return_value=True
    )
    mocker.patch.object(
        ZooKeeperManager, "server_members", new_callable=PropertyMock, return_value=[0]
    )
    mocker.patch.object(
        ZooKeeperManager, "members_syncing", new_callable=PropertyMock, return_value=False
    )
    mocker.patch.object(ZooKeeperCluster, "stable", new_callable=PropertyMock, return_value=True)

    # removes get_leader patch
    mocker.stopall()

    with pytest.raises(ClusterNotReadyError):
        harness.charm.upgrade.pre_upgrade_check()


def test_pre_upgrade_check_succeeds(harness, mocker):
    mocker.patch.object(
        ZooKeeperManager, "members_broadcasting", new_callable=PropertyMock, return_value=True
    )
    mocker.patch.object(
        ZooKeeperManager, "server_members", new_callable=PropertyMock, return_value=[0]
    )
    mocker.patch.object(
        ZooKeeperManager, "members_syncing", new_callable=PropertyMock, return_value=False
    )
    mocker.patch.object(ZooKeeperCluster, "stable", new_callable=PropertyMock, return_value=True)

    harness.charm.upgrade.pre_upgrade_check()


def test_build_upgrade_stack(harness):
    with harness.hooks_disabled():
        harness.add_relation_unit(harness.charm.peer_relation.id, f"{CHARM_KEY}/1")
        harness.update_relation_data(
            harness.charm.peer_relation.id, f"{CHARM_KEY}/1", {"hostname": "111.111.111"}
        )
        harness.add_relation_unit(harness.charm.peer_relation.id, f"{CHARM_KEY}/2")
        harness.update_relation_data(
            harness.charm.peer_relation.id, f"{CHARM_KEY}/2", {"hostname": "222.222.222"}
        )
        harness.add_relation_unit(harness.charm.peer_relation.id, f"{CHARM_KEY}/3")
        harness.update_relation_data(
            harness.charm.peer_relation.id, f"{CHARM_KEY}/3", {"hostname": "333.333.333"}
        )

    stack = harness.charm.upgrade.build_upgrade_stack()

    assert stack[0] == 0
    assert len(stack) == 4


def test_zookeeper_dependency_model():
    assert sorted(ZooKeeperDependencyModel.__fields__.keys()) == sorted(DEPENDENCIES.keys())

    for value in DEPENDENCIES.values():
        assert DependencyModel(**value)


def test_upgrade_granted_sets_failed_if_failed_snap(harness, mocker):
    mocker.patch.object(ZooKeeperSnap, "stop_snap_service")
    mocker.patch.object(ZooKeeperSnap, "restart_snap_service")
    mocker.patch.object(ZooKeeperSnap, "install", return_value=False)
    mocker.patch.object(ZooKeeperUpgrade, "pre_upgrade_check")
    mocker.patch.object(ZooKeeperUpgrade, "set_unit_completed")
    mocker.patch.object(ZooKeeperUpgrade, "set_unit_failed")

    mock_event = mocker.MagicMock()

    harness.charm.upgrade._on_upgrade_granted(mock_event)

    ZooKeeperSnap.stop_snap_service.assert_called_once()
    ZooKeeperSnap.install.assert_called_once()
    ZooKeeperSnap.restart_snap_service.assert_not_called()
    ZooKeeperUpgrade.set_unit_completed.assert_not_called()
    ZooKeeperUpgrade.set_unit_failed.assert_called_once()


def test_upgrade_granted_sets_failed_if_failed_upgrade_check(harness, mocker):
    mocker.patch.object(ZooKeeperSnap, "stop_snap_service")
    mocker.patch.object(ZooKeeperSnap, "restart_snap_service")
    mocker.patch.object(ZooKeeperSnap, "install", return_value=True)
    mocker.patch.object(ZooKeeperUpgrade, "set_unit_completed")
    mocker.patch.object(ZooKeeperUpgrade, "set_unit_failed")
    mocker.patch.object(ZooKeeperConfig, "set_server_jvmflags")

    mock_event = mocker.MagicMock()

    harness.charm.upgrade._on_upgrade_granted(mock_event)

    ZooKeeperSnap.stop_snap_service.assert_called_once()
    ZooKeeperSnap.install.assert_called_once()
    ZooKeeperUpgrade.set_unit_completed.assert_not_called()
    ZooKeeperUpgrade.set_unit_failed.assert_called_once()


def test_upgrade_granted_succeeds(harness, mocker):
    mocker.patch.object(ZooKeeperSnap, "stop_snap_service")
    mocker.patch.object(ZooKeeperSnap, "restart_snap_service")
    mocker.patch.object(ZooKeeperSnap, "install")
    mocker.patch.object(ZooKeeperUpgrade, "pre_upgrade_check")
    mocker.patch.object(ZooKeeperUpgrade, "set_unit_completed")
    mocker.patch.object(ZooKeeperUpgrade, "set_unit_failed")
    mocker.patch.object(ZooKeeperConfig, "set_server_jvmflags")

    mock_event = mocker.MagicMock()

    harness.charm.upgrade._on_upgrade_granted(mock_event)

    ZooKeeperSnap.stop_snap_service.assert_called_once()
    ZooKeeperSnap.install.assert_called_once()
    ZooKeeperSnap.restart_snap_service.assert_called_once()
    ZooKeeperUpgrade.set_unit_completed.assert_called_once()
    ZooKeeperUpgrade.set_unit_failed.assert_not_called()


def test_upgrade_granted_recurses_upgrade_changed_on_leader(harness, mocker):
    mocker.patch.object(ZooKeeperSnap, "stop_snap_service")
    mocker.patch.object(ZooKeeperSnap, "restart_snap_service")
    mocker.patch.object(ZooKeeperSnap, "install")
    mocker.patch.object(ZooKeeperUpgrade, "pre_upgrade_check")
    mocker.patch.object(ZooKeeperUpgrade, "on_upgrade_changed")
    mocker.patch.object(ZooKeeperConfig, "set_server_jvmflags")

    mock_event = mocker.MagicMock()

    harness.charm.upgrade._on_upgrade_granted(mock_event)

    ZooKeeperUpgrade.on_upgrade_changed.assert_not_called()

    with harness.hooks_disabled():
        harness.set_leader(True)

    harness.charm.upgrade._on_upgrade_granted(mock_event)

    ZooKeeperUpgrade.on_upgrade_changed.assert_called_once()
