#!/usr/bin/env python3
# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

import logging
from pathlib import Path
from unittest.mock import PropertyMock

import pytest
import yaml
from charm import ZooKeeperCharm
from charms.data_platform_libs.v0.upgrade import ClusterNotReadyError
from charms.zookeeper.v0.client import ZooKeeperManager
from cluster import ZooKeeperCluster
from kazoo.client import KazooClient
from literals import CHARM_KEY
from ops.testing import Harness

logger = logging.getLogger(__name__)


@pytest.fixture(autouse=True)
def patched_client(mocker):
    mocker.patch.object(ZooKeeperManager, "get_leader")
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
