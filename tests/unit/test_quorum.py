#!/usr/bin/env python3
# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.

import logging
from pathlib import Path
from unittest.mock import DEFAULT, MagicMock, patch

import pytest
import yaml
from ops.testing import Harness

from charm import ZooKeeperCharm
from literals import CHARM_KEY, PEER, REL_NAME

logger = logging.getLogger(__name__)

CONFIG = str(yaml.safe_load(Path("./config.yaml").read_text()))
ACTIONS = str(yaml.safe_load(Path("./actions.yaml").read_text()))
METADATA = str(yaml.safe_load(Path("./metadata.yaml").read_text()))


@pytest.fixture
def harness():
    harness = Harness(ZooKeeperCharm, meta=METADATA, config=CONFIG, actions=ACTIONS)
    upgrade_rel_id = harness.add_relation("upgrade", CHARM_KEY)
    harness.add_relation("restart", CHARM_KEY)
    harness.update_relation_data(upgrade_rel_id, f"{CHARM_KEY}/0", {"state": "idle"})
    harness.add_relation(PEER, CHARM_KEY)
    harness._update_config({"init-limit": 5, "sync-limit": 2, "tick-time": 2000})
    harness.begin()
    return harness


def test_get_updated_servers(harness):
    with harness.hooks_disabled():
        harness.add_relation_unit(harness.charm.state.peer_relation.id, f"{CHARM_KEY}/0")
        harness.update_relation_data(
            harness.charm.state.peer_relation.id,
            f"{CHARM_KEY}",
            {"0": "added", "3": "removed", "4": "added"},
        )

    # at this stage, we have only 1 peer unit-0, and have 'lost' unit-4, which was 'added' but not yet removed
    # we add unit-1 below, and expect the leader to set unit-1 to 'added', and unit-4 to 'removed'

    added_servers = [
        "server.2=gandalf.the.grey",
    ]
    updated_servers = harness.charm.quorum_manager._get_updated_servers(add=added_servers)

    assert updated_servers == {"1": "added", "4": "removed"}


def test_is_child_of(harness):
    chroot = "/gandalf/the/white"
    chroots = {"/gandalf", "/saruman"}

    assert harness.charm.quorum_manager._is_child_of(path=chroot, chroots=chroots)


def test_is_child_of_not(harness):
    chroot = "/the/one/ring"
    chroots = {"/gandalf", "/saruman"}

    assert not harness.charm.quorum_manager._is_child_of(path=chroot, chroots=chroots)


def test_update_acls_does_not_add_empty_chroot(harness):
    with harness.hooks_disabled():
        harness.add_relation(REL_NAME, "application")

    with patch.multiple(
        "charms.zookeeper.v0.client.ZooKeeperManager",
        get_leader=DEFAULT,
        leader_znodes=MagicMock(return_value={"/"}),
        create_znode_leader=DEFAULT,
        set_acls_znode_leader=DEFAULT,
        delete_znode_leader=DEFAULT,
    ) as patched_manager:
        harness.charm.quorum_manager.update_acls()

        patched_manager["create_znode_leader"].assert_not_called()


def test_update_acls_correctly_handles_relation_chroots(harness):
    dummy_leader_znodes = {
        "/fellowship",
        "/fellowship/men",
        "/fellowship/dwarves",
        "/fellowship/men/aragorn",
        "/fellowship/men/boromir",
        "/fellowship/elves/legolas",
        "/fellowship/dwarves/gimli",
        "/fellowship/men/aragorn/anduril",
    }

    with harness.hooks_disabled():
        app_id = harness.add_relation(REL_NAME, "application")
        harness.update_relation_data(app_id, "application", {"database": "/rohan"})
        harness.set_leader(True)

    with patch.multiple(
        "charms.zookeeper.v0.client.ZooKeeperManager",
        get_leader=DEFAULT,
        leader_znodes=MagicMock(return_value=dummy_leader_znodes),
        create_znode_leader=DEFAULT,
        set_acls_znode_leader=DEFAULT,
        delete_znode_leader=DEFAULT,
    ) as patched_manager:
        harness.charm.quorum_manager.update_acls()

        for _, kwargs in patched_manager["create_znode_leader"].call_args_list:
            assert "/rohan" in kwargs["path"]

        for _, kwargs in patched_manager["set_acls_znode_leader"].call_args_list:
            assert "/rohan" in kwargs["path"]

        removed_men = False
        for counter, call in enumerate(patched_manager["delete_znode_leader"].call_args_list):
            _, kwargs = call

            if "/fellowship/men" in kwargs["path"]:
                assert not removed_men, "Parent zNode removed before all it's children"

            if kwargs["path"] == "/fellowship/men":
                removed_men = True

        # ensure last node to go is the parent
        assert (
            patched_manager["delete_znode_leader"].call_args_list[-1][1]["path"] == "/fellowship"
        )
