#!/usr/bin/env python3
# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.

import logging
from pathlib import Path

import pytest
import yaml
from charms.kafka.v0.kafka_snap import SNAP_CONFIG_PATH
from ops.testing import Harness

from charm import ZooKeeperCharm
from config import ZooKeeperConfig
from literals import CHARM_KEY, PEER, REL_NAME

logger = logging.getLogger(__name__)

CONFIG = str(yaml.safe_load(Path("./config.yaml").read_text()))
ACTIONS = str(yaml.safe_load(Path("./actions.yaml").read_text()))
METADATA = str(yaml.safe_load(Path("./metadata.yaml").read_text()))


@pytest.fixture
def harness():
    harness = Harness(ZooKeeperCharm, meta=METADATA, config=CONFIG, actions=ACTIONS)
    harness.add_relation("restart", CHARM_KEY)
    peer_rel_id = harness.add_relation(PEER, CHARM_KEY)
    harness.add_relation_unit(peer_rel_id, f"{CHARM_KEY}/0")
    harness._update_config({"init-limit": "5", "sync-limit": "2", "tick-time": "2000"})
    harness.begin()
    return harness


def test_build_static_properties_removes_necessary_rows():
    properties = [
        "clientPort=2181",
        "authProvider.sasl=org.apache.zookeeper.server.auth.SASLAuthenticationProvider",
        "maxClientCnxns=60",
        "dynamicConfigFile=/var/snap/kafka/common/zookeeper.properties.dynamic.100000041",
    ]

    static = ZooKeeperConfig.build_static_properties(properties=properties)

    assert len(static) == 2
    assert "clientPort" not in "".join(static)
    assert "dynamicConfigFile" not in "".join(static)


def test_kafka_opts_has_jaas(harness):
    opts = ZooKeeperConfig(harness.charm).kafka_opts
    assert f"-Djava.security.auth.login.config={SNAP_CONFIG_PATH}/zookeeper-jaas.cfg" in opts


def test_jaas_users_are_added(harness):
    harness.add_relation(REL_NAME, "application")
    harness.update_relation_data(
        harness.charm.provider.client_relations[0].id, "application", {"chroot": "app"}
    )
    harness.update_relation_data(
        harness.charm.provider.app_relation.id, CHARM_KEY, {"relation-2": "password"}
    )

    assert len(harness.charm.zookeeper_config.jaas_users) == 1


def test_multiple_jaas_users_are_added(harness):
    harness.add_relation(REL_NAME, "application")
    harness.add_relation(REL_NAME, "application2")
    harness.update_relation_data(
        harness.charm.provider.client_relations[0].id, "application", {"chroot": "app"}
    )
    harness.update_relation_data(
        harness.charm.provider.client_relations[1].id, "application2", {"chroot": "app2"}
    )
    harness.update_relation_data(
        harness.charm.provider.app_relation.id,
        CHARM_KEY,
        {"relation-2": "password", "relation-3": "password"},
    )

    assert len(harness.charm.zookeeper_config.jaas_users) == 2


def test_tls_enabled(harness):
    harness.update_relation_data(harness.charm.tls.cluster.id, CHARM_KEY, {"tls": "enabled"})
    assert "ssl.clientAuth=none" in harness.charm.zookeeper_config.zookeeper_properties


def test_tls_disabled(harness):
    assert "ssl.clientAuth=none" not in harness.charm.zookeeper_config.zookeeper_properties


def test_tls_upgrading(harness):
    harness.update_relation_data(harness.charm.tls.cluster.id, CHARM_KEY, {"upgrading": "started"})
    assert "portUnification=true" in harness.charm.zookeeper_config.zookeeper_properties

    harness.update_relation_data(harness.charm.tls.cluster.id, CHARM_KEY, {"upgrading": ""})
    assert "portUnification=true" not in harness.charm.zookeeper_config.zookeeper_properties


def test_tls_ssl_quorum(harness):
    harness.update_relation_data(harness.charm.tls.cluster.id, CHARM_KEY, {"quorum": "ssl"})
    assert "sslQuorum=true" in harness.charm.zookeeper_config.zookeeper_properties

    harness.update_relation_data(harness.charm.tls.cluster.id, CHARM_KEY, {"quorum": "non-ssl"})
    assert "sslQuorum=true" not in harness.charm.zookeeper_config.zookeeper_properties
