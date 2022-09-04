#!/usr/bin/env python3
# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.

import pytest
from charms.kafka.v0.kafka_snap import SNAP_CONFIG_PATH
from ops.charm import CharmBase
from ops.testing import Harness

from cluster import ZooKeeperCluster
from config import ZooKeeperConfig
from provider import ZooKeeperProvider
from tls import ZooKeeperTLS

METADATA = """
    name: zookeeper
    peers:
        cluster:
            interface: cluster
        restart:
            interface: rolling_op
    provides:
        zookeeper:
            interface: zookeeper
    requires:
        certificates:
            interface: tls-certifictes
"""
CONFIG = """
options:
  init-limit:
    description: "Amount of time, in ticks, to allow followers to connect and sync to a leader."
    type: int
    default: 5
  sync-limit:
    description: "Amount of time, in ticks, to allow followers to sync with ZooKeeper."
    type: int
    default: 2
  tick-time:
    description: "the length of a single tick, which is the basic time unit used by ZooKeeper, as measured in milliseconds."
    type: int
    default: 2000
"""


class DummyZooKeeperCharm(CharmBase):
    def __init__(self, *args):
        super().__init__(*args)
        self.cluster = ZooKeeperCluster(self)
        self.client_relation = ZooKeeperProvider(self)
        self.tls = ZooKeeperTLS(self)
        self.zookeeper_config = ZooKeeperConfig(self)


@pytest.fixture(scope="function")
def harness():
    harness = Harness(DummyZooKeeperCharm, meta=METADATA, config=CONFIG)
    harness.begin_with_initial_hooks()
    harness._update_config({"init-limit": "5", "sync-limit": "2", "tick-time": "2000"})
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
    harness.add_relation("zookeeper", "application")
    harness.update_relation_data(
        harness.charm.client_relation.client_relations[0].id, "application", {"chroot": "app"}
    )
    harness.update_relation_data(
        harness.charm.client_relation.app_relation.id, "zookeeper", {"relation-2": "password"}
    )

    assert len(harness.charm.zookeeper_config.jaas_users) == 1


def test_multiple_jaas_users_are_added(harness):
    harness.add_relation("zookeeper", "application")
    harness.add_relation("zookeeper", "application2")
    harness.update_relation_data(
        harness.charm.client_relation.client_relations[0].id, "application", {"chroot": "app"}
    )
    harness.update_relation_data(
        harness.charm.client_relation.client_relations[1].id, "application2", {"chroot": "app2"}
    )
    harness.update_relation_data(
        harness.charm.client_relation.app_relation.id,
        "zookeeper",
        {"relation-2": "password", "relation-3": "password"},
    )

    assert len(harness.charm.zookeeper_config.jaas_users) == 2


def test_tls_enabled(harness):
    harness.update_relation_data(harness.charm.tls.cluster.id, "zookeeper", {"tls": "enabled"})
    assert "ssl.clientAuth=none" in harness.charm.zookeeper_config.zookeeper_properties


def test_tls_disabled(harness):
    assert "ssl.clientAuth=none" not in harness.charm.zookeeper_config.zookeeper_properties


def test_tls_upgrading(harness):
    harness.update_relation_data(
        harness.charm.tls.cluster.id, "zookeeper", {"upgrading": "started"}
    )
    assert "portUnification=true" in harness.charm.zookeeper_config.zookeeper_properties

    harness.update_relation_data(harness.charm.tls.cluster.id, "zookeeper", {"upgrading": ""})
    assert "portUnification=true" not in harness.charm.zookeeper_config.zookeeper_properties


def test_tls_ssl_quorum(harness):
    harness.update_relation_data(harness.charm.tls.cluster.id, "zookeeper", {"quorum": "ssl"})
    assert "sslQuorum=true" in harness.charm.zookeeper_config.zookeeper_properties

    harness.update_relation_data(harness.charm.tls.cluster.id, "zookeeper", {"quorum": "non-ssl"})
    assert "sslQuorum=true" not in harness.charm.zookeeper_config.zookeeper_properties
