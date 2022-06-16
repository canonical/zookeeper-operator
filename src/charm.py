#!/usr/bin/env python3
# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.

"""Charmed Machine Operator for Apache ZooKeeper."""

import logging

from charms.kafka.v0.kafka_snap import KafkaSnap
from charms.zookeeper.v0.cluster import ZooKeeperCluster
from ops.charm import ActionEvent, CharmBase
from ops.framework import EventBase
from ops.main import main
from ops.model import ActiveStatus
import re

logger = logging.getLogger(__name__)


CHARM_KEY = "zookeeper"
PEER = "cluster"


class ZooKeeperCharm(CharmBase):
    """Charmed Operator for ZooKeeper."""

    def __init__(self, *args):
        super().__init__(*args)
        self.name = CHARM_KEY
        self.snap = KafkaSnap()
        self.cluster = ZooKeeperCluster(self)

        self.framework.observe(getattr(self.on, "install"), self._on_install)
        self.framework.observe(getattr(self.on, "start"), self._on_start)
        self.framework.observe(
            getattr(self.on, "leader_elected"), self._on_cluster_relation_updated
        )
        self.framework.observe(
            getattr(self.on, "cluster_relation_changed"), self._on_cluster_relation_updated
        )
        self.framework.observe(
            getattr(self.on, "cluster_relation_joined"), self._on_cluster_relation_updated
        )
        self.framework.observe(
            getattr(self.on, "cluster_relation_departed"), self._on_cluster_relation_updated
        )
        # TODO: Get Properties Action
        self.framework.observe(
            getattr(self.on, "get_snap_apps_action"), self._on_get_snap_apps_action
        )

    def _on_install(self, _) -> None:
        """Handler for the on_install event."""
        self.unit.status = self.snap.status

        self.snap.install_kafka_snap()
        self.snap.write_properties(
            properties=self.config["zookeeper-properties"], property_label="zookeeper", mode="w"
        )
        self.snap.write_zookeeper_myid(myid=self.cluster.get_unit_id(self.unit) + 1)
        self.unit.status = self.snap.status

    def _on_start(self, event: EventBase) -> None:
        """Handler for the on_start event."""

        is_next_server, servers, unit_config = self.cluster.ready_to_start(self.unit)

        if not is_next_server:
            self.unit.status = self.cluster.status
            event.defer()
            return

        logger.info(f"{is_next_server=}")
        logger.info(f"{servers=}")
        self.snap.write_properties(properties=servers, property_label="zookeeper", mode="a")
        self.snap.start_snap_service(snap_service=CHARM_KEY)

        self.unit.status = self.snap.status

        self.cluster.relation.data[self.unit].update(unit_config)

    def _on_cluster_relation_updated(self, event: EventBase) -> None:
        """Handler for events triggered by changing units."""
        if not self.unit.is_leader():
            return

        updated_servers = self.cluster.update_cluster()
        logger.info("{updated_servers=}")
        self.unit.status = self.cluster.status

        if self.cluster.status == ActiveStatus():
            self.cluster.relation.data[self.model.app].update(updated_servers)
            self.cluster.relation.data[self.model.app].update({"init": "completed"})
        else:
            event.defer()
            return

    def _on_get_properties_action(self, event: ActionEvent) -> None:
        """Handler for users to copy currently active config for passing to `juju config`."""
        config_map = self.snap.get_properties(property_label="zookeeper")
        msg = "\n".join([f"{k}={v}" for k, v in config_map.items()])
        event.set_results({"properties": msg})

    def _on_get_snap_apps_action(self, event: ActionEvent) -> None:
        """Handler for users to retrieve the list of available Kafka snap commands."""
        msg = self.snap.get_kafka_apps()
        event.set_results({"apps": msg})


if __name__ == "__main__":
    main(ZooKeeperCharm)
