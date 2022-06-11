# TODO: Add get password action
# TODO: Add logging everywhere
# TODO: Add run it through and write tests as you go

#!/usr/bin/env python3
# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.

"""Charmed Machine Operator for Apache ZooKeeper."""

import logging
from typing import Tuple
from ops.framework import EventBase

from ops.model import ActiveStatus, BlockedStatus, MaintenanceStatus, WaitingStatus

from charms.kafka.v0.kafka_snap import KafkaSnap, KafkaSnapError
from charms.zookeeper.v0.client import MemberNotReadyError, MembersSyncingError
from charms.zookeeper.v0.cluster import ZooKeeperCluster
from ops.charm import CharmBase
from ops.main import main

logger = logging.getLogger(__name__)


CHARM_KEY = "zookeeper"
CLUSTER_KEY = "cluster"


class ZooKeeperCharm(CharmBase):
    """Charmed Operator for ZooKeeper."""

    def __init__(self, *args):
        super().__init__(*args)
        self.name = CHARM_KEY
        self.snap = KafkaSnap()
        self.cluster = ZooKeeperCluster(self.model.get_relation(CLUSTER_KEY))

        self.framework.observe(getattr(self.on, "install"), self._on_install)
        self.framework.observe(
            getattr(self.on, "leader_elected"), self._on_cluster_relation_updated
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

    @property
    def myid(self):
        return  self.cluster.get_server_id(self.unit)

    def _on_install(self, _) -> None:
        """Handler for on_install event."""
        self.unit.status = MaintenanceStatus("starting unit install")

        self.snap.install_kafka_snap()
        self.snap.write_default_properties(
            properties=self.config["zookeeper-properties"], property_label="zookeeper"
        )

        self.snap.write_zookeeper_myid(myid=self.myid)
        self.cluster.relation.data[self.unit].update({"myid": self.myid})
        self.cluster.relation.data[self.unit].update({"state": "ready"})
        # TODO: Add server string to unit data for easier retrieval
        # TODO: Write get_cluster_members to dynamic_config

        self.unit.status = WaitingStatus("waiting for other units to be ready")

        self.snap.start_snap_service(snap_service=CHARM_KEY)
        self.cluster.relation.data[self.unit].update({"state": "started"})
        self.unit.status = ActiveStatus()

    def _on_cluster_relation_updated(self, event) -> None:
        if not self.unit.is_leader():
            return
        # TODO: Set 'started' flag to app data from leader
        self.unit.status = MaintenanceStatus("starting cluster member update")

        if self.cluster.update_cluster():
            self.unit.status = ActiveStatus()
        else:
            event.defer()

    def _on_get_properties_action(self, event) -> None:
        """Handler for users to copy currently active config for passing to `juju config`."""
        config_map = self.snap.get_properties(property_label="zookeeper")
        msg = "\n".join([f"{k}={v}" for k, v in config_map.items()])
        event.set_results({"properties": msg})

    def _on_get_snap_apps_action(self, event) -> None:
        """Handler for users to retrieve the list of available Kafka snap commands."""
        msg = self.snap.get_kafka_apps()
        event.set_results({"apps": msg})


if __name__ == "__main__":
    main(ZooKeeperCharm)
