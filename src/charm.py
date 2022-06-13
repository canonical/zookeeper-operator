#!/usr/bin/env python3
# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.
# TODO: is_next_server not working
# TODO: server0 participant, server 1+ observer, leader does not matter
# TODO: Add get password action
# TODO: Add logging everywhere
# TODO: Add run it through and write tests as you go


"""Charmed Machine Operator for Apache ZooKeeper."""

import logging
from ops.framework import EventBase

from ops.model import ActiveStatus, MaintenanceStatus, WaitingStatus

from charms.kafka.v0.kafka_snap import KafkaSnap
from charms.zookeeper.v0.cluster import ZooKeeperCluster, ZooKeeperClusterError
from ops.charm import CharmBase
from ops.main import main

logger = logging.getLogger(__name__)


CHARM_KEY = "zookeeper"


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
        return self.cluster.get_server_id(self.unit)

    def _on_install(self, _) -> None:
        """Handler for on_install event."""
        self.unit.status = MaintenanceStatus("starting unit install")

        self.snap.install_kafka_snap()
        self.snap.write_default_properties(
            properties=self.config["zookeeper-properties"], property_label="zookeeper", mode="w"
        )
        self.snap.write_zookeeper_myid(myid=self.myid)
        self.unit.status = WaitingStatus("waiting for other units to be ready")

    def _on_leader_elected(self, _):
        if not self.unit.is_leader():
            return
        if self.cluster.relation.data[self.model.app].get("init", None):
            return
        
          
        

    def _on_start(self, event: EventBase) -> None:
        self.cluster.relation.data[self.unit].update({"myid": str(self.myid)})
        self.cluster.relation.data[self.unit].update({"state": "ready"})
        self.cluster.relation.data[self.unit].update(
            {
                "server": self.cluster.build_server_string(
                    unit=self.unit, role="participant" if self.myid != 1 else "observer"
                )
            }
        )
        logger.debug(self.cluster.relation.data[self.unit])
        is_next_server, servers = self.cluster.is_next_server(self.unit)
        if is_next_server:
            self.snap.write_default_properties(
                properties=servers, property_label="zookeeper", mode="a"
            )
            self.snap.start_snap_service(snap_service=CHARM_KEY)
            self.cluster.relation.data[self.unit].update({"state": "started"})
            self.unit.status = ActiveStatus()
        else:
            event.defer()

    def _on_cluster_relation_updated(self, event) -> None:
        if not self.unit.is_leader():
            return
        self.unit.status = MaintenanceStatus("starting cluster member update")
        try:
            if self.cluster.update_cluster():
                self.unit.status = ActiveStatus()
            else:
                logger.debug("EVENT DEFFERRED")
                event.defer()
        except ZooKeeperClusterError as e:
            logger.debug(str(e))
            logger.debug("EVENT DEFFERRED")
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
