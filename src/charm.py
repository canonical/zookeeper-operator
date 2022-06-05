#!/usr/bin/env python3
# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.

"""Charmed Machine Operator for Apache ZooKeeper."""

import logging

from charms.kafka.v0.kafka_snap import KafkaSnap
from charms.kafka.v0.zookeeper_provides import ZooKeeperProvides
from ops.charm import CharmBase
from ops.main import main

from cluster import UnitsChangedEvent, UpdateServersEvent, ZooKeeperCluster, ZooKeeperClusterEvents

logger = logging.getLogger(__name__)


CHARM_KEY = "zookeeper"


class ZooKeeperCharm(CharmBase):
    """Charmed Operator for ZooKeeper."""

    on = ZooKeeperClusterEvents()

    def __init__(self, *args):
        super().__init__(*args)
        self.name = CHARM_KEY
        # self.zookeeper_provides = ZooKeeperProvides(self)
        self.cluster = ZooKeeperCluster(self)
        self.snap = KafkaSnap()

        self.framework.observe(getattr(self.on, "install"), self._on_install)

        self.framework.observe(getattr(self.on, "units_changed"), self._on_units_changed)
        self.framework.observe(getattr(self.on, "update_servers"), self._on_update_servers)

        self.framework.observe(
            getattr(self.on, f"get_{CHARM_KEY}_properties_action"),
            self._on_get_properties_action,
        )
        self.framework.observe(
            getattr(self.on, "get_snap_apps_action"), self._on_get_snap_apps_action
        )

    def _on_units_changed(self, event: UnitsChangedEvent):
        logger.info("***********************************")
        logger.info(f"UnitsChangedEvent detected - {vars(event)}")
        logger.info("***********************************")
        self.cluster.on_units_changed(event=event)

    def _on_update_servers(self, event: UpdateServersEvent):
        return

    def _on_install(self, _) -> None:
        """Handler for on_install event."""
        self.unit.status = self.snap.install_kafka_snap()
        self.unit.status = self.snap.set_properties(
            properties=self.config["zookeeper-properties"], property_label="zookeeper"
        )
        self.unit.status = self.snap.start_snap_service(snap_service=CHARM_KEY)

    def _on_cluster_relation_created(self, _) -> None:
        return

    def _on_cluster_relation_joined(self, _) -> None:
        return

    def _on_restart(self, _):
        return

    def _on_get_properties_action(self, event) -> None:
        """Handler for users to copy currently active config for passing to `juju config`."""
        msg = self.snap.get_merged_properties(property_label="zookeeper")
        event.set_results({"properties": msg})

    def _on_get_snap_apps_action(self, event) -> None:
        """Handler for users to retrieve the list of available Kafka snap commands."""
        msg = self.snap.get_kafka_apps()
        event.set_results({"apps": msg})


if __name__ == "__main__":
    main(ZooKeeperCharm)
