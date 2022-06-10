# TODO: Add unitid to myid
# TODO: Add run it through and write tests as you go


#!/usr/bin/env python3
# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.

"""Charmed Machine Operator for Apache ZooKeeper."""

import logging
from typing import Any, Callable

from ops.model import MaintenanceStatus, WaitingStatus

from charms.kafka.v0.kafka_snap import KafkaSnap
from charms.zookeeper.v0.cluster import ZooKeeperCluster
from ops.charm import CharmBase
from ops.main import main

logger = logging.getLogger(__name__)


CHARM_KEY = "zookeeper"


# small decorator to ensure function is ran as leader
def leader_check(func: Callable) -> Any:
    def check_unit_leader(*args, **kwargs):
        if not kwargs["event"].framework.model.unit.is_leader():
            return
        else:
            return func(*args, **kwargs)

    return check_unit_leader


class ZooKeeperCharm(CharmBase):
    """Charmed Operator for ZooKeeper."""

    def __init__(self, *args):
        super().__init__(*args)
        self.name = CHARM_KEY
        self.snap = KafkaSnap()
        self.zookeeper = ZooKeeperCluster(self)

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

        self.framework.observe(
            getattr(self.on, f"get_{CHARM_KEY}_properties_action"),
            self._on_get_properties_action,
        )
        self.framework.observe(
            getattr(self.on, "get_snap_apps_action"), self._on_get_snap_apps_action
        )

    def _on_install(self, _) -> None:
        """Handler for on_install event."""
        self.unit.status = self.snap.install_kafka_snap()
        self.unit.status = self.snap.set_properties(
            properties=self.config["zookeeper-properties"], property_label="zookeeper"
        )
        # self.unit.status = self.cluster.set_myid()
        self.unit.status = self.snap.start_snap_service(snap_service=CHARM_KEY)

    def _on_initialise_service(self, _):
        return

    @leader_check
    def _on_cluster_relation_updated(self, event):
        self.unit.status = MaintenanceStatus("starting cluster member update")
        self.unit.status = self.zookeeper.update_cluster()
        if isinstance(self.unit.status, WaitingStatus):
            event.defer()

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
