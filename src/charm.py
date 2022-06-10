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


def event_handler(raise_status: str, raise_conditions: Tuple, run_on_leader=False):
    def wrap(func):
        def wrapped_event(*args, **kwargs):
            unit = getattr(kwargs["event"].framework, "model").unit
            if run_on_leader and not unit.is_leader():
                return

            try:
                return func(*args, **kwargs)
            except raise_conditions as e:
                status_message = e.get("status", "")
                status_map = {
                    "waiting": WaitingStatus(status_message),
                    "blocked": BlockedStatus(status_message),
                }
                unit.status = status_map[raise_status]

        return wrapped_event

    return wrap


class ZooKeeperCharm(CharmBase):
    """Charmed Operator for ZooKeeper."""

    def __init__(self, *args):
        super().__init__(*args)
        self.name = CHARM_KEY
        self.snap = KafkaSnap()
        self.zookeeper = ZooKeeperCluster(self)

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

        self.framework.observe(
            getattr(self.on, "get_snap_apps_action"), self._on_get_snap_apps_action
        )

    @event_handler(raise_status="blocked", raise_conditions=(KafkaSnapError,))
    def _on_install(self, event) -> None:
        """Handler for on_install event."""
        self.unit.status = MaintenanceStatus("starting unit install")

        self.snap.install_kafka_snap()
        self.snap.write_default_properties(
            properties=self.config["zookeeper-properties"], property_label="zookeeper"
        )
        self.snap.write_zookeeper_myid(myid=self.zookeeper.get_server_id(self.unit))

    @event_handler(raise_status="blocked", raise_conditions=(KafkaSnapError,))
    def _on_start(self, event) -> None:
        self.snap.start_snap_service(snap_service=CHARM_KEY)
        self.unit.status = ActiveStatus()

    @event_handler(
        raise_status="waiting",
        raise_conditions=(MembersSyncingError, MemberNotReadyError),
        run_on_leader=True,
    )
    def _on_cluster_relation_updated(self, event) -> None:
        self.unit.status = MaintenanceStatus("starting cluster member update")

        if self.zookeeper.update_cluster():
            self.unit.status = ActiveStatus()
            return
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
