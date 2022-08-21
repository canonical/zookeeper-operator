#!/usr/bin/env python3
# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.

"""Charmed Machine Operator for Apache ZooKeeper."""

import logging

from charms.kafka.v0.kafka_snap import KafkaSnap
from charms.rolling_ops.v0.rollingops import RollingOpsManager
from charms.zookeeper.v0.zookeeper_provider import ZooKeeperProvider
from ops.charm import CharmBase
from ops.framework import EventBase
from ops.main import main
from ops.model import ActiveStatus, BlockedStatus, MaintenanceStatus

from cluster import (
    NoPasswordError,
    NotUnitTurnError,
    UnitNotFoundError,
    ZooKeeperCluster,
)
from config import ZooKeeperConfig
from utils import generate_password

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
        self.restart = RollingOpsManager(self, relation="restart", callback=self._restart)
        self.provider = ZooKeeperProvider(self)
        self.zookeeper_config = ZooKeeperConfig(self)

        self.framework.observe(getattr(self.on, "install"), self._on_install)
        self.framework.observe(getattr(self.on, "start"), self._on_start)
        self.framework.observe(
            getattr(self.on, "leader_elected"), self._on_cluster_relation_updated
        )
        self.framework.observe(getattr(self.on, "config_changed"), self._on_config_changed)
        self.framework.observe(
            getattr(self.on, "cluster_relation_changed"), self._on_cluster_relation_updated
        )
        self.framework.observe(
            getattr(self.on, "cluster_relation_joined"), self._on_cluster_relation_updated
        )
        self.framework.observe(
            getattr(self.on, "cluster_relation_departed"), self._on_cluster_relation_updated
        )

    def _on_install(self, _) -> None:
        """Handler for the `on_install` event.

        This includes:
            - Installing the snap
            - Writing config to config files
        """
        self.unit.status = MaintenanceStatus("installing Kafka snap")

        install = self.snap.install()
        if not install:
            self.unit.status = BlockedStatus("unalbe to install Kafka snap")

        # setting default properties
        self.zookeeper_config.set_zookeeper_properties()
        self.zookeeper_config.set_zookeeper_myid()

    def _on_start(self, event: EventBase) -> None:
        """Handler for the `on_start` event.

        This includes:
            - Setting unit readiness to relation data
            - Checking if the unit is next in line to start
            - Writing config to config files
            - Starting the snap service
        """
        # setting default app passwords on leader start
        if self.unit.is_leader():
            for password in ["super_password", "sync_password"]:
                current_value = self.cluster.relation.data[self.app].get(password, None)
                self.cluster.relation.data[self.app].update(
                    {password: current_value or generate_password()}
                )

        if not self.cluster.passwords_set:
            event.defer()
            return

        self.unit.status = MaintenanceStatus("starting ZooKeeper unit")

        # checks if the unit is next, grabs the servers to add, and it's own config for debugging
        try:
            servers, unit_config = self.cluster.ready_to_start(self.unit)
        except (NotUnitTurnError, UnitNotFoundError, NoPasswordError) as e:
            logger.info(str(e))
            self.unit.status = self.cluster.status
            event.defer()
            return

        # servers properties needs to be written to dynamic config
        self.zookeeper_config.set_zookeeper_properties()
        self.zookeeper_config.set_zookeeper_dynamic_properties(servers=servers)

        # grabbing up-to-date jaas users from the relations
        self.zookeeper_config.set_jaas_config()

        # KAFKA_OPTS env var gets loaded on snap start
        self.zookeeper_config.set_kafka_opts()

        self.snap.restart_snap_service(snap_service=CHARM_KEY)
        self.unit.status = ActiveStatus()

        # unit flags itself as 'started' so it can be retrieved by the leader
        self.cluster.relation.data[self.unit].update(unit_config)
        self.cluster.relation.data[self.unit].update({"state": "started"})

    def _on_config_changed(self, _):
        """Handler for the `config-changed` event.

        This includes:
            - Writing config to config files
            - Restarting zookeeper service
        """
        self.on[self.restart.name].acquire_lock.emit()

    def _on_cluster_relation_updated(self, event: EventBase) -> None:
        """Handler for events triggered by changing units.

        This includes:
            - Adding ready-to-start units to app data
            - Updating ZK quorum config
            - Updating app data state
        """
        if not self.unit.is_leader():
            return

        # ensures leader doesn't remove all units upon departure
        if getattr(event, "departing_unit", None) == self.unit:
            return

        # units need to exist in the app data to be iterated through for next_turn
        for unit in self.cluster.started_units:
            unit_id = self.cluster.get_unit_id(unit)
            current_value = self.cluster.relation.data[self.app].get(str(unit_id), None)

            # sets to "added" for init quorum leader, if not already exists
            # may already exist if during the case of a failover of the first unit
            if unit_id == self.cluster.lowest_unit_id:
                self.cluster.relation.data[self.app].update(
                    {str(unit_id): current_value or "added"}
                )

        if not self.cluster.passwords_set:
            event.defer()
            return

        # adds + removes members for all self-confirmed started units
        updated_servers = self.cluster.update_cluster()

        # either Active if successful, else Maintenance
        self.unit.status = self.cluster.status

        if self.cluster.status == ActiveStatus():
            self.cluster.relation.data[self.app].update(updated_servers)
        else:
            # in the event some unit wasn't started/ready
            event.defer()
            return

    def _restart(self, event: EventBase):
        """Handler for rolling restart events triggered by zookeeper_relation_changed/broken."""
        # for when relations trigger during start-up of the cluster
        if not self.cluster.relation.data[self.unit].get("state", None) == "started":
            event.defer()
            return

        self._on_start(event=event)


if __name__ == "__main__":
    main(ZooKeeperCharm)
