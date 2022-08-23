#!/usr/bin/env python3
# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.

"""Charmed Machine Operator for Apache ZooKeeper."""

import logging

from charms.kafka.v0.kafka_snap import KafkaSnap
from charms.rolling_ops.v0.rollingops import RollingOpsManager
from ops.charm import CharmBase
from ops.framework import EventBase
from ops.main import main
from ops.model import ActiveStatus, BlockedStatus, MaintenanceStatus

from cluster import ZooKeeperCluster
from config import ZooKeeperConfig
from literals import CHARM_KEY
from provider import ZooKeeperProvider
from utils import generate_password, safe_get_file

logger = logging.getLogger(__name__)


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
        self.framework.observe(
            getattr(self.on, "leader_elected"), self._on_cluster_relation_changed
        )
        self.framework.observe(
            getattr(self.on, "config_changed"), self._on_cluster_relation_changed
        )
        self.framework.observe(
            getattr(self.on, "cluster_relation_changed"), self._on_cluster_relation_changed
        )
        self.framework.observe(
            getattr(self.on, "cluster_relation_joined"), self._on_cluster_relation_changed
        )
        self.framework.observe(
            getattr(self.on, "cluster_relation_departed"), self._on_cluster_relation_changed
        )

    def _on_install(self, event) -> None:
        """Handler for the `on_install` event."""
        self.unit.status = MaintenanceStatus("installing Kafka snap")

        install = self.snap.install()
        if not install:
            self.unit.status = BlockedStatus("unable to install Kafka snap")

        if self.unit.is_leader():
            # sometimes `install` triggers before peer_relation_joined
            if not self.cluster.relation:
                event.defer()
                return
            else:
                self.set_passwords()

    def _on_cluster_relation_changed(self, event: EventBase) -> None:
        """Generic handler for all `config_changed` events across relations."""
        # don't run if leader has not yet created passwords
        if not self.cluster.passwords_set:
            return

        # defer to ensure `cluster_relation_joined/departed` isn't lost on leader during scaling
        if not self.cluster.relation:
            event.defer()
            return

        # triggers a `cluster_relation_changed` to wake up following units
        self.add_init_leader()

        # don't rolling restart if unit has not yet started
        if not self.cluster.relation.data[self.unit].get("state", None) == "started":
            unit_id = self.cluster.get_unit_id(unit=self.unit)
            if self.cluster.is_unit_turn(unit_id=unit_id):
                self.init_server()

            # return if init or not, as units should not do anything until started
            return

        # ensures leader doesn't remove all units upon departure
        if getattr(event, "departing_unit", None) == self.unit:
            return

        # first run sets `quorum` to cluster app data
        # triggers a `cluster_relation_changed` to wake up following units
        self.update_quorum()

        # avoids messing with the running quorum they have been updated
        if self.cluster.relation.data[self.app].get("quorum", None) == "started":
            self.on[self.restart.name].acquire_lock.emit()

    def _restart(self, _):
        """Handler for emitted restart events."""
        # don't restart if unit not initialised
        if not self.cluster.relation.data[self.unit].get("state") == "started":
            return

        # don't restart until quorum has been updated by leader
        if not self.cluster.relation.data[self.app].get("quorum") == "started":
            return

        if self.config_changed():
            self.snap.restart_snap_service(snap_service=CHARM_KEY)
            self.unit.status = ActiveStatus()

    def init_server(self):
        """Calls startup functions for server start.

        Sets myid, opts env_var, initial servers in dynamic properties,
            default properties and jaas_config
        """
        self.unit.status = MaintenanceStatus("starting ZooKeeper server")
        logger.info(f"Server.{self.cluster.get_unit_id(self.unit) + 1} initializing")

        # setting default properties
        self.zookeeper_config.set_zookeeper_myid()
        self.zookeeper_config.set_kafka_opts()

        # servers properties needs to be written to dynamic config
        servers = self.cluster.startup_servers(unit=self.unit)
        self.zookeeper_config.set_zookeeper_dynamic_properties(servers=servers)

        self.zookeeper_config.set_zookeeper_properties()
        self.zookeeper_config.set_jaas_config()

        self.snap.restart_snap_service(snap_service=CHARM_KEY)
        self.unit.status = ActiveStatus()

        # unit flags itself as 'started' so it can be retrieved by the leader
        logger.info(f"Server.{self.cluster.get_unit_id(self.unit) + 1} started")
        self.cluster.relation.data[self.unit].update({"state": "started"})

    def config_changed(self):
        """Compares expected vs actual config that would require a restart to apply."""
        properties = safe_get_file(self.zookeeper_config.properties_filepath) or []
        server_properties = self.zookeeper_config.build_static_properties(properties)
        config_properties = self.zookeeper_config.static_properties

        properties_changed = set(server_properties) ^ set(config_properties)

        jaas_config = safe_get_file(self.zookeeper_config.jaas_filepath) or []
        jaas_changed = set(jaas_config) ^ set(self.zookeeper_config.jaas_config.splitlines())

        if not (properties_changed or jaas_changed):
            return False

        if properties_changed:
            logger.info(
                (
                    f"Server.{self.cluster.get_unit_id(self.unit) + 1} updating properties - "
                    f"OLD PROPERTIES = {set(server_properties) - set(config_properties)}, "
                    f"NEW PROPERTIES = {set(config_properties) - set(server_properties)}"
                )
            )
            self.zookeeper_config.set_zookeeper_properties()

        if jaas_changed:
            clean_server_jaas = [conf.strip() for conf in jaas_config]
            clean_config_jaas = [
                conf.strip() for conf in self.zookeeper_config.jaas_config.splitlines()
            ]
            logger.info(
                (
                    f"Server.{self.cluster.get_unit_id(self.unit) + 1} updating JAAS config - "
                    f"OLD JAAS = {set(clean_server_jaas) - set(clean_config_jaas)}, "
                    f"NEW JAAS = {set(clean_config_jaas) - set(clean_server_jaas)}"
                )
            )
            self.zookeeper_config.set_jaas_config()

        return True

    def set_passwords(self):
        """Sets super-user and server-server auth user passwords to relation data."""
        if not self.cluster.passwords_set:
            self.cluster.relation.data[self.app].update({"sync-password": generate_password()})
            self.cluster.relation.data[self.app].update({"super-password": generate_password()})

    def update_quorum(self):
        """Updates the server quorum members for all currently started units in the relation."""
        if not self.unit.is_leader():
            return

        # triggers a `cluster_relation_changed` to wake up following units
        updated_servers = self.cluster.update_cluster()
        if updated_servers:
            # flag to permit restarts only after first quorum update
            self.cluster.relation.data[self.app].update({"quorum": "started"})

        self.cluster.relation.data[self.app].update(updated_servers)

    def add_init_leader(self):
        """Adds the first leader server to the relation data for other units to ack."""
        if not self.unit.is_leader():
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


if __name__ == "__main__":
    main(ZooKeeperCharm)
