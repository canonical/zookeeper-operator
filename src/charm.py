#!/usr/bin/env python3
# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.

"""Charmed Machine Operator for Apache ZooKeeper."""

import logging
import time

from charms.grafana_k8s.v0.grafana_dashboard import GrafanaDashboardProvider
from charms.prometheus_k8s.v0.prometheus_scrape import MetricsEndpointProvider
from charms.rolling_ops.v0.rollingops import RollingOpsManager
from cluster import ZooKeeperCluster
from config import ZooKeeperConfig
from literals import CHARM_KEY, CHARM_USERS, JMX_PORT, METRICS_PROVIDER_PORT, NODE_EXPORTER_PORT
from ops.charm import (
    ActionEvent,
    CharmBase,
    InstallEvent,
    LeaderElectedEvent,
    RelationDepartedEvent,
)
from ops.framework import EventBase
from ops.main import main
from ops.model import ActiveStatus, BlockedStatus, MaintenanceStatus, WaitingStatus
from provider import ZooKeeperProvider
from snap import ZooKeeperSnap
from tls import ZooKeeperTLS
from utils import generate_password, safe_get_file

logger = logging.getLogger(__name__)


class ZooKeeperCharm(CharmBase):
    """Charmed Operator for ZooKeeper."""

    def __init__(self, *args):
        super().__init__(*args)
        self.name = CHARM_KEY
        self.snap = ZooKeeperSnap()
        self.cluster = ZooKeeperCluster(self)
        self.restart = RollingOpsManager(self, relation="restart", callback=self._restart)
        self.provider = ZooKeeperProvider(self)
        self.zookeeper_config = ZooKeeperConfig(self)
        self.tls = ZooKeeperTLS(self)
        self.grafana_dashboards = GrafanaDashboardProvider(self)
        self.metrics_endpoint = MetricsEndpointProvider(
            self,
            refresh_event=self.on.start,
            jobs=[
                {
                    "static_configs": [
                        {
                            "targets": [
                                f"*:{NODE_EXPORTER_PORT}",
                                f"*:{JMX_PORT}",
                                f"*:{METRICS_PROVIDER_PORT}",
                            ]
                        }
                    ]
                }
            ],
        )

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
        self.framework.observe(
            getattr(self.on, "get_super_password_action"), self._get_super_password_action
        )
        self.framework.observe(
            getattr(self.on, "get_sync_password_action"), self._get_sync_password_action
        )
        self.framework.observe(getattr(self.on, "set_password_action"), self._set_password_action)

    def _on_install(self, event: InstallEvent) -> None:
        """Handler for the `on_install` event."""
        self.unit.status = MaintenanceStatus("installing ZooKeeper Snap")

        install = self.snap.install()
        if not install:
            self.unit.status = BlockedStatus("unable to install ZooKeeper Snap")

        # don't complete install until passwords set
        if not self.cluster.relation:
            self.unit.status = WaitingStatus("waiting for peer relation")
            event.defer()
            return

        self.set_passwords()

        # give the init leader a quorum
        if self.unit.is_leader():
            self.cluster.relation.data[self.app].update({"quorum": "non-ssl"})

    def _on_cluster_relation_changed(self, event: EventBase) -> None:
        """Generic handler for all 'something changed, update' events across all relations."""
        # not all methods called
        if not self.cluster.relation:
            self.unit.status = WaitingStatus("waiting for peer relation")
            return

        # If a password rotation is needed, or in progress
        if not self.rotate_passwords():
            return

        # attempt startup of server
        if not self.cluster.started:
            self.init_server()

        # even if leader has not started, attempt update quorum
        self.update_quorum(event=event)

        # don't delay scale-down leader ops by restarting dying unit
        if getattr(event, "departing_unit", None) == self.unit:
            return

        # check whether restart is needed for all `*_changed` events
        if (self.config_changed() or self.tls.upgrading) and self.cluster.started:
            self.on[f"{self.restart.name}"].acquire_lock.emit()

        # ensures events aren't lost during an upgrade on single units
        if self.tls.upgrading and len(self.cluster.peer_units) == 1:
            event.defer()

    def _restart(self, event: EventBase) -> None:
        """Handler for emitted restart events."""
        if not self.cluster.stable:
            logger.info(f"RESTART - NOT STABLE - DEFERRING")
            event.defer()
            return

        logger.info(f"Server.{self.cluster.get_unit_id(self.unit)} restarting")
        self.snap.restart_snap_service(snap_service="daemon")

        # gives time for server to rejoin quorum, as command exits too fast
        # without, other units might restart before this unit rejoins, losing quorum
        time.sleep(5)

        self.unit.status = ActiveStatus()

        # Indicate that unit has completed restart on password rotation
        if self.cluster.relation.data[self.app].get("rotate-passwords"):
            self.cluster.relation.data[self.unit]["password-rotated"] = "true"

        if not self.cluster.relation.data[self.unit].get("unified") and self.tls.upgrading:
            logger.info("SETTING UNIFIED")

        self.cluster.relation.data[self.unit].update(
            {
                # flag to declare unit running `portUnification` during ssl<->no-ssl upgrade
                "unified": "true" if self.tls.upgrading else "",
                # flag to declare unit restarted with new quorum encryption
                "quorum": self.cluster.quorum,
            }
        )

        self.provider.apply_relation_data()

    def init_server(self):
        """Calls startup functions for server start.

        Sets myid, server_jvmflgas env_var, initial servers in dynamic properties,
            default properties and jaas_config
        """
        # don't run if leader has not yet created passwords
        if not self.cluster.passwords_set:
            self.unit.status = MaintenanceStatus("waiting for passwords to be created")
            return

        # start units in order
        if not self.cluster.is_unit_turn(self.unit):
            self.unit.status = MaintenanceStatus("waiting for unit turn to start")
            return

        self.unit.status = MaintenanceStatus("starting ZooKeeper server")
        logger.info(f"Server.{self.cluster.get_unit_id(self.unit)} initializing")

        # setting default properties
        self.zookeeper_config.set_zookeeper_myid()
        self.zookeeper_config.set_server_jvmflags()

        # servers properties needs to be written to dynamic config
        servers = self.cluster.startup_servers(unit=self.unit)
        logger.debug(f"{servers=}")
        self.zookeeper_config.set_zookeeper_dynamic_properties(servers=servers)

        logger.debug("setting properties and jaas")
        self.zookeeper_config.set_zookeeper_properties()
        self.zookeeper_config.set_jaas_config()

        logger.debug("starting snap service")
        self.snap.start_snap_service(snap_service="daemon")
        self.unit.status = ActiveStatus()

        # unit flags itself as 'started' so it can be retrieved by the leader
        logger.info(f"Server.{self.cluster.get_unit_id(self.unit)} started")

        # added here in case a `restart` was missed
        self.cluster.relation.data[self.unit].update(
            {
                "state": "started",
                "unified": "true" if self.tls.upgrading else "",
                "quorum": self.cluster.quorum,
            }
        )

    def config_changed(self) -> bool:
        """Compares expected vs actual config that would require a restart to apply."""
        properties = safe_get_file(self.zookeeper_config.properties_filepath) or []
        server_properties = self.zookeeper_config.build_static_properties(properties)
        config_properties = self.zookeeper_config.static_properties

        properties_changed = set(server_properties) ^ set(config_properties)
        logger.debug(f"{properties_changed=}")

        jaas_config = safe_get_file(self.zookeeper_config.jaas_filepath) or []
        jaas_changed = set(jaas_config) ^ set(self.zookeeper_config.jaas_config.splitlines())

        if not (properties_changed or jaas_changed):
            return False

        if properties_changed:
            logger.info(
                (
                    f"Server.{self.cluster.get_unit_id(self.unit)} updating properties - "
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
                    f"Server.{self.cluster.get_unit_id(self.unit)} updating JAAS config - "
                    f"OLD JAAS = {set(clean_server_jaas) - set(clean_config_jaas)}, "
                    f"NEW JAAS = {set(clean_config_jaas) - set(clean_server_jaas)}"
                )
            )
            self.zookeeper_config.set_jaas_config()

        return True

    def set_passwords(self) -> None:
        """Sets super-user and server-server auth user passwords to relation data."""
        if not self.unit.is_leader():
            return

        if not self.cluster.passwords_set:
            self.cluster.relation.data[self.app].update({"sync-password": generate_password()})
            self.cluster.relation.data[self.app].update({"super-password": generate_password()})

    def update_quorum(self, event: EventBase) -> None:
        """Updates the server quorum members for all currently started units in the relation."""
        if not self.unit.is_leader() or getattr(event, "departing_unit", None) == self.unit:
            return

        # set first unit to "added" asap to get the units starting sooner
        self.add_init_leader()

        if (
            self.cluster.stale_quorum  # in the case of scale-up
            or isinstance(  # to run without delay to maintain quorum on scale down
                event,
                (RelationDepartedEvent, LeaderElectedEvent),
            )
        ):
            updated_servers = self.cluster.update_cluster()
            logger.debug(f"{updated_servers=}")
            # triggers a `cluster_relation_changed` to wake up following units
            self.cluster.relation.data[self.app].update(updated_servers)

        # default startup without ssl relation
        logger.info("UPDATE QUORUM - CHECKING STABLE")
        if not self.cluster.stable:
            return

        # declare upgrade complete only when all peer units have started
        # triggers `cluster_relation_changed` to rolling-restart without `portUnification`
        if self.tls.all_units_unified:
            logger.info("ALL UNITS UNIFIED")
            if self.tls.enabled:
                logger.info("TLS ENABLED - SWITCHING TO SSL")
                self.cluster.relation.data[self.app].update({"quorum": "ssl"})
            else:
                logger.info("TLS ENABLED - SWITCHING TO NON-SSL")
                self.cluster.relation.data[self.app].update({"quorum": "non-ssl"})

            if self.cluster.all_units_quorum:
                logger.info("ALL UNITS QUORUM - REMOVING UPGRADE")
                self.cluster.relation.data[self.app].update({"upgrading": ""})
                logger.info(f"ZooKeeper cluster switching to {self.cluster.quorum} quorum")
                logger.info("UPDATE QUORUM - NEW QUORUM - UPDATING RELATION DATA")

        if self.cluster.all_units_quorum and not self.tls.upgrading:
            logger.info("ALL UNITS QUORUM AND NOT UPGRADING - APPLYING")
            self.provider.apply_relation_data()

    def add_init_leader(self) -> None:
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

    def rotate_passwords(self) -> bool:
        """Handle password rotation and check the status of the process.

        If a password rotation is happening, take the necessary steps to issue a
        rolling restart from each unit.

        Return:
            bool: True when password rotation is finished, false otherwise.
        """
        # Logic for password rotation
        if self.cluster.relation.data[self.app].get("rotate-passwords"):
            # All units have rotated the password, we can remove the global flag
            if self.unit.is_leader() and self.cluster._all_rotated():
                self.cluster.relation.data[self.app]["rotate-passwords"] = ""
                return False

            # Own unit finished rotation, no need to issue a new lock
            if self.cluster.relation.data[self.unit].get("password-rotated"):
                return False

            logger.info("Acquiring lock for password rotation")
            self.on[f"{self.restart.name}"].acquire_lock.emit()
            return False

        else:
            # After removal of global flag, each unit can reset its state so more
            # password rotations can happen
            self.cluster.relation.data[self.unit]["password-rotated"] = ""
            return True

    def _get_super_password_action(self, event: ActionEvent) -> None:
        """Handler for get-super-password action event."""
        event.set_results({"super-password": self.cluster.passwords[0]})

    def _get_sync_password_action(self, event: ActionEvent) -> None:
        """Handler for get-sync-password action event."""
        event.set_results({"sync-password": self.cluster.passwords[1]})

    def _set_password_action(self, event: ActionEvent) -> None:
        """Handler for set-password action.

        Set the password for a specific user, if no passwords are passed, generate them.
        """
        if not self.unit.is_leader():
            msg = "Password rotation must be called on leader unit"
            logger.error(msg)
            event.fail(msg)
            return

        username = event.params.get("username", "super")
        if username not in CHARM_USERS:
            msg = f"The action can be run only for users used by the charm: {CHARM_USERS} not {username}."
            logger.error(msg)
            event.fail(msg)
            return

        new_password = event.params.get("password", generate_password())

        # Passwords should not be the same.
        if new_password in self.cluster.passwords:
            event.log("The old and new passwords are equal.")
            event.set_results({f"{username}-password": new_password})
            return

        # Store those passwords on application databag
        self.cluster.relation.data[self.app].update({f"{username}-password": new_password})

        # Add password flag
        self.cluster.relation.data[self.app]["rotate-passwords"] = "true"
        event.set_results({f"{username}-password": new_password})


if __name__ == "__main__":
    main(ZooKeeperCharm)
