#!/usr/bin/env python3
# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.

"""Charmed Machine Operator for Apache ZooKeeper."""

import logging
import time

from charms.grafana_agent.v0.cos_agent import COSAgentProvider
from charms.rolling_ops.v0.rollingops import RollingOpsManager
from ops.charm import (
    CharmBase,
    InstallEvent,
    LeaderElectedEvent,
    RelationDepartedEvent,
)
from ops.framework import EventBase
from ops.main import main
from ops.model import ActiveStatus, StatusBase

from core.cluster import ClusterState
from events.password_actions import PasswordActionEvents
from events.provider import ProviderEvents
from events.tls import TLSEvents
from events.upgrade import ZKUpgradeEvents, ZooKeeperDependencyModel
from literals import (
    CHARM_KEY,
    CHARM_USERS,
    DEPENDENCIES,
    JMX_PORT,
    METRICS_PROVIDER_PORT,
    SUBSTRATE,
    DebugLevel,
    Status,
)
from managers.config import ConfigManager
from managers.quorum import QuorumManager
from managers.tls import TLSManager
from workload import ZKWorkload

logger = logging.getLogger(__name__)


class ZooKeeperCharm(CharmBase):
    """Charmed Operator for ZooKeeper."""

    def __init__(self, *args):
        super().__init__(*args)
        self.name = CHARM_KEY
        self.state = ClusterState(self, substrate=SUBSTRATE)
        self.workload = ZKWorkload()

        # --- CHARM EVENT HANDLERS ---

        self.password_action_events = PasswordActionEvents(self)
        self.tls_events = TLSEvents(self)
        self.provider_events = ProviderEvents(self)
        self.upgrade_events = ZKUpgradeEvents(
            self,
            substrate=SUBSTRATE,
            dependency_model=ZooKeeperDependencyModel(
                **DEPENDENCIES  # pyright: ignore[reportGeneralTypeIssues]
            ),
        )

        # --- MANAGERS ---

        self.quorum_manager = QuorumManager(state=self.state)
        self.tls_manager = TLSManager(
            state=self.state, workload=self.workload, substrate=SUBSTRATE
        )
        self.config_manager = ConfigManager(
            state=self.state, workload=self.workload, substrate=SUBSTRATE, config=self.config
        )

        # --- LIB EVENT HANDLERS ---

        self.restart = RollingOpsManager(self, relation="restart", callback=self._restart)
        self._grafana_agent = COSAgentProvider(
            self,
            metrics_endpoints=[
                {"path": "/metrics", "port": JMX_PORT},
                {"path": "/metrics", "port": METRICS_PROVIDER_PORT},
            ],
            metrics_rules_dir="./src/alert_rules/prometheus",
            logs_rules_dir="./src/alert_rules/loki",
            log_slots=["charmed-zookeeper:logs"],
        )
        # --- CORE EVENTS ---

        self.framework.observe(getattr(self.on, "install"), self._on_install)
        self.framework.observe(getattr(self.on, "start"), self._manual_restart)
        self.framework.observe(
            getattr(self.on, "update_status"), self._on_cluster_relation_changed
        )
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

    # --- CORE EVENT HANDLERS ---

    def _on_install(self, event: InstallEvent) -> None:
        """Handler for the `on_install` event."""
        install = self.workload.install()
        if not install:
            self._set_status(Status.SERVICE_NOT_INSTALLED)
            event.defer()
            return

        self.unit.set_workload_version(self.workload.get_version())
        # don't complete install until passwords set
        if not self.state.peer_relation:
            self._set_status(Status.NO_PEER_RELATION)
            event.defer()
            return

        if self.unit.is_leader() and not self.state.cluster.internal_user_credentials:
            for user in CHARM_USERS:
                self.state.cluster.update({f"{user}-password": self.workload.generate_password()})

        # refreshing unit hostname relation data in case ip changed
        self.state.unit_server.update(self.quorum_manager.get_hostname_mapping())

        # give the leader a default quorum during cluster initialisation
        if self.unit.is_leader():
            self.state.cluster.update({"quorum": "default - non-ssl"})

    def _on_cluster_relation_changed(self, event: EventBase) -> None:
        """Generic handler for all 'something changed, update' events across all relations."""
        # not all methods called
        if not self.state.peer_relation:
            self._set_status(Status.NO_PEER_RELATION)
            return

        # don't want to prematurely set config using outdated/missing relation data
        # also skip update-status overriding statues during upgrades
        if not self.upgrade_events.idle:
            event.defer()
            return

        self.unit.set_workload_version(self.workload.get_version())

        # refreshing unit hostname relation data in case ip changed
        self.state.unit_server.update(self.quorum_manager.get_hostname_mapping())
        self.config_manager.set_etc_hosts()

        # don't run (and restart) if some units are still joining
        # instead, wait for relation-changed from it's setting of 'started'
        # also don't run (and restart) if some units still need to set ip
        self._set_status(self.state.all_installed)
        if not isinstance(self.unit.status, ActiveStatus):
            return

        # attempt startup of server
        if not self.state.unit_server.started:
            self.init_server()

        # even if leader has not started, attempt update quorum
        self.update_quorum(event=event)

        # don't delay scale-down leader ops by restarting dying unit
        if getattr(event, "departing_unit", None) == self.unit:
            return

        # check whether restart is needed for all `*_changed` events
        # only restart where necessary to avoid slowdowns
        # config_changed call here implicitly updates jaas + zoo.cfg
        if (
            (self.config_manager.config_changed() or self.state.cluster.switching_encryption)
            and self.state.unit_server.started
            and self.upgrade_events.idle
        ):
            self.on[f"{self.restart.name}"].acquire_lock.emit()

        # ensures events aren't lost during an upgrade on single units
        if self.state.cluster.switching_encryption and len(self.state.servers) == 1:
            event.defer()

        # during normal operation after cluster set-up
        # alerts in the status if any unit has silently stopped running
        if not self.workload.alive:
            self._set_status(Status.SERVICE_NOT_RUNNING)
            return

        # service can stop serving requests if the quorum is lost
        if self.state.unit_server.started and not self.workload.healthy:
            self._set_status(Status.SERVICE_UNHEALTHY)
            return

        self._set_status(Status.ACTIVE)

    def _manual_restart(self, event: EventBase) -> None:
        """Forces a rolling-restart event.

        Necessary for ensuring that `on_start` restarts roll.
        """
        if not self.state.peer_relation or not self.state.stable or not self.upgrade_events.idle:
            event.defer()
            return

        # not needed during application init
        # only needed for scenarios where the LXD goes down (e.g PC shutdown)
        if not self.workload.alive:
            self.on[f"{self.restart.name}"].acquire_lock.emit()

    def _restart(self, event: EventBase) -> None:
        """Handler for emitted restart events."""
        self._set_status(self.state.stable)
        if not isinstance(self.unit.status, ActiveStatus) or not self.upgrade_events.idle:
            event.defer()
            return

        logger.info(f"{self.unit.name} restarting...")
        self.workload.restart()

        # gives time for server to rejoin quorum, as command exits too fast
        # without, other units might restart before this unit rejoins, losing quorum
        time.sleep(5)

        self.state.unit_server.update(
            {
                # flag to declare unit running `portUnification` during ssl<->no-ssl upgrade
                "unified": "true" if self.state.cluster.switching_encryption else "",
                # flag to declare unit restarted with new quorum encryption
                "quorum": self.state.cluster.quorum,
                # indicate that unit has completed restart on password rotation
                "password-rotated": "true" if self.state.cluster.rotate_passwords else "",
            }
        )

        self.update_client_data()

    # --- CONVENIENCE METHODS ---

    def init_server(self):
        """Calls startup functions for server start.

        Sets myid, server_jvmflgas env_var, initial servers in dynamic properties,
            default properties and jaas_config
        """
        # don't run if leader has not yet created passwords
        if not self.state.cluster.internal_user_credentials:
            self._set_status(Status.NO_PASSWORDS)
            return

        # start units in order
        if (
            self.state.next_server
            and self.state.next_server.component.name != self.state.unit_server.component.name
        ):
            self._set_status(Status.NOT_UNIT_TURN)
            return

        logger.info(f"{self.unit.name} initializing...")

        # setting default properties
        self.config_manager.set_zookeeper_myid()
        self.config_manager.set_server_jvmflags()

        # setting ip, fqdn and hostname
        self.state.unit_server.update(self.quorum_manager.get_hostname_mapping())

        # servers properties needs to be written to dynamic config
        self.config_manager.set_zookeeper_dynamic_properties(servers=self.state.startup_servers)

        self.config_manager.set_zookeeper_properties()
        self.config_manager.set_jaas_config()

        # during reschedules (e.g upgrades or otherwise) we lose all files
        # need to manually add-back key/truststores
        if (
            self.state.cluster.tls
            and self.state.unit_server.certificate
            and self.state.unit_server.ca
        ):  # TLS is probably completed
            self.tls_manager.set_private_key()
            self.tls_manager.set_ca()
            self.tls_manager.set_certificate()
            self.tls_manager.set_truststore()
            self.tls_manager.set_p12_keystore()

        self.workload.start()

        # unit flags itself as 'started' so it can be retrieved by the leader
        logger.info(f"{self.unit.name} started")

        # added here in case a `restart` was missed
        self.state.unit_server.update(
            {
                "state": "started",
                "unified": "true" if self.state.cluster.switching_encryption else "",
                "quorum": self.state.cluster.quorum,
            }
        )

    def update_quorum(self, event: EventBase) -> None:
        """Updates the server quorum members for all currently started units in the relation.

        Also sets app-data pertaining to quorum encryption state during upgrades.
        """
        if not self.unit.is_leader() or getattr(event, "departing_unit", None) == self.unit:
            return

        # set first unit to "added" asap to get the units starting sooner
        # sets to "added" for init quorum leader, if not already exists
        # may already exist if during the case of a failover of the first unit
        if (init_leader := self.state.init_leader) and init_leader.started:
            self.state.cluster.update({str(init_leader.unit_id): "added"})

        if (
            self.state.stale_quorum  # in the case of scale-up
            or isinstance(  # to run without delay to maintain quorum on scale down
                event,
                (RelationDepartedEvent, LeaderElectedEvent),
            )
            or self.state.healthy  # to ensure run on update-status
        ):
            updated_servers = self.quorum_manager.update_cluster()
            logger.debug(f"{updated_servers=}")

            # triggers a `cluster_relation_changed` to wake up following units
            self.state.cluster.update(updated_servers)

        # default startup without ssl relation
        logger.debug("updating quorum - checking cluster stability")

        self._set_status(self.state.stable)
        if not isinstance(self.unit.status, ActiveStatus):
            return

        # declare upgrade complete only when all peer units have started
        # triggers `cluster_relation_changed` to rolling-restart without `portUnification`
        if self.state.all_units_unified:
            logger.debug("all units unified")
            if self.state.cluster.tls:
                logger.debug("tls enabled - switching to ssl")
                self.state.cluster.update({"quorum": "ssl"})
            else:
                logger.debug("tls disabled - switching to non-ssl")
                self.state.cluster.update({"quorum": "non-ssl"})

            if self.state.all_units_quorum:
                logger.debug(
                    "all units running desired encryption - removing switching-encryption"
                )
                self.state.cluster.update({"switching-encryption": ""})
                logger.info(f"ZooKeeper cluster switching to {self.state.cluster.quorum} quorum")

        self.update_client_data()

    def update_client_data(self) -> None:
        """Writes necessary relation data to all related applications."""
        if not self.unit.is_leader():
            return

        self._set_status(self.state.ready)
        if not isinstance(self.unit.status, ActiveStatus):
            return

        for client in self.state.clients:
            if (
                not client.password  # password not set to peer data, i.e ACLs created
                or client.password
                not in "".join(
                    self.config_manager.current_jaas
                )  # if password in jaas file, unit has probably restarted
            ):
                logger.debug(f"Skipping update of {client.component.name}, ACLs not yet set...")
                continue

            client.update(
                {
                    "uris": client.uris,
                    "endpoints": client.endpoints,
                    "tls": client.tls,
                    "username": client.username,
                    "password": client.password,
                    "chroot": client.chroot,
                }
            )

    def _set_status(self, key: Status) -> None:
        """Sets charm status."""
        status: StatusBase = key.value.status
        log_level: DebugLevel = key.value.log_level

        getattr(logger, log_level.lower())(status.message)
        self.unit.status = status


if __name__ == "__main__":
    main(ZooKeeperCharm)
