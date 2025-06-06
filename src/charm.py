#!/usr/bin/env python3
# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.

"""Charmed Machine Operator for Apache ZooKeeper."""

import logging
import time

from charms.data_platform_libs.v0.data_models import TypedCharmBase
from charms.grafana_agent.v0.cos_agent import COSAgentProvider
from charms.rolling_ops.v0.rollingops import RollingOpsManager
from charms.zookeeper.v0.client import QuorumLeaderNotFoundError
from kazoo.exceptions import BadArgumentsError, BadVersionError, ReconfigInProcessError
from ops import (
    ActiveStatus,
    EventBase,
    InstallEvent,
    RelationDepartedEvent,
    SecretChangedEvent,
    StartEvent,
    StatusBase,
    StorageAttachedEvent,
    WaitingStatus,
    main,
)

from core.cluster import ClusterState
from core.structured_config import CharmConfig
from events.backup import BackupEvents
from events.password_actions import PasswordActionEvents
from events.provider import ProviderEvents
from events.tls import TLSEvents
from events.upgrade import ZKUpgradeEvents, ZooKeeperDependencyModel
from literals import (
    CHARM_KEY,
    CHARM_USERS,
    DEPENDENCIES,
    GROUP,
    JMX_PORT,
    METRICS_PROVIDER_PORT,
    PEER,
    SUBSTRATE,
    USER,
    DebugLevel,
    Status,
)
from managers.config import ConfigManager
from managers.k8s import K8sManager
from managers.quorum import QuorumManager
from managers.tls import TLSManager
from workload import ZKWorkload

logger = logging.getLogger(__name__)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpxcore").setLevel(logging.WARNING)


class ZooKeeperCharm(TypedCharmBase[CharmConfig]):
    """Charmed Operator for ZooKeeper."""

    config_type = CharmConfig

    def __init__(self, *args):
        super().__init__(*args)
        self.name = CHARM_KEY
        self.state = ClusterState(self, substrate=SUBSTRATE)
        self.workload = ZKWorkload()

        # --- CHARM EVENT HANDLERS ---

        self.backup_events = BackupEvents(self)
        self.password_action_events = PasswordActionEvents(self)
        self.tls_events = TLSEvents(self)
        self.provider_events = ProviderEvents(self)
        self.upgrade_events = ZKUpgradeEvents(
            self,
            substrate=SUBSTRATE,
            dependency_model=ZooKeeperDependencyModel(
                **DEPENDENCIES  # pyright: ignore[reportArgumentType]
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
        self.k8s_manager = K8sManager(
            pod_name=self.state.unit_server.pod_name, namespace=self.model.name
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

        self.framework.observe(getattr(self.on, "secret_changed"), self._on_secret_changed)

        self.framework.observe(
            getattr(self.on, "cluster_relation_changed"), self._on_cluster_relation_changed
        )
        self.framework.observe(
            getattr(self.on, "cluster_relation_joined"), self._on_cluster_relation_changed
        )
        self.framework.observe(
            getattr(self.on, "cluster_relation_departed"), self._on_cluster_relation_departed
        )

        self.framework.observe(
            getattr(self.on, "data_storage_attached"), self._on_storage_attached
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
            self.unit.status = WaitingStatus("waiting for peer relation")
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

    def _on_cluster_relation_changed(self, event: EventBase) -> None:  # noqa: C901
        """Generic handler for all 'something changed, update' events across all relations."""
        # not all methods called
        if not self.state.peer_relation:
            self._set_status(Status.NO_PEER_RELATION)
            return

        if self.state.cluster.is_restore_in_progress:
            # Ongoing backup restore, we can early return here since the
            # chain of events is only relevant to the backup event handler
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
        # instead, wait for relation-changed from its setting of 'started'
        # also don't run (and restart) if some units still need to set ip
        self._set_status(self.state.all_installed)
        if not isinstance(self.unit.status, ActiveStatus):
            return

        # attempt startup of server
        if not self.state.unit_server.started:
            self.init_server()

        # even if leader has not started, attempt update quorum
        self.update_quorum(event=event)

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

        # in case server was erroneously removed from the quorum
        if not self.state.stale_quorum and not self.quorum_manager.server_in_quorum:
            self._set_status(Status.SERVICE_NOT_QUORUM)
            return

        self._set_status(Status.ACTIVE)

    def _on_cluster_relation_departed(self, event: RelationDepartedEvent) -> None:
        """Handler for `relation_departed` events."""
        # is related to issue found in https://bugs.launchpad.net/juju/+bug/2053055
        # likely due to a controller upgrade or a cloud maintenance with machines being reshuffled
        # periodically, juju would emit a LeaderElected event, and would return no peer units
        # the leader would then remove all other units from the quorum, which when restarted, would fail
        if not event.departing_unit or not self.model.app.planned_units():
            return

        departing_server_id = (
            int(event.departing_unit.name.split("/")[1]) + 1
        )  # server-ids must be positive integers

        try:
            self.quorum_manager.client.remove_members(members=[f"server.{departing_server_id}"])
        except (
            ReconfigInProcessError,  # another unit already handling
            BadVersionError,  # another unit handled
            QuorumLeaderNotFoundError,  # this unit is departing, can't find leader in peer data
            BadArgumentsError,  # already handled
        ):
            pass

        # NOTE: if the leader is also going down, it may miss the event to set {unit.id: removed}
        # to avoid this, eventual clean up occurs during update-status calling update_quorum

    def _on_storage_attached(self, _: StorageAttachedEvent) -> None:
        """Handler for `storage_attached` events."""
        self.workload.exec(["chmod", "750", f"{self.workload.paths.data_path}"])
        self.workload.exec(["chown", f"{USER}:{GROUP}", f"{self.workload.paths.data_path}"])

    def _on_secret_changed(self, event: SecretChangedEvent):
        """Reconfigure services on a secret changed event."""
        if not event.secret.label:
            return

        if not self.state.cluster.relation:
            return

        if event.secret.label == self.state.cluster.data_interface._generate_secret_label(
            PEER,
            self.state.cluster.relation.id,
            "extra",  # type:ignore noqa  -- Changes with the https://github.com/canonical/data-platform-libs/issues/124
        ):
            self._on_cluster_relation_changed(event)

    def _manual_restart(self, event: StartEvent) -> None:
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
            and self.state.next_server.component
            and self.state.unit_server.component
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
        self.config_manager.set_client_jaas_config()

        # during reschedules (e.g upgrades or otherwise) we lose all files
        # need to manually add-back key/truststores
        if (
            self.state.cluster.tls
            and self.state.unit_server.certificate
            and self.state.unit_server.ca_cert
        ):  # TLS is probably completed
            self.tls_manager.set_private_key()
            self.tls_manager.set_ca()
            self.tls_manager.set_chain()
            self.tls_manager.set_bundle()
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

    def disconnect_clients(self) -> None:
        """Remove a necessary part of the client databag, acting as a logical disconnect."""
        if not self.unit.is_leader():
            return

        for client in self.state.clients:
            client.update({"endpoints": ""})

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
                if client.component:
                    logger.debug(
                        f"Skipping update of {client.component.name}, ACLs not yet set..."
                    )
                else:
                    logger.debug("Client has not component (app|unit) specified, quitting...")
                continue

            client.update(
                {
                    "endpoints": client.endpoints,
                    "tls": client.tls,
                    "username": client.username,
                    "password": client.password,
                    "database": client.database,
                    # Duplicated for compatibility with older requirers
                    # TODO (zkclient): Remove these entries
                    "chroot": client.database,
                    "uris": client.uris,
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
