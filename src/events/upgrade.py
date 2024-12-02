#!/usr/bin/env python3
# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Event handler for handling ZooKeeper in-place upgrades."""
import logging
import time
from functools import cached_property
from typing import TYPE_CHECKING

from charms.data_platform_libs.v0.upgrade import (
    BaseModel,
    ClusterNotReadyError,
    DataUpgrade,
    DependencyModel,
    UpgradeGrantedEvent,
)
from charms.zookeeper.v0.client import ZooKeeperManager
from tenacity import retry, stop_after_attempt, wait_random
from typing_extensions import override

from literals import CLIENT_PORT

if TYPE_CHECKING:
    from charm import ZooKeeperCharm

logger = logging.getLogger(__name__)


class ZooKeeperDependencyModel(BaseModel):
    """Model for ZooKeeper Operator dependencies."""

    service: DependencyModel


class ZKUpgradeEvents(DataUpgrade):
    """Implementation of :class:`DataUpgrade` overrides for in-place upgrades."""

    def __init__(self, charm: "ZooKeeperCharm", **kwargs):
        super().__init__(charm, **kwargs)
        self.charm = charm

    @property
    def idle(self) -> bool:
        """Checks if cluster state is idle.

        Returns:
            True if cluster state is idle. Otherwise False
        """
        return not bool(self.upgrade_stack)

    @cached_property
    def client(self) -> ZooKeeperManager:
        """Cached client manager application for performing ZK commands."""
        return ZooKeeperManager(
            hosts=[server.internal_address for server in self.charm.state.started_servers],
            client_port=CLIENT_PORT,
            username="super",
            password=self.charm.state.cluster.internal_user_credentials.get("super", ""),
            read_only=False,
        )

    @retry(stop=stop_after_attempt(5), wait=wait_random(min=1, max=5), reraise=True)
    def post_upgrade_check(self) -> None:
        """Runs necessary checks validating the unit is in a healthy state after upgrade."""
        self.pre_upgrade_check()

    @override
    def pre_upgrade_check(self) -> None:
        status = self.charm.quorum_manager.is_syncing()
        if not status.passed:
            raise ClusterNotReadyError(
                message="Pre-upgrade check failed and cannot safely upgrade", cause=status.cause
            )

    @override
    def build_upgrade_stack(self) -> list[int]:
        upgrade_stack = []
        for server in self.charm.state.servers:
            # upgrade quorum leader last
            if server.internal_address == self.client.zk_host:
                upgrade_stack.insert(0, server.unit_id)
            else:
                upgrade_stack.append(server.unit_id)

        return upgrade_stack

    @override
    def log_rollback_instructions(self) -> None:
        logger.critical(
            "\n".join(
                [
                    "Unit failed to upgrade and requires manual rollback to previous stable version.",
                    "    1. Re-run `pre-upgrade-check` action on the leader unit to enter 'recovery' state",
                    "    2. Run `juju refresh` to the previously deployed charm revision",
                ]
            )
        )
        return

    @override
    def _on_upgrade_granted(self, event: UpgradeGrantedEvent) -> None:
        self.charm.workload.stop()

        if not self.charm.workload.install():
            logger.error("Unable to install ZooKeeper...")
            self.set_unit_failed()
            return

        self.apply_backwards_compatibility_fixes()

        logger.info(f"{self.charm.unit.name} upgrading workload...")
        self.charm.workload.restart()

        time.sleep(5.0)

        try:
            logger.debug("Running post-upgrade check...")
            self.post_upgrade_check()

            logger.debug("Marking unit completed...")
            self.set_unit_completed()

            # ensures leader gets it's own relation-changed when it upgrades
            if self.charm.unit.is_leader():
                logger.debug("Re-emitting upgrade-changed on leader...")
                self.on_upgrade_changed(event)

        except ClusterNotReadyError as e:
            logger.error(e.cause)
            self.set_unit_failed()

    def apply_backwards_compatibility_fixes(self) -> None:
        """A range of functions needed for backwards compatibility."""
        # Rev.113 - needed for allowing log-level upgrades
        self.charm.config_manager.set_server_jvmflags()

        # Rev.115 - needed for new unique truststore password relation-data
        # Defaults to keystore password for keeping existing store from older revisions
        self.charm.state.unit_server.update(
            {
                "truststore-password": self.charm.state.unit_server.truststore_password
                or self.charm.state.unit_server.keystore_password,
            }
        )
