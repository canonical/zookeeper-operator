# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Manager for handling ZooKeeper in-place upgrades."""

import logging
import time
from functools import cached_property
from typing import TYPE_CHECKING

from charms.data_platform_libs.v0.upgrade import (
    ClusterNotReadyError,
    DataUpgrade,
    DependencyModel,
    UpgradeGrantedEvent,
)
from charms.zookeeper.v0.client import (
    QuorumLeaderNotFoundError,
    ZooKeeperManager,
)
from kazoo.client import ConnectionClosedError
from pydantic import BaseModel
from tenacity import retry, stop_after_attempt, wait_random
from typing_extensions import override

if TYPE_CHECKING:
    from charm import ZooKeeperCharm

logger = logging.getLogger(__name__)


class ZooKeeperDependencyModel(BaseModel):
    """Model for ZooKeeper Operator dependencies."""

    service: DependencyModel


class ZooKeeperUpgrade(DataUpgrade):
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
        return self.cluster_state == "idle"

    @cached_property
    def client(self) -> ZooKeeperManager:
        """Cached client manager application for performing ZK commands."""
        return ZooKeeperManager(
            hosts=self.charm.cluster.active_hosts,
            client_port=self.charm.cluster.client_port,
            username="super",
            password=self.charm.cluster.passwords[0],
        )

    @retry(stop=stop_after_attempt(5), wait=wait_random(min=1, max=5), reraise=True)
    def post_upgrade_check(self) -> None:
        """Runs necessary checks validating the unit is in a healthy state after upgrade."""
        self.pre_upgrade_check()

    @override
    def pre_upgrade_check(self) -> None:
        default_message = "Pre-upgrade check failed and cannot safely upgrade"
        try:
            if not self.client.members_broadcasting or not len(self.client.server_members) == len(
                self.charm.cluster.peer_units
            ):
                logger.info("Check failed: broadcasting error")
                raise ClusterNotReadyError(
                    message=default_message,
                    cause="Not all application units are connected and broadcasting in the quorum",
                )

            if self.client.members_syncing:
                logger.info("Check failed: quorum members syncing")
                raise ClusterNotReadyError(
                    message=default_message, cause="Some quorum members are syncing data"
                )

            if not self.charm.cluster.stable:
                logger.info("Check failed: cluster initializing")
                raise ClusterNotReadyError(
                    message=default_message, cause="Charm has not finished initialising"
                )

        except QuorumLeaderNotFoundError:
            logger.info("Check failed: Quorum leader not found")
            raise ClusterNotReadyError(message=default_message, cause="Quorum leader not found")
        except ConnectionClosedError:
            logger.info("Check failed: Unable to connect to the cluster")
            raise ClusterNotReadyError(
                message=default_message, cause="Unable to connect to the cluster"
            )
        except Exception as e:
            logger.info(f"Check failed: Unknown error: {e}")
            raise ClusterNotReadyError(message=default_message, cause="Unknown error")

    @override
    def build_upgrade_stack(self) -> list[int]:
        upgrade_stack = []
        for unit in self.charm.cluster.peer_units:
            config = self.charm.cluster.unit_config(unit=unit)

            # upgrade quorum leader last
            if config["host"] == self.client.leader:
                upgrade_stack.insert(0, int(config["unit_id"]))
            else:
                upgrade_stack.append(int(config["unit_id"]))

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
        self.charm.snap.stop_snap_service()

        if not self.charm.snap.install():
            logger.error("Unable to install ZooKeeper Snap")
            self.set_unit_failed()
            return

        self.charm.zookeeper_config.set_server_jvmflags()

        logger.info(f"{self.charm.unit.name} upgrading service...")
        self.charm.snap.restart_snap_service()

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
