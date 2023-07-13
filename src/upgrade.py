# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Manager for handling ZooKeeper in-place upgrades."""

import logging
from functools import cached_property
from typing import TYPE_CHECKING

from charms.data_platform_libs.v0.upgrade import (
    ClusterNotReadyError,
    DataUpgrade,
    DependencyModel,
    UpgradeGrantedEvent,
)
from charms.zookeeper.v0.client import QuorumLeaderNotFoundError, ZooKeeperManager
from pydantic import BaseModel
from typing_extensions import override

if TYPE_CHECKING:
    from charm import ZooKeeperCharm

logger = logging.getLogger(__name__)


class ZooKeeperDependencyModel(BaseModel):
    """Model for ZooKeeper Operator dependencies."""

    charm: DependencyModel
    snap: DependencyModel
    service: DependencyModel


class ZooKeeperUpgrade(DataUpgrade):
    """Implementation of :class:`DataUpgrade` overrides for in-place upgrades."""

    def __init__(self, charm: "ZooKeeperCharm", **kwargs):
        super().__init__(charm, **kwargs)
        self.charm = charm

    @cached_property
    def client(self) -> ZooKeeperManager:
        """Cached client manager application for performing ZK commands."""
        return ZooKeeperManager(
            hosts=self.charm.cluster.active_hosts,
            client_port=self.charm.cluster.client_port,
            username="super",
            password=self.charm.cluster.passwords[0],
        )

    @override
    def pre_upgrade_check(self) -> None:
        default_message = "Pre-upgrade check failed and cannot safely upgrade"
        try:
            if not self.client.members_broadcasting or not len(self.client.server_members) == len(
                self.charm.cluster.peer_units
            ):
                raise ClusterNotReadyError(
                    message=default_message,
                    cause="Not all application units are connected and broadcasting in the quorum",
                )

            if self.client.members_syncing:
                raise ClusterNotReadyError(
                    message=default_message, cause="Some quorum members are syncing data"
                )

            if not self.charm.cluster.stable:
                raise ClusterNotReadyError(
                    message=default_message, cause="Charm has not finished initialising"
                )

        except QuorumLeaderNotFoundError:
            raise ClusterNotReadyError(message=default_message, cause="Quorum leader not found")

    @override
    def build_upgrade_stack(self) -> list[int]:
        upgrade_stack = []
        for unit in self.charm.cluster.peer_units:
            config = self.charm.cluster.unit_config(unit=unit)

            # upgrade quorum leader last
            if config["host"] == self.client.leader:
                upgrade_stack.insert(0, config["unit_id"])
            else:
                upgrade_stack.append(config["unit_id"])

        return upgrade_stack

    @override
    def log_rollback_instructions(self) -> None:
        logger.warning("SOME USEFUL INSTRUCTIONS")  # TODO: do it?

    @override
    def _on_upgrade_granted(self, event: UpgradeGrantedEvent) -> None:
        if not self.charm.snap.install():
            logger.error("Unable to install ZooKeeper Snap")
            self.set_unit_failed()
            return

        self.charm._restart(event)

        try:
            self.pre_upgrade_check()
            self.set_unit_completed()

            # ensures leader gets it's own relation-changed when it upgrades
            if self.charm.unit.is_leader():
                self.on_upgrade_changed(event)

        except ClusterNotReadyError as e:
            logger.error(e.cause)
            self.set_unit_failed()
