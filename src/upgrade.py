# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Manager for handling ZooKeeper in-place upgrades."""

from charms.data_platform_libs.v0.upgrade import DataUpgrade, DependencyModel, UpgradeGrantedEvent
from charms.zookeeper.v0.client import ZooKeeperManager
from pydantic import BaseModel
from typing_extensions import override

from src.charm import ZooKeeperCharm


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

    @override
    def pre_upgrade_check(self) -> None:
        raise NotImplementedError

    @override
    def build_upgrade_stack(self) -> list[int]:
        zk = ZooKeeperManager(
            hosts=self.charm.cluster.active_hosts,
            client_port=self.charm.cluster.client_port,
            username="super",
            password=self.charm.cluster.passwords[0],
        )

        upgrade_stack = []
        for unit in self.charm.cluster.peer_units:
            config = self.charm.cluster.unit_config(unit=unit)

            if config["host"] == zk.leader:
                upgrade_stack.insert(0, config["unit_id"])
            else:
                upgrade_stack.append(config["unit_id"])

        return upgrade_stack

    @override
    def log_rollback_instructions(self) -> None:
        raise NotImplementedError

    @override
    def _on_upgrade_granted(self, event: UpgradeGrantedEvent) -> None:
        raise NotImplementedError
