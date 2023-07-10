# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

from typing_extensions import override
from pydantic import BaseModel

from charms.data_platform_libs.v0.upgrade import DataUpgrade, DependencyModel, UpgradeGrantedEvent


class ZooKeeperDependencyModel(BaseModel):
    charm: DependencyModel
    snap: DependencyModel
    service: DependencyModel

class ZooKeeperUpgrade(DataUpgrade):
    @override
    def pre_upgrade_check(self) -> None:
        raise NotImplementedError

    @override
    def build_upgrade_stack(self) -> list[int]
        raise NotImplementedError

    @override
    def log_rollback_instructions(self) -> None:
        raise NotImplementedError
    
    @override
    def _on_upgrade_granted(self, event: UpgradeGrantedEvent) -> None:
        raise NotImplementedError
