#!/usr/bin/env python3
# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.

import logging
from typing import Set

from ops.charm import (
    CharmBase,
)
from ops.framework import EventBase, Object
from ops.model import ActiveStatus, BlockedStatus, MaintenanceStatus, StatusBase, WaitingStatus

from charms.zookeeper.v0.client import (
    MemberNotReadyError,
    MembersSyncingError,
    ZooKeeperManager,
)

logger = logging.getLogger(__name__)

CLUSTER_KEY = "cluster"


class ZooKeeperCluster(Object):
    def __init__(
        self,
        charm: CharmBase,
        client_port: int = 2181,
        server_port: int = 2888,
        election_port: int = 3888,
    ) -> None:
        super().__init__(charm, CLUSTER_KEY)
        self.charm = charm
        self.client_port = client_port
        self.server_port = server_port
        self.election_port = election_port

    @property
    def relation(self):
        return self.charm.model.get_relation(CLUSTER_KEY)

    def _get_server_id(self, unit):
        return int(unit.name.split("/")[1]) + 1

    def write_server_id(self):
        for line in self.charm.config["zookeeper-properties"].splitlines():
            if "dataDir" in line:
                data_dir = line.split("=")[1]
                with open(f"{data_dir}/myid", "w") as f:
                    f.write(str(self._get_server_id(self.charm.unit)))
                return MaintenanceStatus("successfully added myid file to zookeeper")

            logger.warning("unable to set myid file to zookeeper - dataDir config option missing")
            return BlockedStatus(
                "unable to set myid file to zookeeper - dataDir config option missing"
            )

    @property
    def hosts(self):
        return [self.relation.data[unit]["private-address"] for unit in self.relation.data.units]

    @property
    def juju_members(self) -> Set[str]:
        servers = []
        for unit in self.relation.data.units:
            server_id = self._get_server_id(unit=unit)
            host_address = self.relation.data[unit]["private-address"]

            servers.append(
                f"server.{server_id}={host_address}:{self.server_port}:{self.election_port}:participant;0.0.0.0:{self.client_port}"
            )

        return set(servers)

    def update_cluster(self) -> StatusBase:
        zk = ZooKeeperManager(hosts=self.hosts, client_port=self.client_port)
        zk_members = zk.server_members

        try:
            zk.remove_members(members=zk_members - self.juju_members)
            zk.add_members(members=sorted(self.juju_members - zk_members))
            return ActiveStatus()
        except (MembersSyncingError, MemberNotReadyError):
            return WaitingStatus("cluster members not ready for update, waiting")
        except Exception as e:
            logger.error(e)
            return BlockedStatus("failure during cluster update")
