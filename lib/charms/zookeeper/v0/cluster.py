#!/usr/bin/env python3
# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.

import logging
from typing import Set, Tuple
from ops.charm import CharmBase

from ops.model import (
    ActiveStatus,
    BlockedStatus,
    MaintenanceStatus,
    StatusBase,
    Unit,
    WaitingStatus,
)

from charms.zookeeper.v0.client import (
    MemberNotReadyError,
    MembersSyncingError,
    ZooKeeperManager,
)

logger = logging.getLogger(__name__)

CHARM_KEY = "zookeeper"
CLUSTER_KEY = "cluster"


class ZooKeeperCluster:
    def __init__(
        self,
        charm: CharmBase,
        client_port: int = 2181,
        server_port: int = 2888,
        election_port: int = 3888,
    ) -> None:
        self.charm = charm
        self.client_port = client_port
        self.server_port = server_port
        self.election_port = election_port
        self.blocked = False
        self.status: StatusBase = MaintenanceStatus("performing cluster operation")

    @property
    def relation(self):
        return self.charm.model.get_relation(CLUSTER_KEY)

    @property
    def hosts(self):
        hosts = []
        for unit in self.relation.units:
            logger.info(f"{unit=}")
            if self.relation.data[unit].get("state", None) == "started":
                hosts.append(self.relation.data[unit]["private-address"])
        logger.info(f"{hosts=}")
        return hosts

    def build_server_string(self, unit: Unit, role: str = "participant"):
        logger.info("---------- build_server_string ----------")
        server_id = self.get_server_id(unit=unit)
        host_address = self.relation.data[unit]["private-address"]
        return f"server.{server_id}={host_address}:{self.server_port}:{self.election_port}:{role};0.0.0.0:{self.client_port}"

    def get_cluster_members(self) -> Set[str]:
        logger.info("---------- get_cluster_members ----------")
        servers = []
        for unit in self.relation.units:
            server = self.build_server_string(unit)
            servers.append(server)

        return set(servers)

    def update_cluster(self) -> None:
        logger.info("---------- update_cluster ----------")
        if self.blocked:
            return

        if not self.hosts:
            self.blocked = True
            self.status = MaintenanceStatus("no units found in relation data")
            return

        zk = ZooKeeperManager(hosts=self.hosts, client_port=self.client_port)
        zk_members = zk.server_members
        active_cluster_members = self.get_cluster_members()

        try:
            zk.remove_members(members=zk_members - active_cluster_members)
            zk.add_members(members=sorted(active_cluster_members - zk_members))
            self.status = ActiveStatus()
        except (MembersSyncingError, MemberNotReadyError) as e:
            logger.info(str(e))
            self.status = MaintenanceStatus(str(e))

    def is_next_server(self, unit: Unit) -> Tuple[bool, str]:
        logger.info("---------- is_next_server ----------")
        servers_property = f"{self.relation.data[unit].get('server')}\n"
        logger.info(f"{self.relation.units=}")

        for item in self.relation.data:
            logger.info(f"{item=}")
            if not isinstance(item, Unit):
                continue

            if (self.relation.data[item].get("state", None) == "started") and (
                self.relation.data[item].get("myid", None) < self.relation.data[unit].get("server")
            ):
                servers_property += f"{self.build_server_string(item)}\n"

                if int(self.relation.data[item].get("myid", None)) == int(self.relation.data[unit].get("myid")) - 1:
                    return True, servers_property

        return False, ""

    @staticmethod
    def get_server_id(unit: Unit) -> int:
        return int(unit.name.split("/")[1]) + 1
