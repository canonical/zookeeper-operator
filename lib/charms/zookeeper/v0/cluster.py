#!/usr/bin/env python3
# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.

import logging
from typing import Iterable, Set

from ops.charm import (
    CharmBase,
)
from ops.framework import Object
from ops.model import Unit

from charms.zookeeper.v0.client import (
    MemberNotReadyError,
    MembersSyncingError,
    ZooKeeperManager,
)

logger = logging.getLogger(__name__)

CLUSTER_KEY = "cluster"


class ZooKeeperClusterError(Exception):
    def __init__(self, message: str):
        self.message = message


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

    def _build_members(self, servers: Iterable[str], role: str) -> Set[str]:
        return {f"{server}:{role};0.0.0.0:{self.client_port}" for server in servers}

    @property
    def relation(self):
        logger.debug("---------- relation ----------")
        return self.charm.model.get_relation(CLUSTER_KEY)

    @property
    def hosts(self):
        logger.debug("---------- hosts ----------")
        return [self.relation.data[unit]["private-address"] for unit in self.relation.data.units]

    @property
    def cluster_servers(self) -> Set[str]:
        logger.debug("---------- cluster_servers ----------")
        servers = []
        for unit in self.relation.data.units:
            logger.debug(f"{unit=}")
            server_id = self.get_server_id(unit=unit)
            logger.debug(f"{server_id=}")
            host_address = self.relation.data[unit]["private-address"]
            logger.debug(f"{host_address=}")
            server_address = (
                f"server.{server_id}={host_address}:{self.server_port}:{self.election_port}"
            )
            logger.debug(f"{server_address=}")

            servers.append(server_address)
            logger.debug(f"{servers=}")

        return set(servers)

    def update_cluster(self) -> bool:
        logger.debug("---------- update_cluster ----------")
        zk = ZooKeeperManager(hosts=self.hosts, client_port=self.client_port)
        logger.debug(f"{zk=}")
        zk_members = zk.server_members
        logger.debug(f"{zk_members=}")
        active_cluster_members = self._build_members(
            servers=self.cluster_servers, role="participant"
        )
        logger.debug(f"{active_cluster_members=}")

        try:
            zk.remove_members(members=zk_members - active_cluster_members)
            zk.add_members(members=sorted(active_cluster_members - zk_members))
            return True
        except (MembersSyncingError, MemberNotReadyError) as e:
            raise ZooKeeperClusterError(str(e))

    @staticmethod
    def get_server_id(unit: Unit) -> int:
        logger.debug("---------- get_server_id ----------")
        return int(unit.name.split("/")[1]) + 1
