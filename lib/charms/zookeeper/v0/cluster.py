#!/usr/bin/env python3
# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.

import logging
from typing import Iterable, Set

from ops.charm import (
    CharmBase,
)
from ops.framework import Object
from ops.model import Relation, Unit

from charms.zookeeper.v0.client import (
    MemberNotReadyError,
    MembersSyncingError,
    ZooKeeperManager,
)

logger = logging.getLogger(__name__)


class ZooKeeperClusterError(Exception):
    def __init__(self, message: str):
        self.message = message


class ZooKeeperCluster:
    def __init__(
        self,
        relation: Relation,
        client_port: int = 2181,
        server_port: int = 2888,
        election_port: int = 3888,
    ) -> None:
        self.relation = relation
        self.client_port = client_port
        self.server_port = server_port
        self.election_port = election_port

    @property
    def hosts(self):
        logger.debug("---------- hosts ----------")
        return [self.relation.data[unit]["private-address"] for unit in self.relation.data.units]

    def get_cluster_members(self, role: str = "participant") -> Set[str]:
        logger.debug("---------- cluster_servers ----------")
        servers = []
        for unit in self.relation.data.units:
            _role = role
            logger.debug(f"{unit=}")
            server_id = self.get_server_id(unit=unit)
            logger.debug(f"{server_id=}")
            if server_id == 1:
                _role = "participant"
            host_address = self.relation.data[unit]["private-address"]
            logger.debug(f"{host_address=}")
            server = f"server.{server_id}={host_address}:{self.server_port}:{self.election_port}:{_role};0.0.0.0:{self.client_port}"
            logger.debug(f"{server=}")

            servers.append(server)
            logger.debug(f"{servers=}")

        return set(servers)

    def update_cluster(self) -> bool:
        logger.debug("---------- update_cluster ----------")
        zk = ZooKeeperManager(hosts=self.hosts, client_port=self.client_port)
        logger.debug(f"{zk=}")
        zk_members = zk.server_members
        logger.debug(f"{zk_members=}")
        active_cluster_members = self.get_cluster_members()
        logger.debug(f"{active_cluster_members=}")

        try:
            zk.remove_members(members=zk_members - active_cluster_members)
            zk.add_members(members=sorted(active_cluster_members - zk_members))
            return True
        except (MembersSyncingError, MemberNotReadyError) as e:
            logger.info(str(e))
            return False

    @staticmethod
    def get_server_id(unit: Unit) -> int:
        logger.debug("---------- get_server_id ----------")
        return int(unit.name.split("/")[1]) + 1
