#!/usr/bin/env python3
# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.

import logging
from typing import List, Set, Tuple
from ops.charm import CharmBase

from ops.model import Relation, Unit

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

    @property
    def relation(self):
        return self.charm.model.get_relation(CLUSTER_KEY)

    @property
    def units(self):
        units = []
        for item in self.relation.data:
            logger.debug(f"{item=}")
            if isinstance(item, Unit):
                units.append(item)
        logger.debug(f"{units=}")
        return units

    @property
    def hosts(self):
        logger.debug("---------- hosts ----------")
        hosts = []
        for unit in self.units:
            logger.debug(f"{unit=}")
            hosts.append(self.relation.data[unit]["private-address"])
        logger.debug(f"{hosts=}")
        return hosts

    def build_server_string(self, unit: Unit, role: str = "participant"):
        logger.debug("---------- hosts ----------")
        server_id = self.get_server_id(unit=unit)
        logger.debug(f"{server_id=}")
        host_address = self.relation.data[unit]["private-address"]
        logger.debug(f"{host_address=}")
        return f"server.{server_id}={host_address}:{self.server_port}:{self.election_port}:{role};0.0.0.0:{self.client_port}"

    def get_cluster_members(self) -> Set[str]:
        logger.debug("---------- cluster_servers ----------")
        servers = []
        for unit in self.units:
            server = self.build_server_string(unit)
            logger.debug(f"{server=}")
            servers.append(server)
            logger.debug(f"{servers=}")

        return set(servers)

    def update_cluster(self) -> bool:
        logger.debug("---------- update_cluster ----------")
        if not self.hosts:
            raise ZooKeeperClusterError(message="no units found")
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

    def is_next_server(self, unit: Unit) -> Tuple[bool, str]:
        logger.debug("---------- is_next_server ----------")
        servers_property = ""
        logger.debug(f"self.units={self.units}")
        for relation_unit in self.units:
            logger.debug(f"{relation_unit=}")
            server_id = self.relation.data[relation_unit]["myid"]
            logger.debug(f"{server_id=}")
            server = self.relation.data[relation_unit]["server"]
            logger.debug(f"{server=}")
            servers_property += f"server.{server_id}={server}\n"
            logger.debug(f"{servers_property=}")
            if self.relation.data[relation_unit]["state"] == "ready":
                if unit == relation_unit:
                    return True, servers_property
                return False, ""
        return False, ""

    @staticmethod
    def get_server_id(unit: Unit) -> int:
        logger.debug("---------- get_server_id ----------")
        return int(unit.name.split("/")[1]) + 1
