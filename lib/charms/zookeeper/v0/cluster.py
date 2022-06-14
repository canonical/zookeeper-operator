#!/usr/bin/env python3
# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.

import logging
from typing import List, Set, Tuple
from kazoo.handlers.threading import KazooTimeoutError
from ops.charm import CharmBase

from ops.model import (
    ActiveStatus,
    MaintenanceStatus,
    Relation,
    StatusBase,
    Unit,
)

from charms.zookeeper.v0.client import (
    MemberNotReadyError,
    MembersSyncingError,
    QuorumLeaderNotFoundError,
    ZooKeeperManager,
)

logger = logging.getLogger(__name__)

CHARM_KEY = "zookeeper"
CLUSTER_KEY = "cluster"


class ZooKeeperCluster:
    """Handler for performing ZK cluster + peer relation commands."""

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
        self.status: StatusBase = MaintenanceStatus("performing cluster operation")

    @property
    def relation(self) -> Relation:
        """The charm's peer relation.

        Returns:
            Relation
        """
        return self.charm.model.get_relation(CLUSTER_KEY)

    @property
    def hosts(self) -> List[str]:
        """The current started units' IPaddreses from the charm's relation data bucket.

        Returns:
            [str]: list of IP addresses of currently started units
        """
        hosts = []
        for unit in self.relation.data:
            if self.relation.data[unit].get("state", None) == "started":
                hosts.append(self.relation.data[unit]["private-address"])
        return hosts

    def build_server_string(self, unit: Unit, role: str = "participant"):
        """Builds the ZK compliant server string for a given unit.

        args:
            unit (Unit): the unit to get the server string of
            role (str): the ZooKeeper role for the unit. Defaults to 'participant'

        Returns:
            str: the server string
                e.g "server.1=10.141.89.132:2888:3888:participant;0.0.0.0:2181"
        """
        server_id = self.get_server_id(unit=unit)
        host_address = self.relation.data[unit]["private-address"]
        return f"server.{server_id}={host_address}:{self.server_port}:{self.election_port}:{role};0.0.0.0:{self.client_port}"

    def get_cluster_members(self) -> Set[str]:
        """Retreives the server strings for all started, peer-related units.

        This is grabbed from relation.data and not relation.units because we want
        units that may not have started yet.

        Returns:
            {str}: the set of server strings
                e.g {"server.1=10.141.89.132:2888:3888:participant;0.0.0.0:2181"}
        """
        servers = []
        for item in self.relation.data:
            # to ignore returns of type Application
            if not isinstance(item, Unit):
                continue

            server = self.build_server_string(item)
            servers.append(server)

        return set(servers)

    def update_cluster(self) -> None:
        """Compares the current ZK quorum units with application units, and updates leader config
        to align accordingly.
        """
        if not self.hosts:
            self.blocked = True
            self.status = MaintenanceStatus("no units found in relation data")
            return

        try:
            logger.info(f"{self.hosts=}")
            zk = ZooKeeperManager(hosts=self.hosts, client_port=self.client_port)
            zk_members = zk.server_members  # the current members in the ZK quorum
            active_cluster_members = self.get_cluster_members()  # the active charm units

            # remove units first, faster due to no startup/sync delay
            zk.remove_members(members=zk_members - active_cluster_members)

            # sorting units to ensure units are added in id order
            zk.add_members(members=sorted(active_cluster_members - zk_members))
            self.status = ActiveStatus()
        except (MembersSyncingError, MemberNotReadyError, QuorumLeaderNotFoundError, KazooTimeoutError) as e:
            self.status = MaintenanceStatus(str(e))
            return

    def is_next_server(self, unit: Unit) -> Tuple[bool, str]:
        """Checks whether a given unit is the next in order for initial startup.

        args:
            Unit: the unit to check

        Returns:
            (bool, str): tuple of whether it is the unit's turn to start, and the servers property to set during startup
        """
        is_next = None
        servers_property = ""

        for item in self.relation.data:
            if not isinstance(item, Unit):
                continue
            
            # units were found, default to the passed unit to being next
            is_next = True

            # builds server string for every started unit in the peer relation
            servers_property += f"{self.build_server_string(item)}\n"

            # checks if the unit before the passed unit has started
            if (self.relation.data[item].get("state", None) != "started") and (
                self.get_server_id(item) < self.get_server_id(unit)
            ):
                is_next = False
                break

        next_server = is_next or True
        return next_server, servers_property

    @staticmethod
    def get_server_id(unit: Unit) -> int:
        """Retrieves the ZooKeeper server id for a given unit.

        args:
            unit (Unit): the unit to check

        Returns:
            int: the ZK id, equivalent to the application's unit number + 1
        """

        return int(unit.name.split("/")[1]) + 1
