#!/usr/bin/env python3
# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.

import logging
import re
from typing import Dict, Iterable, List, Optional, Tuple
from kazoo.handlers.threading import KazooTimeoutError
from ops.charm import CharmBase
import json

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
PEER = "cluster"


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
        return self.charm.model.get_relation(PEER)

    @property
    def units(self) -> Iterable[Unit]:
        units = []
        for item in self.relation.data:
            if not isinstance(item, Unit):
                continue
            else:
                units.append(item)

        return units

    @property
    def planned_units(self) -> bool:
        return True if self.charm.app.planned_units() else False

    @property
    def servers(self) -> Dict[str, Dict[str, str]]:
        servers = {}
        for item in self.relation.data[self.charm.app]:
            result = json.loads(item)
            if result.get("state", ""):
                servers = {**servers, **result}

        logger.info(f"{servers=}")
        return servers

    @property
    def app_started(self) -> bool:
        if self.relation.data[self.charm.app].get("state", None) != "started":
            return False

        return True

    @staticmethod
    def get_server_id(unit: Unit) -> int:
        return int(unit.name.split("/")[1]) + 1

    def get_server_host(self, unit: Unit) -> str:
        return self.relation.data[unit]["private-address"]

    def get_server_string(self, unit: Unit, role: str = "participant"):
        if not self.app_started and self.get_server_id(unit) == 1:
            role = "participant"

        server_id = self.get_server_id(unit=unit)
        host_address = self.get_server_host(unit=unit)
        return f"server.{server_id}={host_address}:{self.server_port}:{self.election_port}:{role};0.0.0.0:{self.client_port}"

    def unit_to_server_config(self, unit: Unit, state: str) -> Dict[str, Dict[str, str]]:
        server_id = str(self.get_server_id(unit))
        server_string = self.get_server_string(unit)
        host = self.get_server_host(unit)

        return {server_id: {"host": host, "server": server_string, "state": state}}

    def server_string_to_server_config(
        self, server_string: str, state: str
    ) -> Dict[str, Dict[str, str]]:
        server_id = str(re.findall(r"server.([1-9]*)", server_string)[0])
        host = str(re.findall(r"(\=[\d.]+)\:", server_string)[0])

        return {server_id: {"host": host, "server": server_string, "state": state}}

    def get_unit_from_server_id(self, myid: int) -> Unit:
        for unit in self.units:
            if f"/{myid}" in unit.name:
                return unit

        raise

    def update_cluster(self) -> Dict[str, Dict[str, str]]:
        if not self.app_started or self.planned_units:
            self.status = MaintenanceStatus("waiting for units to start")
            return {}

        active_hosts = [
            value["hosts"] for _, value in self.servers.items() if value["state"] != "removed"
        ]
        active_servers = {
            value["server"] for _, value in self.servers.items() if value["state"] != "removed"
        }

        try:
            zk = ZooKeeperManager(hosts=active_hosts, client_port=self.client_port)
            zk_members = zk.server_members  # the current members in the ZK quorum

            # remove units first, faster due to no startup/sync delay
            servers_to_remove = list(zk_members - active_servers)
            zk.remove_members(members=servers_to_remove)

            removed_servers = {}
            for removed_server in servers_to_remove:
                removed_servers = {
                    **removed_servers,
                    **self.server_string_to_server_config(removed_server, state="removed"),
                }

            # sorting units to ensure units are added in id order
            servers_to_add = sorted(active_servers - zk_members)
            zk.add_members(members=servers_to_add)

            added_servers = {}
            for added_server in servers_to_add:
                added_servers = {
                    **added_servers,
                    **self.server_string_to_server_config(added_server, state="added"),
                }

            self.status = ActiveStatus()

            return {**removed_servers, **added_servers}

        except (
            MembersSyncingError,
            MemberNotReadyError,
            QuorumLeaderNotFoundError,
            KazooTimeoutError,
        ) as e:
            self.status = MaintenanceStatus(str(e))
            return {}

    def _is_unit_turn(self, unit) -> bool:
        my_turn = True
        server_id = self.get_server_id(unit)
        for myid in range(1, server_id):
            if not self.servers.get(str(myid), None):
                my_turn = False

        logger.info(f"{my_turn=}")
        return my_turn

    def ready_to_start(self, unit: Unit) -> Tuple[bool, str]:
        candidate_server = [self.relation.data[unit].get("server", "")]
        servers_property = list(self.servers.values())
        candidate_servers_property = "\n".join(candidate_server + servers_property)

        # move the initial server through
        if self.get_server_id(unit) == 1 and not self.app_started:
            return True, str(self.relation.data[unit].get("server"))

        logger.info(f"{self._is_unit_turn(unit=unit)=}")
        # this is a scale up, so check for unit order
        if self.planned_units:
            if not self._is_unit_turn(unit=unit):
                return False, ""
        
        return True, candidate_servers_property
