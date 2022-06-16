#!/usr/bin/env python3
# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.

import logging
import re
from typing import Dict, List, Set, Tuple, Union
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
PEER = "cluster"


class UnitNotFoundError(Exception):
    pass


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
    def init_finished(self) -> bool:
        if not self.relation.data[self.charm.app].get("init", None) == "finished":
            return False
        return True

    @property
    def peer_units(self) -> Set[Unit]:
        return set([self.charm.unit] + list(self.relation.units))

    @property
    def quorum_reached(self) -> bool:
        if len(self.peer_units) < 2:
            return False

        return True

    @staticmethod
    def get_unit_id(unit: Unit) -> int:
        return int(unit.name.split("/")[1])

    def get_unit_from_id(self, unit_id: int) -> Unit:
        for unit in self.peer_units:
            if int(unit.name.split("/")[1]) == unit_id:
                return unit

        raise UnitNotFoundError("could not find unit in peer relation")

    def unit_config(
        self, unit: Union[Unit, int], state: str = "ready", role: str = "participant"
    ) -> Dict[str, str]:
        unit_id = None
        server_id = None
        if isinstance(unit, Unit):
            unit = unit
            unit_id = self.get_unit_id(unit=unit)
            server_id = unit_id + 1
        if isinstance(unit, int):
            unit_id = unit
            server_id = unit + 1
            unit = self.get_unit_from_id(unit)

        if unit not in self.peer_units:
            raise UnitNotFoundError

        host = self.relation.data[unit]["private-address"]
        server_string = f"server.{server_id}={host}:{self.server_port}:{self.election_port}:{role};0.0.0.0:{self.client_port}"

        return {
            "host": host,
            "server_string": server_string,
            "server_id": str(server_id),
            "unit_id": str(unit_id),
            "unit_name": unit.name,
            "state": state,
        }

    def update_cluster(self) -> List:
        if not self.quorum_reached:
            logger.info("units not ready")
            self.status = MaintenanceStatus("Peer members not yet started")
            return []

        active_hosts = []
        active_servers = set()
        for unit in self.peer_units:
            active_hosts.append(self.unit_config(unit=unit)["host"])
            active_servers.add(self.unit_config(unit=unit)["server_string"])

        logger.info(f"{self.peer_units}")

        try:
            zk = ZooKeeperManager(hosts=active_hosts, client_port=self.client_port)
            zk_members = zk.server_members  # the current members in the ZK quorum

            # remove units first, faster due to no startup/sync delay
            servers_to_remove = list(zk_members - active_servers)
            zk.remove_members(members=servers_to_remove)

            # sorting units to ensure units are added in id order
            servers_to_add = sorted(active_servers - zk_members)
            zk.add_members(members=servers_to_add)

            self.status = ActiveStatus()

            updated_servers = []
            for server in servers_to_add:
                unit_id = str(int(re.findall(r"server.([1-9]+)", server)[0]) - 1)
                updated_servers.append({unit_id: "added"})

            updated_servers.append({"0": "added"})  # for during initial startup

            return updated_servers

        except (
            MembersSyncingError,
            MemberNotReadyError,
            QuorumLeaderNotFoundError,
            KazooTimeoutError,
            UnitNotFoundError,
        ) as e:
            self.status = MaintenanceStatus(str(e))
            return []

    def _is_unit_turn(self, unit: Unit) -> bool:
        my_turn = True
        unit_id = self.get_unit_id(unit=unit)

        # looping through all app data, ensuring an item exists
        for myid in range(1, unit_id):
            # if it doesn't exist, it hasn't been added by the leader yet
            # i.e not ready
            if not self.relation.data[self.charm.app].get(str(myid), None):
                my_turn = False

        return my_turn

    def _generate_init_units(self, unit_string: str) -> str:
        try:
            quorum_leader_config = self.unit_config(unit=0, state="ready", role="participant")
            quorum_leader_string = quorum_leader_config["server_string"]
        except UnitNotFoundError:  # leader unit not yet found, can't add
            return ""

        return unit_string + "\n" + quorum_leader_string

    def _generate_units(self, unit_string: str) -> str:
        servers = ""
        for unit_id in self.relation.data[self.charm.app]:
            try:
                server_string = self.unit_config(unit=unit_id)["server_string"]
                servers = servers + "\n" + server_string
            except UnitNotFoundError:
                return ""

        servers = servers + "\n" + unit_string

        return servers

    def ready_to_start(self, unit: Unit) -> Tuple[bool, str, Dict]:
        servers = ""
        unit_config = self.unit_config(unit=unit, state="ready", role="observer")
        unit_string = unit_config["server_string"]
        unit_id = unit_config["unit_id"]

        if int(unit_id == 0) and not self.init_finished:
            unit_string = unit_string.replace("observer", "participant")
            return True, unit_string.replace("observer", "participant"), unit_config

        if not self._is_unit_turn(unit=unit):
            return False, "", {}

        if not self.init_finished:
            servers = self._generate_init_units(unit_string=unit_string)
        else:
            servers = self._generate_units(unit_string=unit_string)

        if not servers:
            return False, "", {}

        return True, servers, unit_config
